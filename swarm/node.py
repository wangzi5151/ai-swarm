"""工作节点：跑在每台"已经有 agent 的 Linux"上。

    swarm node --discover --agent cli --cmd 'aider --yes --message {prompt}'

它做四件事：
  1. 加入局域网里的 hub（UDP 自动发现，或者 --hub 指定）
  2. 长轮询领任务（不空转 CPU），多个槽并发跑
  3. 把任务快照铺进自己的临时工作目录（只拉本地缓存里没有的 blob）
  4. 让本机 agent 干活、跑验收命令，把改动清单回传，主工程由 hub 合并

节点不碰别人的文件，也碰不到主工程 —— 这是隔离的物理保证。
任务工作目录按 (项目, 任务, 第几次尝试) 命名，两个项目同名任务不会互相删。
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

from . import DEFAULT_DISCOVERY_PORT, DEFAULT_PORT, __version__
from . import agents, discover, proto, workspace
from .proto import AuthError, ForbiddenError, HubClient, SwarmError
from .store import BlobStore, sha256_text

FETCH_WORKERS = 8


def _safe_name(text, limit=48):
    text = re.sub(r"[^0-9A-Za-z._-]", "_", str(text or ""))
    return (text or "task")[:limit]


class Node(object):
    def __init__(self, root=None, hub=None, token=None, name=None, slots=2, kind="mock",
                 cmd=None, base_url=None, model=None, api_key=None, mock_delay=0.4,
                 discovery_port=DEFAULT_DISCOVERY_PORT, keep_work=False, verbose=True,
                 verify=None, tls=False, tls_pin=None, ca=None, insecure=False,
                 once=False):
        self.root = os.path.abspath(root or os.path.join(os.path.expanduser("~"), ".swarm-node"))
        os.makedirs(self.root, exist_ok=True)
        self.work_root = os.path.join(self.root, "work")
        os.makedirs(self.work_root, exist_ok=True)
        self.cache = BlobStore(os.path.join(self.root, "cache"))

        self.hub_addr = hub
        self.token = token or os.environ.get("SWARM_TOKEN") or ""
        self.name = name or proto.machine_name()
        self.slots = max(1, int(slots))
        self.kind = kind
        self.cmd = cmd or os.environ.get("SWARM_AGENT_CMD")
        self.base_url = base_url or os.environ.get("SWARM_LLM_BASE")
        self.model = model or os.environ.get("SWARM_LLM_MODEL")
        self.api_key = api_key or os.environ.get("SWARM_LLM_KEY")
        self.verify = verify or os.environ.get("SWARM_VERIFY")
        self.mock_delay = float(mock_delay)
        self.discovery_port = int(discovery_port)
        self.keep_work = keep_work
        self.verbose = verbose
        self.tls = bool(tls) or bool(os.environ.get("SWARM_TLS"))
        self.tls_pin = (tls_pin or os.environ.get("SWARM_TLS_PIN") or "").strip() or None
        self.ca = ca or os.environ.get("SWARM_CA") or None
        self.insecure = bool(insecure) or bool(os.environ.get("SWARM_TLS_INSECURE"))
        self.once = bool(once)

        self.stop = threading.Event()
        self.node_id = None
        self.host = None
        self.port = None
        self.lease = 180
        self.heartbeat_interval = 5.0
        self._running = {}          # (project, task) -> client
        self._running_attempt = {}  # (project, task) -> attempt
        self._lock = threading.Lock()
        self._clients = []
        self._threads = []
        self._pool = ThreadPoolExecutor(max_workers=FETCH_WORKERS)
        self._pool_local = threading.local()
        self.meta_path = os.path.join(self.root, "node.json")
        self._load_meta()

    # ------------------------------------------------------------------
    def log(self, msg):
        if self.verbose:
            sys.stderr.write("[%s] %s\n" % (self.name, msg))
            sys.stderr.flush()

    def _load_meta(self):
        if os.path.exists(self.meta_path):
            try:
                with open(self.meta_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.node_id = data.get("node_id")
            except Exception:
                pass

    def _save_meta(self):
        tmp = self.meta_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"node_id": self.node_id, "name": self.name,
                       "hub": [self.host, self.port]}, f)
        os.replace(tmp, self.meta_path)

    # ------------------------------------------------------------------
    def resolve_hub(self):
        if not self.hub_addr:
            self.log("自动发现 hub（UDP %d）…" % self.discovery_port)
            found = discover.find_hub(self.discovery_port, log=self.log)
            if not found:
                raise SwarmError(
                    "局域网里没找到 hub。确认 hub 已启动、在同一网段，"
                    "或者用 --hub <ip:port> 直接指定。")
            host, port, info = found
            self.host, self.port = host, port
            if info.get("tls"):
                self.tls = True
                self.tls_pin = self.tls_pin or info["tls"]
                self.log("hub 启用了 TLS，用自动发现带回的指纹固定（TOFU，首次信任）%s…"
                         % info["tls"][:16])
        else:
            scheme, self.host, self.port = proto.split_hub_addr(self.hub_addr, DEFAULT_PORT)
            if scheme == "https":
                self.tls = True
        if not self.token:
            raise SwarmError("缺少令牌：--token 或环境变量 SWARM_TOKEN")
        if self.tls and not (self.tls_pin or self.ca or self.insecure):
            self.log("⚠ 启用了 TLS 但没给 --tls-pin/--ca：只加密、不校验身份。"
                     "从 hub 启动日志里抄证书指纹，用 --tls-pin 固定更安全。")
        self.log("连接 hub %s://%s:%d" % ("https" if self.tls else "http",
                                          self.host, self.port))

    def _new_client(self):
        c = HubClient(self.host, self.port, self.token, timeout=60,
                      scheme="https" if self.tls else "http",
                      tls_pin=self.tls_pin, ca=self.ca, insecure=self.insecure)
        c.node_id = self.node_id
        self._clients.append(c)
        return c

    def _rejoin(self, client):
        info = {
            "node_id": self.node_id,
            "name": self.name,
            "kinds": [self.kind],
            "slots": self.slots,
            "os": "%s %s" % (sys.platform, os.uname().machine if hasattr(os, "uname") else ""),
            "version": __version__,
            "addr": "%s:%d" % (proto.lan_ip(), os.getpid()),
        }
        res = client.join(info)
        self.node_id = res.get("node_id") or self.node_id
        client.node_id = self.node_id
        try:
            self.lease = max(5, int(res.get("lease") or 180))
        except (TypeError, ValueError):
            self.lease = 180
        self.heartbeat_interval = max(1.0, min(5.0, self.lease / 3.0))
        self._save_meta()
        self.log("已加入 hub「%s」，节点 id %s，%d 个槽位，租约 %ds" %
                 (res.get("hub"), self.node_id, self.slots, self.lease))
        return res

    # ------------------------------------------------------------------
    def run(self):
        self.resolve_hub()
        client = self._new_client()
        self._rejoin(client)
        hb = threading.Thread(target=self._heartbeat_loop, name="swarm-hb", daemon=True)
        hb.start()
        self._threads.append(hb)
        if self.once:
            return self._run_once(client)
        for i in range(self.slots):
            t = threading.Thread(target=self._worker, args=(i,), name="swarm-slot-%d" % i, daemon=True)
            t.start()
            self._threads.append(t)
        self.log("开始领任务（Ctrl-C 退出）")
        try:
            while not self.stop.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()
        return 0

    def _run_once(self, client):
        """--once：领一个任务、干完、退出。用来验证 agent 配置是否真的能干活。"""
        res = client.claim({"node_id": self.node_id, "slots": 1}, wait=2)
        task = res.get("task")
        if not task:
            self.log("当前没有可领的任务（--once 退出）")
            self.shutdown()
            return 0
        self.log("执行一次任务 %s/%s：%s" % (task.get("project"), task.get("id"),
                                             task.get("title")))
        try:
            self._execute(client, task, res.get("snapshot") or [])
        except Exception as exc:
            self.log("任务执行失败：%s" % exc)
            try:
                client.fail({"node_id": self.node_id, "project": task.get("project"),
                             "task_id": task.get("id"), "attempt": task.get("attempt"),
                             "error": "%s: %s" % (type(exc).__name__, exc)})
            except Exception:
                pass
            self.shutdown()
            return 2
        self.shutdown()
        return 0

    def _running_refs(self):
        with self._lock:
            return [{"project": pid, "task": tid, "attempt": self._running_attempt.get((pid, tid))}
                    for (pid, tid) in self._running]

    def shutdown(self):
        if self.stop.is_set():
            return
        self.stop.set()
        refs = self._running_refs()
        if refs:
            self.log("退出前把 %d 个在跑的任务交还队列" % len(refs))
        for ref in refs:
            try:
                client = self._running.get((ref["project"], ref["task"]))
                if client is not None:
                    client.fail({"node_id": self.node_id, "project": ref["project"],
                                 "task_id": ref["task"], "attempt": ref.get("attempt"),
                                 "error": "节点退出，任务交还队列"})
            except Exception:
                pass
        try:
            self._pool.shutdown(wait=False)
        except Exception:
            pass
        for c in self._clients:
            c.close_all()
        self.log("已断开")

    # ------------------------------------------------------------------
    def _heartbeat_loop(self):
        client = self._new_client()
        while not self.stop.wait(self.heartbeat_interval):
            try:
                client.heartbeat({
                    "node_id": self.node_id,
                    "running": self._running_refs(),
                    "slots": self.slots,
                    "kinds": [self.kind],
                })
            except (AuthError, ForbiddenError) as exc:
                self.log("鉴权失败（令牌被吊销？）：%s" % exc)
                self.stop.set()
                return
            except Exception:
                try:
                    self._rejoin(client)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    def _worker(self, idx):
        client = self._new_client()
        while not self.stop.is_set():
            try:
                res = client.claim({"node_id": self.node_id, "slots": self.slots}, wait=25)
            except (AuthError, ForbiddenError) as exc:
                self.log("鉴权失败，停止领取：%s" % exc)
                self.stop.set()
                return
            except Exception as exc:
                if self.stop.is_set():
                    break
                self.log("领取失败（%s），2 秒后重试" % exc)
                time.sleep(2)
                try:
                    self._rejoin(client)
                except Exception:
                    pass
                continue
            task = res.get("task")
            if not task:
                continue
            try:
                self._execute(client, task, res.get("snapshot") or [])
            except (AuthError, ForbiddenError) as exc:
                self.log("交付鉴权失败：%s" % exc)
                self.stop.set()
                return
            except Exception as exc:
                traceback.print_exc()
                try:
                    client.fail({"node_id": self.node_id,
                                 "project": task.get("project"), "task_id": task["id"],
                                 "attempt": task.get("attempt"),
                                 "error": "%s: %s" % (type(exc).__name__, exc)})
                except Exception:
                    pass

    # ------------------------------------------------------------------
    @staticmethod
    def _work_path(work_root, pid, tid, attempt):
        """任务工作目录：可读的名字 + 原始 (pid,tid) 的短哈希，避免清洗后撞名。"""
        tag = sha256_text("%s\x00%s" % (pid, tid))[:10]
        return os.path.join(work_root, _safe_name(pid),
                            "%s-%s-%d" % (_safe_name(tid), tag, int(attempt)))

    def _execute(self, client, task, snapshot):
        tid = task["id"]
        pid = task.get("project") or "p?"
        attempt = int(task.get("attempt") or 1)
        work = self._work_path(self.work_root, pid, tid, attempt)
        shutil.rmtree(work, ignore_errors=True)
        os.makedirs(work, exist_ok=True)
        key = (pid, tid)
        with self._lock:
            self._running[key] = client
            self._running_attempt[key] = attempt
        t0 = time.time()
        self.log("领到任务 %s/%s「%s」：%d 个文件" % (pid, tid, task["title"], len(snapshot)))

        buf = []
        lock = threading.RLock()
        last = [time.time()]

        def log_line(line):
            with lock:
                buf.append(str(line)[:300])
                if len(buf) > 400:
                    del buf[:len(buf) - 400]
                now = time.time()
                if now - last[0] > 1.5:
                    last[0] = now
                    self._flush(client, pid, tid, buf, lock, attempt)

        try:
            self._fetch_missing(client, snapshot)
            workspace.materialize(work, None, snapshot, cache=self.cache)

            feedback = str(task.get("feedback") or "").strip()
            prompt = task.get("prompt") or task.get("title") or ""
            if feedback:
                prompt = (prompt + "\n\n## 上次尝试失败的原因（这次务必避开）\n\n"
                          + feedback[:5000])
                log_line("↺ 带上次失败反馈重试（%d 字）" % len(feedback))
            ctx = {
                "workdir": work,
                "prompt": prompt,
                "feedback": feedback,
                "attempt": attempt,
                "files": task.get("write") or ["**"],
                "file_list": [e["path"] for e in snapshot],
                "task_id": tid,
                "title": task.get("title") or tid,
                "node": self.name,
                "log": log_line,
                "cmd": task.get("cmd") or self.cmd,
                "base_url": self.base_url,
                "model": task.get("model") or self.model,
                "api_key": self.api_key,
                "mock_delay": self.mock_delay,
                "timeout": task.get("timeout") or 1800,
            }
            try:
                agents.run_agent(self.kind, ctx)
            except Exception as exc:
                with lock:
                    buf.append("!! %s" % str(exc)[:300])
                self._flush(client, pid, tid, buf, lock, attempt)
                raise

            # 验收命令：任务级优先，其次节点默认 --verify
            verify_cmd = task.get("verify") or self.verify
            verify_res = None
            if verify_cmd:
                log_line("验收：%s" % verify_cmd)
                ok, out = agents.run_shell(
                    verify_cmd, work,
                    timeout=min(int(task.get("timeout") or 600), 3600),
                    log=log_line)
                verify_res = {"cmd": verify_cmd, "ok": bool(ok), "out": out[-4000:]}
                if not ok:
                    log_line("⚠ 验收失败")

            write_scope = task.get("write") or ["**"]
            ignore = workspace.load_ignores(work)
            all_changes = workspace.diff_work(work, snapshot, ignore=ignore)
            outside = workspace.out_of_scope(all_changes, write_scope)
            changes = [c for c in all_changes if workspace.match_any(c["path"], write_scope)]
            if outside:
                log_line("⚠ 越界修改已丢弃（不在任务范围）：%s" % ", ".join(outside[:6]))
            if not changes:
                log_line("⚠ 没有产生任何改动，hub 会判为失败")
            big = workspace.oversized(work)
            if big:
                log_line("⚠ 超过 %d MB 的文件不参与同步：%s" % (
                    workspace.MAX_FILE >> 20,
                    ", ".join("%s(%.1fMB)" % (n, sz / 1048576.0) for n, sz in big[:4])))

            for ch in changes:
                if ch.get("deleted"):
                    continue
                full = os.path.join(work, ch["path"])
                ch["sha"] = self.cache.put_file(full)
            self._push_blobs(client, changes)

            with lock:
                tail = buf[-40:]
            try:
                res = client.complete({
                    "node_id": self.node_id,
                    "project": pid,
                    "task_id": tid,
                    "attempt": attempt,
                    "changes": changes,
                    "verify": verify_res,
                    "log": tail + ["用时 %.1fs，改动 %d 个文件" % (time.time() - t0, len(changes))],
                })
            except AuthError:
                raise
            except SwarmError as exc:
                self.log("交付被拒（%s），任务可能已被回收" % exc)
                return
            if res.get("already"):
                self.log("任务 %s/%s 之前已交付（幂等）" % (pid, tid))
            elif res.get("failed"):
                self.log("任务 %s/%s 被 hub 判失败（%s）" % (pid, tid, res.get("task_status")))
            else:
                self.log("交付 %s/%s：%d 个文件，%.1fs" %
                         (pid, tid, len(changes), time.time() - t0))
        finally:
            with self._lock:
                self._running.pop(key, None)
                self._running_attempt.pop(key, None)
            if not self.keep_work:
                shutil.rmtree(work, ignore_errors=True)

    def _flush(self, client, pid, tid, buf, lock, attempt=None):
        with lock:
            if not buf:
                return
            lines = buf[:]
            del buf[:]
        try:
            client.progress(tid, lines, project=pid, attempt=attempt)
        except Exception:
            pass

    def _fetch_missing(self, client, snapshot):
        missing = [e for e in snapshot if not self.cache.has(e.get("sha"))]
        if not missing:
            return
        self.log("拉取 %d 个文件" % len(missing))

        def one(entry):
            data = self._pool_client().fetch_blob(entry["sha"])
            self.cache.put_bytes(data)

        errors = []
        futs = [self._pool.submit(one, e) for e in missing]
        for f in futs:
            try:
                f.result()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise SwarmError("拉取文件失败：%s" % errors[0])

    def _push_blobs(self, client, changes):
        """先批量问 hub 有哪些，只传缺的：同一文件全网只传一次。"""
        shas = [c["sha"] for c in changes if not c.get("deleted") and c.get("sha")]
        if not shas:
            return
        exists = client.has_blobs(shas)
        if exists is None:
            exists = {}
        todo = [c for c in changes
                if not c.get("deleted") and c.get("sha") and not exists.get(c["sha"])]
        if not todo:
            return

        def one(ch):
            data = self.cache.get(ch["sha"])
            self._pool_client().push_blob(ch["sha"], data)

        list(self._pool.map(one, todo))

    def _pool_client(self):
        c = getattr(self._pool_local, "client", None)
        if c is None:
            c = self._new_client()
            self._pool_local.client = c
        return c

    @staticmethod
    def _hub_has(client, sha):
        """hub 上已有就跳过上传：同一文件全网只传一次。"""
        try:
            return client.has_blob(sha)
        except Exception:
            return False


def main_from_args(args):
    node = Node(
        root=args.root, hub=args.hub, token=args.token, name=args.name,
        slots=args.slots, kind=args.agent, cmd=args.cmd,
        base_url=args.llm_base, model=args.model, api_key=args.api_key,
        mock_delay=args.mock_delay, discovery_port=args.discovery_port,
        keep_work=args.keep_work, verify=getattr(args, "verify", None),
        tls=getattr(args, "tls", False), tls_pin=getattr(args, "tls_pin", None),
        ca=getattr(args, "ca", None), insecure=getattr(args, "insecure", False),
        once=getattr(args, "once", False),
    )
    return node.run()

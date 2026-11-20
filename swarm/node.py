"""工作节点：跑在每台"已经有 agent 的 Linux"上。

    swarm node --discover --agent cli --cmd 'aider --yes --message {prompt}'

它做四件事：
  1. 加入局域网里的 hub（UDP 自动发现，或者 --hub 指定）
  2. 长轮询领任务（不空转 CPU），多个槽并发跑
  3. 把任务快照铺进自己的临时工作目录（只拉本地缓存里没有的 blob）
  4. 让本机 agent 干活，把改动清单回传，主工程由 hub 合并

节点不碰别人的文件，也碰不到主工程 —— 这是隔离的物理保证。
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

from . import DEFAULT_DISCOVERY_PORT, DEFAULT_PORT, __version__
from . import agents, discover, proto, workspace
from .proto import HubClient, SwarmError
from .store import BlobStore

FETCH_WORKERS = 8


class Node(object):
    def __init__(self, root=None, hub=None, token=None, name=None, slots=2, kind="mock",
                 cmd=None, base_url=None, model=None, api_key=None, mock_delay=0.4,
                 discovery_port=DEFAULT_DISCOVERY_PORT, keep_work=False, verbose=True):
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
        self.mock_delay = float(mock_delay)
        self.discovery_port = int(discovery_port)
        self.keep_work = keep_work
        self.verbose = verbose

        self.stop = threading.Event()
        self.node_id = None
        self.host = None
        self.port = None
        self._running = {}
        self._lock = threading.Lock()
        self._clients = []
        self._threads = []
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
            json.dump({"node_id": self.node_id, "name": self.name, "hub": [self.host, self.port]}, f)
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
            host, port, _info = found
            self.host, self.port = host, port
        else:
            self.host, self.port = proto.parse_hub_addr(self.hub_addr, DEFAULT_PORT)
        if not self.token:
            raise SwarmError("缺少令牌：--token 或环境变量 SWARM_TOKEN")
        self.log("连接 hub %s:%d" % (self.host, self.port))

    def _new_client(self):
        c = HubClient(self.host, self.port, self.token, timeout=60)
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
        self._save_meta()
        self.log("已加入 hub「%s」，节点 id %s，%d 个槽位" % (res.get("hub"), self.node_id, self.slots))
        return res

    # ------------------------------------------------------------------
    def run(self):
        self.resolve_hub()
        client = self._new_client()
        self._rejoin(client)
        hb = threading.Thread(target=self._heartbeat_loop, name="swarm-hb", daemon=True)
        hb.start()
        self._threads.append(hb)
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

    def shutdown(self):
        if self.stop.is_set():
            return
        self.stop.set()
        with self._lock:
            running = list(self._running.items())
        if running:
            self.log("退出前把 %d 个在跑的任务交还队列" % len(running))
        for tid, client in running:
            try:
                client.call("POST", "/api/fail", {
                    "node_id": self.node_id, "task_id": tid,
                    "error": "节点退出，任务交还队列",
                }, timeout=5)
            except Exception:
                pass
        for c in self._clients:
            c.close_all()
        self.log("已断开")

    # ------------------------------------------------------------------
    def _heartbeat_loop(self):
        client = self._new_client()
        while not self.stop.wait(5.0):
            with self._lock:
                running = list(self._running.keys())
            try:
                client.heartbeat({
                    "node_id": self.node_id,
                    "running": running,
                    "slots": self.slots,
                    "kinds": [self.kind],
                })
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
            except Exception as exc:
                traceback.print_exc()
                try:
                    client.fail({"node_id": self.node_id, "task_id": task["id"],
                                 "error": "%s: %s" % (type(exc).__name__, exc)})
                except Exception:
                    pass

    # ------------------------------------------------------------------
    def _execute(self, client, task, snapshot):
        tid = task["id"]
        work = os.path.join(self.work_root, "%s-%s" % (tid, task.get("attempt", 1)))
        shutil.rmtree(work, ignore_errors=True)
        os.makedirs(work, exist_ok=True)
        with self._lock:
            self._running[tid] = client
        t0 = time.time()
        self.log("领到任务 %s「%s」：%d 个文件" % (tid, task["title"], len(snapshot)))
        try:
            self._fetch_missing(client, snapshot)
            workspace.materialize(work, None, snapshot, cache=self.cache)

            buf = []
            lock = threading.RLock()
            last = [0.0]

            def log_line(line):
                with lock:
                    buf.append(str(line)[:300])
                    now = time.time()
                    if now - last[0] > 1.5:
                        last[0] = now
                        self._flush(client, tid, buf, lock)

            ctx = {
                "workdir": work,
                "prompt": task.get("prompt") or task.get("title") or "",
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
                with lock:
                    self._flush(client, tid, buf, lock)
                raise

            write_scope = task.get("write") or ["**"]
            changes = workspace.diff_work(work, snapshot)
            outside = workspace.out_of_scope(changes, write_scope)
            if outside:
                log_line("⚠ 越界修改已丢弃（不在任务范围）：%s" % ", ".join(outside[:6]))
            changes = [c for c in changes if workspace.match_any(c["path"], write_scope)]

            for ch in changes:
                if ch.get("deleted"):
                    continue
                full = os.path.join(work, ch["path"])
                sha = self.cache.put_file(full)
                ch["sha"] = sha
            self._push_blobs(client, changes)

            with lock:
                tail = buf[-40:]
            try:
                client.complete({
                    "node_id": self.node_id,
                    "task_id": tid,
                    "changes": changes,
                    "log": tail + ["用时 %.1fs，改动 %d 个文件" % (time.time() - t0, len(changes))],
                })
                self.log("交付 %s：%d 个文件，%.1fs" % (tid, len(changes), time.time() - t0))
            except SwarmError as exc:
                self.log("交付被拒（%s），任务可能已被回收" % exc)
        finally:
            with self._lock:
                self._running.pop(tid, None)
            if not self.keep_work:
                shutil.rmtree(work, ignore_errors=True)

    def _flush(self, client, tid, buf, lock):
        with lock:
            if not buf:
                return
            lines = buf[:]
            del buf[:]
        try:
            client.progress({"node_id": self.node_id, "task_id": tid, "lines": lines})
        except Exception:
            pass

    def _fetch_missing(self, client, snapshot):
        missing = [e for e in snapshot if not self.cache.has(e["sha"])]
        if not missing:
            return
        self.log("拉取 %d 个文件" % len(missing))

        def one(entry):
            data = client.fetch_blob(entry["sha"])
            self.cache.put_bytes(data)

        errors = []
        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
            futs = [ex.submit(one, e) for e in missing]
            for f in futs:
                try:
                    f.result()
                except Exception as exc:
                    errors.append(exc)
        if errors:
            raise SwarmError("拉取文件失败：%s" % errors[0])

    def _push_blobs(self, client, changes):
        todo = [c for c in changes if not c.get("deleted") and not self._hub_has(c["sha"])]
        if not todo:
            return

        def one(ch):
            data = self.cache.get(ch["sha"])
            client.push_blob(ch["sha"], data)

        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
            list(ex.map(one, todo))

    def _hub_has(self, sha):
        return False


def main_from_args(args):
    node = Node(
        root=args.root, hub=args.hub, token=args.token, name=args.name,
        slots=args.slots, kind=args.agent, cmd=args.cmd,
        base_url=args.llm_base, model=args.model, api_key=args.api_key,
        mock_delay=args.mock_delay, discovery_port=args.discovery_port,
        keep_work=args.keep_work,
    )
    return node.run()

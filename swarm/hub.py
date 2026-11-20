"""调度中心（hub）。

职责：
  1. 收项目 -> 拆任务图（DAG）
  2. 节点长轮询领任务：谁空谁拿，拿到就干，不搞集中式派发
  3. 租约（lease）+ 心跳：节点掉线/App 被杀，任务自动回到队列重派
  4. 收改动清单 -> 逐个 blob 合并进主工程；越界修改、同文件冲突都拦下来
  5. 全程记账，手机浏览器打开 http://<hub>:47770 就能看进度

一条原则：**节点只交差分，不交整个项目；主工程永远只有 hub 在写。**
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import DEFAULT_DISCOVERY_PORT, DEFAULT_PORT, PROTO, __version__
from . import discover, planner, proto, workspace
from .dashboard import DASHBOARD_HTML
from .proto import AuthError, SwarmError
from .store import BlobStore, sha256_bytes

EVENT_KEEP = 500
LOG_KEEP = 400


class Hub(object):
    def __init__(self, root, name=None, token=None, port=DEFAULT_PORT, bind="0.0.0.0",
                 discovery_port=DEFAULT_DISCOVERY_PORT, lease=180, stale_after=25,
                 max_attempts=2, use_git=True, verbose=True):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        self.port = int(port)
        self.bind = bind
        self.discovery_port = int(discovery_port)
        self.name = name or ("%s-hub" % proto.machine_name())
        self.lease = int(lease)
        self.stale_after = int(stale_after)
        self.max_attempts = int(max_attempts)
        self.use_git = use_git
        self.verbose = verbose

        os.makedirs(os.path.join(self.root, "projects"), exist_ok=True)
        self.blobs = BlobStore(os.path.join(self.root, "blobs"))
        self.token = token or self._load_token()

        self.projects = {}
        self.nodes = {}
        self.events = deque(maxlen=EVENT_KEEP)
        self.cond = threading.Condition()
        self.merge_lock = threading.Lock()
        self.stop = threading.Event()
        self._dirty = False
        self._snap_index = {}     # project id -> {(path,size,mtime): sha}
        self.stats = {"claimed": 0, "done": 0, "failed": 0, "conflicts": 0, "blob_bytes": 0}
        self._server = None
        self._threads = []
        self.started_at = time.time()
        self._load_state()

    # ------------------------------------------------------------------
    # 基础
    # ------------------------------------------------------------------
    def _load_token(self):
        path = os.path.join(self.root, "token")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                t = f.read().strip()
            if t:
                return t
        token = sha256_bytes(os.urandom(32))
        with open(path, "w", encoding="utf-8") as f:
            f.write(token + "\n")
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        return token

    def log(self, msg, level="info"):
        item = {"t": time.time(), "lvl": level, "msg": str(msg)[:600]}
        self.events.append(item)
        if self.verbose:
            sys.stderr.write("[hub %s] %s\n" % (level, msg))
            sys.stderr.flush()

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------
    def _state_path(self):
        return os.path.join(self.root, "state.json")

    def _load_state(self):
        path = self._state_path()
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            self.log("状态文件损坏，忽略：%s" % exc, "warn")
            return
        self.projects = data.get("projects") or {}
        for p in self.projects.values():
            for t in p["tasks"].values():
                if t["status"] == "running":
                    t["status"] = "queued"
                    t["node"] = None
                    t["lease"] = 0
        self.log("恢复 %d 个项目的状态" % len(self.projects))

    def _save_state(self):
        data = {"proto": PROTO, "hub": self.name, "projects": self.projects}
        path = self._state_path()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)

    def _touch(self):
        self._dirty = True

    # ------------------------------------------------------------------
    # 项目
    # ------------------------------------------------------------------
    def _new_project_id(self):
        n = 1
        while ("p%d" % n) in self.projects:
            n += 1
        return "p%d" % n

    def create_project(self, title, spec, plan_obj=None, project_id=None):
        pid = project_id or self._new_project_id()
        tree = os.path.join(self.root, "projects", pid, "tree")
        os.makedirs(tree, exist_ok=True)

        plan_obj = plan_obj or planner.plan(spec, title=title)
        plan_obj = planner.validate(plan_obj)
        plan_obj = planner.critical_path_order(plan_obj)

        for name, content in self._starter_files(title, spec):
            full = os.path.join(tree, name)
            os.makedirs(os.path.dirname(full) or tree, exist_ok=True)
            if not os.path.exists(full):
                with open(full, "w", encoding="utf-8") as f:
                    f.write(content)

        tasks = {}
        order = []
        for raw in plan_obj["tasks"]:
            tid = raw["id"]
            order.append(tid)
            tasks[tid] = {
                "id": tid,
                "title": raw.get("title") or tid,
                "prompt": raw.get("prompt") or "",
                "read": raw.get("read") or ["**"],
                "write": raw.get("write") or ["**"],
                "deps": [d for d in (raw.get("deps") or []) if d in [r["id"] for r in plan_obj["tasks"]]],
                "agent": raw.get("agent"),
                "cmd": raw.get("cmd"),
                "model": raw.get("model"),
                "priority": int(raw.get("priority") or 0),
                "status": "queued",
                "attempts": 0,
                "node": None,
                "lease": 0,
                "started": 0,
                "ended": 0,
                "log": [],
                "changes": [],
                "conflicts": [],
                "stats": {},
            }

        project = {
            "id": pid,
            "title": plan_obj.get("title") or title or pid,
            "spec": spec,
            "tree": tree,
            "status": "running",
            "created": time.time(),
            "order": order,
            "tasks": tasks,
        }
        with self.cond:
            self.projects[pid] = project
            self._touch()
            self.cond.notify_all()

        if self.use_git:
            self._git_init(tree, "初始化项目：%s" % project["title"])
        self.log("新项目 %s「%s」：%d 个任务" % (pid, project["title"], len(tasks)))
        return project

    @staticmethod
    def _starter_files(title, spec):
        readme = "# %s\n\n由局域网上多台机器的 agent 协作产出。\n\n## 需求\n\n%s\n" % (
            title or "项目", (spec or "").strip()[:6000])
        arch = "# 架构\n\n> 由 t-core 负责填充：目录结构、模块边界、关键接口。\n"
        return [("README.md", readme), ("docs/architecture.md", arch)]

    # ------------------------------------------------------------------
    # 节点
    # ------------------------------------------------------------------
    def join(self, info):
        nid = info.get("node_id")
        if not nid:
            nid = "n-" + sha256_bytes(os.urandom(8))[:8]
        node = {
            "id": nid,
            "name": info.get("name") or nid,
            "addr": info.get("addr") or "",
            "kinds": info.get("kinds") or ["mock"],
            "slots": int(info.get("slots") or 1),
            "running": [],
            "last_seen": time.time(),
            "joined": time.time(),
            "os": info.get("os") or "",
            "version": info.get("version") or "",
            "done": 0,
            "failed": 0,
        }
        with self.cond:
            old = self.nodes.get(nid)
            if old:
                node["done"] = old.get("done", 0)
                node["failed"] = old.get("failed", 0)
                node["joined"] = old.get("joined", node["joined"])
            self.nodes[nid] = node
            self.cond.notify_all()
        self.log("节点加入：%s（%s，%d 槽，%s）" % (node["name"], node["addr"] or "?", node["slots"], ",".join(node["kinds"])))
        return {
            "node_id": nid,
            "hub": self.name,
            "proto": PROTO,
            "version": __version__,
            "lease": self.lease,
            "server_time": time.time(),
        }

    def heartbeat(self, nid, payload):
        now = time.time()
        with self.cond:
            node = self.nodes.get(nid)
            if not node:
                raise SwarmError("节点未注册，请重新 join")
            node["last_seen"] = now
            node["running"] = list(payload.get("running") or [])[:32]
            node["slots"] = int(payload.get("slots") or node["slots"])
            if payload.get("kinds"):
                node["kinds"] = list(payload["kinds"])[:8]
            for pid, p in self.projects.items():
                for tid in node["running"]:
                    t = p["tasks"].get(tid)
                    if t and t["status"] == "running" and t.get("node") == nid:
                        t["lease"] = now + self.lease
            self.cond.notify_all()
        return {"ok": True, "server_time": now}

    # ------------------------------------------------------------------
    # 领任务
    # ------------------------------------------------------------------
    def claim(self, nid, wait=25):
        deadline = time.time() + max(0, wait)
        with self.cond:
            node = self.nodes.get(nid)
            if not node:
                raise SwarmError("节点未注册")
            node["last_seen"] = time.time()
            while not self.stop.is_set():
                picked = self._pick_locked(nid, node)
                if picked is not None:
                    pid, tid = picked
                    return self._dispatch_locked(pid, tid, nid)
                remain = deadline - time.time()
                if remain <= 0:
                    return {"task": None}
                self.cond.wait(min(remain, 2.0))
        return {"task": None}

    def _pick_locked(self, nid, node):
        kinds = node.get("kinds") or []
        best = None
        best_key = None
        for pid in sorted(self.projects, key=lambda x: self.projects[x]["created"]):
            p = self.projects[pid]
            if p["status"] != "running":
                continue
            running = [p["tasks"][t] for t in p["order"] if p["tasks"][t]["status"] == "running"]
            for tid in p["order"]:
                t = p["tasks"][tid]
                if t["status"] != "queued":
                    continue
                if any(p["tasks"][d]["status"] != "done" for d in t["deps"]):
                    continue
                if t.get("agent") and t["agent"] not in kinds:
                    continue
                if any(workspace.scopes_overlap(t["write"], r["write"]) for r in running):
                    continue
                key = (-int(t.get("priority") or 0), p["created"], p["order"].index(tid))
                if best_key is None or key < best_key:
                    best_key = key
                    best = (pid, tid)
        return best

    def _dispatch_locked(self, pid, tid, nid):
        p = self.projects[pid]
        t = p["tasks"][tid]
        now = time.time()
        index = self._snap_index.setdefault(pid, {})
        entries = workspace.snapshot(p["tree"], self.blobs, patterns=t["read"], index=index)
        # 顺手清掉超出范围的路径
        entries = [e for e in entries if workspace.match_any(e["path"], t["read"])]
        t["status"] = "running"
        t["node"] = nid
        t["lease"] = now + self.lease
        t["started"] = now
        t["attempts"] += 1
        t["base"] = {e["path"]: e["sha"] for e in entries}
        t["changes"] = []
        t["conflicts"] = []
        t["log"] = []
        self.stats["claimed"] += 1
        node = self.nodes.get(nid)
        if node is not None and tid not in node["running"]:
            node["running"] = (node["running"] + [tid])[-32:]
        self._touch()
        self.cond.notify_all()
        return {
            "task": {
                "id": tid,
                "project": pid,
                "title": t["title"],
                "prompt": t["prompt"],
                "read": t["read"],
                "write": t["write"],
                "attempt": t["attempts"],
                "timeout": self.lease * 4,
                "cmd": t.get("cmd"),
                "model": t.get("model"),
            },
            "snapshot": entries,
            "server_time": now,
        }

    # ------------------------------------------------------------------
    # 回传
    # ------------------------------------------------------------------
    def progress(self, nid, payload):
        tid = payload.get("task_id")
        lines = payload.get("lines") or []
        with self.cond:
            for p in self.projects.values():
                t = p["tasks"].get(tid)
                if not t or t.get("node") != nid:
                    continue
                t["log"].extend(str(x)[:300] for x in lines[:50])
                if len(t["log"]) > LOG_KEEP:
                    del t["log"][:len(t["log"]) - LOG_KEEP]
                t["lease"] = time.time() + self.lease
                self._touch()
                return {"ok": True}
        return {"ok": False}

    def complete(self, nid, payload):
        tid = payload.get("task_id")
        changes = payload.get("changes") or []
        with self.cond:
            found = self._find_task(tid)
            if not found:
                raise SwarmError("没有这个任务：%s" % tid)
            pid, p, t = found
            if t.get("node") != nid or t["status"] != "running":
                raise SwarmError("任务 %s 不归你（状态 %s）" % (tid, t["status"]))
            t["log"].extend(str(x)[:300] for x in (payload.get("log") or [])[-40:])
            t["changes"] = changes
            with self.merge_lock:
                applied, conflicts = self._apply_changes(p, t, changes)
                t["conflicts"] = conflicts
                t["stats"] = self._diff_stats(p, t, applied)
                if conflicts:
                    t["status"] = "conflict"
                    self.stats["conflicts"] += 1
                else:
                    t["status"] = "done"
                    self.stats["done"] += 1
                t["ended"] = time.time()
                t["lease"] = 0
                node = self.nodes.get(nid)
                if node:
                    node["running"] = [x for x in node["running"] if x != tid]
                    node["done"] = node.get("done", 0) + 1
                self._after_merge(p)
            self._touch()
            self.cond.notify_all()
        if applied or conflicts:
            self.log("任务 %s 完成：+%d 文件，冲突 %d" % (tid, len(applied), len(conflicts)),
                     "warn" if conflicts else "info")
        return {"ok": True, "applied": len(applied), "conflicts": len(conflicts),
                "task_status": t["status"]}

    def fail(self, nid, payload):
        tid = payload.get("task_id")
        err = str(payload.get("error") or "未知错误")[:2000]
        with self.cond:
            found = self._find_task(tid)
            if not found:
                raise SwarmError("没有这个任务：%s" % tid)
            pid, p, t = found
            if t.get("node") != nid or t["status"] != "running":
                raise SwarmError("任务 %s 不归你" % tid)
            t["log"].append("!! " + err.replace("\n", " ⏎ ")[:300])
            node = self.nodes.get(nid)
            if node:
                node["running"] = [x for x in node["running"] if x != tid]
                node["failed"] = node.get("failed", 0) + 1
            if t["attempts"] >= self.max_attempts:
                t["status"] = "failed"
                t["ended"] = time.time()
                t["lease"] = 0
                self.stats["failed"] += 1
                self.log("任务 %s 失败（已试 %d 次）：%s" % (tid, t["attempts"], err.splitlines()[0] if err else ""), "error")
            else:
                t["status"] = "queued"
                t["node"] = None
                t["lease"] = 0
                self.log("任务 %s 失败，重排队（第 %d 次）" % (tid, t["attempts"]), "warn")
            self._touch()
            self.cond.notify_all()
        return {"ok": True, "status": t["status"]}

    def _find_task(self, tid):
        for pid, p in self.projects.items():
            if tid in p["tasks"]:
                return pid, p, p["tasks"][tid]
        return None

    # ------------------------------------------------------------------
    # 合并
    # ------------------------------------------------------------------
    def _apply_changes(self, p, t, changes):
        tree = p["tree"]
        base = t.get("base") or {}
        applied = []
        conflicts = []
        for ch in changes:
            path = str(ch.get("path") or "").lstrip("/").replace("\\", "/")
            if not path or ".." in path.split("/"):
                conflicts.append({"path": path or "?", "reason": "非法路径"})
                continue
            if not workspace.match_any(path, t["write"]):
                conflicts.append({
                    "path": path, "reason": "越界修改：不在任务范围内 %s" % ", ".join(t["write"]),
                    "theirs": ch.get("sha"), "base": base.get(path),
                    "current": workspace.tree_sha(tree, path), "out_of_scope": True,
                })
                continue
            cur = workspace.tree_sha(tree, path)
            old = base.get(path)
            if ch.get("deleted"):
                if cur is None:
                    continue
                if old is not None and cur != old:
                    conflicts.append({"path": path, "reason": "删除时文件已被别的任务改过",
                                      "theirs": None, "base": old, "current": cur})
                    continue
                try:
                    os.unlink(os.path.join(tree, path))
                    workspace._prune_empty(os.path.dirname(os.path.join(tree, path)), tree)
                    applied.append({"path": path, "deleted": True})
                except OSError as exc:
                    conflicts.append({"path": path, "reason": "删除失败 %s" % exc})
                continue
            sha = ch.get("sha")
            if not sha or not self.blobs.has(sha):
                conflicts.append({"path": path, "reason": "blob 缺失，改动作废"})
                continue
            if cur == sha:
                continue
            if old is None:
                if cur is not None:
                    conflicts.append({"path": path, "reason": "两个任务都新建了同一个文件",
                                      "theirs": sha, "base": None, "current": cur})
                    continue
            elif cur != old:
                conflicts.append({"path": path, "reason": "同一文件被并行改动",
                                  "theirs": sha, "base": old, "current": cur})
                continue
            try:
                workspace.apply_change(tree, {"path": path, "sha": sha, "deleted": False}, self.blobs)
                applied.append({"path": path, "sha": sha, "deleted": False})
            except Exception as exc:
                conflicts.append({"path": path, "reason": "写入失败 %s" % exc})
        for a in applied:
            t["log"].append(("删除 " if a.get("deleted") else "合并 ") + a["path"])
        return applied, conflicts

    def _diff_stats(self, p, t, applied):
        added = removed = 0
        for a in applied:
            if a.get("deleted"):
                removed += 1
                continue
            sha = a.get("sha")
            try:
                new = self.blobs.get(sha)
            except OSError:
                continue
            if len(new) > 300000:
                continue
            try:
                text = new.decode("utf-8")
            except UnicodeDecodeError:
                continue
            added += text.count("\n") + 1
        return {"files": len(applied), "lines": added}

    def _after_merge(self, p):
        if self.use_git:
            msg = "合并任务改动"
            self._git_commit(p["tree"], msg)
        pend = [t for t in p["tasks"].values() if t["status"] in ("queued", "running")]
        if not pend:
            if any(t["status"] == "conflict" for t in p["tasks"].values()):
                p["status"] = "review"
            elif any(t["status"] == "failed" for t in p["tasks"].values()):
                p["status"] = "failed"
            else:
                p["status"] = "done"
                self.log("项目 %s 全部任务完成 🎉" % p["id"])

    def resolve(self, payload):
        pid = payload.get("project")
        tid = payload.get("task")
        path = payload.get("path")
        choice = payload.get("choice") or "theirs"
        with self.cond:
            p = self.projects.get(pid)
            if not p:
                raise SwarmError("没有这个项目")
            t = p["tasks"].get(tid)
            if not t:
                raise SwarmError("没有这个任务")
            item = None
            for c in t["conflicts"]:
                if c.get("path") == path:
                    item = c
                    break
            if item is None:
                raise SwarmError("这个路径没有冲突")
            with self.merge_lock:
                if choice == "theirs" and item.get("theirs") and self.blobs.has(item["theirs"]):
                    workspace.apply_change(p["tree"], {"path": path, "sha": item["theirs"], "deleted": False}, self.blobs)
                t["conflicts"] = [c for c in t["conflicts"] if c.get("path") != path]
                if not t["conflicts"] and t["status"] == "conflict":
                    t["status"] = "done"
                if self.use_git:
                    self._git_commit(p["tree"], "裁决冲突：%s" % path)
                self._after_merge(p)
            self._touch()
            self.cond.notify_all()
        return {"ok": True}

    # ------------------------------------------------------------------
    # git 快照（可选，用于回滚）
    # ------------------------------------------------------------------
    def _git(self, tree, *args, check=False):
        try:
            return subprocess.run(
                ["git", "-C", tree, "-c", "user.name=swarm", "-c", "user.email=swarm@local"] + list(args),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30,
            )
        except Exception:
            return None

    def _git_init(self, tree, msg):
        if not os.path.isdir(os.path.join(tree, ".git")):
            self._git(tree, "init", "-q")
        self._git_commit(tree, msg)

    def _git_commit(self, tree, msg):
        if not os.path.isdir(os.path.join(tree, ".git")):
            return
        self._git(tree, "add", "-A")
        st = self._git(tree, "status", "--porcelain")
        if st is None or st.returncode != 0 or not st.stdout.strip():
            return
        res = self._git(tree, "commit", "-q", "-m", msg)
        if res is not None and res.returncode != 0:
            self.log("git commit 失败：%s" % res.stderr.decode("utf-8", "replace")[:200], "warn")

    # ------------------------------------------------------------------
    # 后台线程
    # ------------------------------------------------------------------
    def _janitor(self):
        while not self.stop.wait(3.0):
            now = time.time()
            with self.cond:
                changed = False
                for nid, node in list(self.nodes.items()):
                    if now - node["last_seen"] <= self.stale_after:
                        continue
                    if node.get("_dead"):
                        continue
                    node["_dead"] = True
                    self.log("节点失联：%s，回收它的任务" % node["name"], "warn")
                    for p in self.projects.values():
                        for t in p["tasks"].values():
                            if t["status"] == "running" and t.get("node") == nid:
                                t["status"] = "queued"
                                t["node"] = None
                                t["lease"] = 0
                                changed = True
                if changed:
                    self._touch()
                    self.cond.notify_all()
            if self._dirty:
                try:
                    self._save_state()
                    self._dirty = False
                except Exception as exc:
                    self.log("保存状态失败：%s" % exc, "warn")

    def start_background(self):
        self.stop.clear()
        t1 = threading.Thread(target=self._janitor, name="swarm-janitor", daemon=True)
        t1.start()
        t2 = threading.Thread(
            target=discover.serve_responder,
            args=(self.discovery_port, {"name": self.name, "port": self.port, "addr": proto.lan_ip()},
                  self.stop, self.log),
            name="swarm-discover", daemon=True)
        t2.start()
        self._threads = [t1, t2]
        self._save_state()
        return self

    def make_server(self):
        hub = self

        class Handler(ApiHandler):
            pass

        Handler.hub = hub
        ThreadingHTTPServer.allow_reuse_address = True
        ThreadingHTTPServer.daemon_threads = True
        srv = ThreadingHTTPServer((self.bind, self.port), Handler)
        self._server = srv
        return srv

    def serve_forever(self):
        srv = self.make_server()
        self.start_background()
        prints = [
            "",
            "  ╭──────────────────────────────────────────────╮",
            "  │  Swarm 调度中心已启动                        │",
            "  ╰──────────────────────────────────────────────╯",
            "  看板   http://%s:%d/   （手机浏览器直接开）" % (proto.lan_ip(), self.port),
            "  地址   %s:%d" % (proto.lan_ip(), self.port),
            "  令牌   %s" % self.token,
            "  自动发现 UDP %d（节点用 --discover 即可，不用填 IP）" % self.discovery_port,
            "",
        ]
        sys.stdout.write("\n".join(prints) + "\n")
        sys.stdout.flush()
        try:
            srv.serve_forever(poll_interval=0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()

    def shutdown(self):
        self.stop.set()
        with self.cond:
            self.cond.notify_all()
        try:
            self._save_state()
        except Exception:
            pass
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        self.log("调度中心已停止")

    # ------------------------------------------------------------------
    # 状态视图
    # ------------------------------------------------------------------
    def snapshot_state(self):
        now = time.time()
        nodes = []
        for n in self.nodes.values():
            nodes.append({
                "id": n["id"], "name": n["name"], "addr": n["addr"],
                "slots": n["slots"], "running": list(n["running"]),
                "online": (now - n["last_seen"]) <= self.stale_after,
                "last_seen": round(now - n["last_seen"], 1),
                "kinds": n["kinds"], "done": n.get("done", 0), "failed": n.get("failed", 0),
                "os": n.get("os", ""),
            })
        nodes.sort(key=lambda x: x["name"])
        projects = []
        for p in sorted(self.projects.values(), key=lambda x: x["created"]):
            counts = {}
            tasks = []
            for tid in p["order"]:
                t = p["tasks"][tid]
                counts[t["status"]] = counts.get(t["status"], 0) + 1
                tasks.append({
                    "id": t["id"], "title": t["title"], "status": t["status"],
                    "node": t.get("node"), "deps": t["deps"], "write": t["write"],
                    "attempts": t["attempts"], "conflicts": t["conflicts"],
                    "stats": t.get("stats") or {}, "started": t.get("started"),
                    "ended": t.get("ended"), "log_tail": (t.get("log") or [])[-6:],
                })
            projects.append({
                "id": p["id"], "title": p["title"], "status": p["status"],
                "created": p["created"], "counts": counts, "tasks": tasks,
                "spec": (p.get("spec") or "")[:1500],
            })
        return {
            "hub": {"name": self.name, "proto": PROTO, "version": __version__,
                    "uptime": round(now - self.started_at, 1), "token_hint": self.token[:6]},
            "nodes": nodes,
            "projects": projects,
            "stats": dict(self.stats),
            "events": list(self.events)[-80:],
        }


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

class ApiHandler(BaseHTTPRequestHandler):
    hub = None
    protocol_version = "HTTP/1.1"
    server_version = "SwarmHub/" + __version__

    def log_message(self, fmt, *args):
        pass

    # -- 输出 -----------------------------------------------------------
    def _send(self, status, body, ctype="application/json; charset=utf-8", headers=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _json(self, status, obj):
        self._send(status, json.dumps(obj, ensure_ascii=False))

    def _err(self, status, msg):
        self._json(status, {"error": msg})

    # -- 鉴权 -----------------------------------------------------------
    DASHBOARD_PATHS = {"/api/state", "/api/control", "/api/resolve", "/api/submit", "/api/tasklog"}

    def _session_cookie(self):
        val = hmac.new(self.hub.token.encode("utf-8"), b"dashboard", hashlib.sha256).hexdigest()
        return "swarm_session=%s; Path=/; Max-Age=2592000; SameSite=Lax" % val

    def _dashboard_ok(self):
        cookie = self.headers.get("Cookie") or ""
        want = hmac.new(self.hub.token.encode("utf-8"), b"dashboard", hashlib.sha256).hexdigest()
        for part in cookie.split(";"):
            name, _, val = part.strip().partition("=")
            if name == "swarm_session" and hmac.compare_digest(val, want):
                return True
        return False

    def _auth(self, body, path):
        ts = self.headers.get("X-Swarm-Ts")
        sig = self.headers.get("X-Swarm-Sig")
        proto.verify(self.hub.token, ts or "", sig or "", self.command, path, body)

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length > proto.BODY_LIMIT:
            raise SwarmError("请求体过大")
        return self.rfile.read(length) if length else b""

    # -- 路由 -----------------------------------------------------------
    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def _route(self, method):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            body = b""
            if method == "POST":
                body = self._body()

            if path == "/" or path == "/index.html":
                supplied = (query.get("t") or [""])[0]
                if supplied and hmac.compare_digest(supplied, self.hub.token):
                    self.send_response(302)
                    self.send_header("Location", "/")
                    self.send_header("Set-Cookie", self._session_cookie())
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return None
                return self._send(200, DASHBOARD_HTML, "text/html; charset=utf-8")
            if path == "/favicon.ico":
                return self._send(204, b"")
            if path == "/healthz":
                return self._send(200, "ok", "text/plain")
            if path == "/api/ping":
                return self._json(200, {"ok": True, "hub": self.hub.name, "proto": PROTO,
                                        "version": __version__, "server_time": time.time()})

            if not (self._dashboard_ok() and path in self.DASHBOARD_PATHS):
                self._auth(body, self.path)

            if path.startswith("/api/blob/"):
                return self._blob(method, path, body)

            payload = {}
            if body:
                try:
                    payload = json.loads(body.decode("utf-8"))
                except Exception:
                    return self._err(400, "body 不是合法 JSON")

            if path == "/api/join" and method == "POST":
                payload.setdefault("addr", self.client_address[0] + ":" + str(self.client_address[1]))
                return self._json(200, self.hub.join(payload))
            if path == "/api/heartbeat" and method == "POST":
                return self._json(200, self.hub.heartbeat(payload.get("node_id"), payload))
            if path == "/api/claim" and method == "POST":
                wait = int((query.get("wait") or ["25"])[0])
                return self._json(200, self.hub.claim(payload.get("node_id"), wait=wait))
            if path == "/api/progress" and method == "POST":
                return self._json(200, self.hub.progress(payload.get("node_id"), payload))
            if path == "/api/complete" and method == "POST":
                return self._json(200, self.hub.complete(payload.get("node_id"), payload))
            if path == "/api/fail" and method == "POST":
                return self._json(200, self.hub.fail(payload.get("node_id"), payload))
            if path == "/api/state" and method == "GET":
                return self._json(200, self.hub.snapshot_state())
            if path == "/api/submit" and method == "POST":
                plan_obj = payload.get("plan")
                p = self.hub.create_project(payload.get("title") or "未命名项目",
                                            payload.get("spec") or "", plan_obj)
                return self._json(200, {"project": p["id"], "title": p["title"],
                                        "tasks": len(p["tasks"])})
            if path == "/api/control" and method == "POST":
                return self._json(200, self._control(payload))
            if path == "/api/resolve" and method == "POST":
                return self._json(200, self.hub.resolve(payload))
            if path == "/api/tasklog" and method == "GET":
                pid = (query.get("project") or [""])[0]
                tid = (query.get("task") or [""])[0]
                p = self.hub.projects.get(pid)
                if not p or tid not in p["tasks"]:
                    return self._err(404, "没有这个任务")
                t = p["tasks"][tid]
                return self._json(200, {"id": tid, "title": t["title"], "status": t["status"],
                                        "changes": t.get("changes") or [],
                                        "conflicts": t.get("conflicts") or [],
                                        "log": t.get("log") or []})
            return self._err(404, "没有这个接口：%s" % path)
        except AuthError as exc:
            return self._err(401, str(exc))
        except SwarmError as exc:
            return self._err(400, str(exc))
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception as exc:
            traceback.print_exc()
            return self._err(500, "hub 内部错误：%s" % exc)

    def _control(self, payload):
        pid = payload.get("project")
        action = payload.get("action")
        hub = self.hub
        with hub.cond:
            p = hub.projects.get(pid)
            if not p:
                raise SwarmError("没有这个项目")
            if action == "pause":
                p["status"] = "paused"
            elif action == "resume":
                p["status"] = "running"
            elif action == "retry":
                tid = payload.get("task")
                t = p["tasks"].get(tid)
                if not t:
                    raise SwarmError("没有这个任务")
                t["status"] = "queued"
                t["attempts"] = 0
                t["node"] = None
                t["lease"] = 0
            elif action == "cancel":
                p["status"] = "cancelled"
                for t in p["tasks"].values():
                    if t["status"] in ("queued", "running"):
                        t["status"] = "cancelled"
                        t["node"] = None
            elif action == "finish":
                p["status"] = "done"
            else:
                raise SwarmError("未知动作：%s" % action)
            hub._touch()
            hub.cond.notify_all()
        return {"ok": True}

    def _blob(self, method, path, body):
        sha = path.rsplit("/", 1)[-1]
        if not re.match(r"^[0-9a-f]{64}$", sha):
            return self._err(400, "sha 非法")
        store = self.hub.blobs
        if method == "GET":
            if not store.has(sha):
                return self._err(404, "没有这个 blob")
            data = store.get(sha)
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True
            self.hub.stats["blob_bytes"] += len(data)
            return None
        if method == "POST":
            if not body or sha256_bytes(body) != sha:
                return self._err(400, "内容与 sha 不符")
            store.put_bytes(body)
            self.hub.stats["blob_bytes"] += len(body)
            return self._json(200, {"ok": True})
        return self._err(405, "不支持的方法")

"""调度中心（hub）。

职责：
  1. 收项目 -> 拆任务图（DAG）；也可以直接导入一个已有仓库
  2. 节点长轮询领任务：谁空谁拿，拿到就干，不搞集中式派发
  3. 租约（lease）+ 心跳：节点掉线、App 被杀、agent 卡死，任务都会回到队列
  4. 收改动清单 -> 逐个 blob 合并进主工程；越界修改、同文件冲突都拦下来
  5. 合并可回滚、冲突可看 diff、任务可验收（verify）
  6. 全程记账，手机浏览器打开 http://<hub>:47770 就能看进度

两条原则：
  **节点只交差分，不交整个项目；主工程永远只有 hub 在写。**
  **任务键是 (project_id, task_id)，任何时刻都能唯一定位一条任务。**

状态里的每个任务都会经历：
    queued -> preparing -> running -> done / conflict -> done
                              \\-> queued（租约过期/节点掉线）
                              \\-> failed（重试用尽）-> 下游 blocked
"""

from __future__ import annotations

import base64
import difflib
import hmac
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import traceback
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import DEFAULT_DISCOVERY_PORT, DEFAULT_PORT, PROTO, __version__
from . import discover, planner, proto, workspace
from .dashboard import DASHBOARD_HTML
from .proto import AuthError, ForbiddenError, SwarmError
from .store import BlobStore, is_sha, sha256_bytes, sha256_file

EVENT_KEEP = 500
LOG_KEEP = 400
MAX_SPEC = 500 << 10
MAX_IMPORT = 128 << 20


class Hub(object):
    def __init__(self, root, name=None, token=None, port=DEFAULT_PORT, bind="0.0.0.0",
                 discovery_port=DEFAULT_DISCOVERY_PORT, lease=180, stale_after=25,
                 max_attempts=2, use_git=True, verbose=True, allow_empty=False,
                 session_ttl=12 * 3600):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        self.port = int(port)
        self.bind = bind
        self.discovery_port = int(discovery_port)
        self.name = name or ("%s-hub" % proto.machine_name())
        self.lease = max(5, int(lease))
        self.stale_after = max(3, int(stale_after))
        self.max_attempts = max(1, int(max_attempts))
        self.use_git = use_git
        self.verbose = verbose
        self.allow_empty = bool(allow_empty)
        self.session_ttl = max(60, int(session_ttl))

        os.makedirs(os.path.join(self.root, "projects"), exist_ok=True)
        self.blobs = BlobStore(os.path.join(self.root, "blobs"))
        self.token = token or self._load_token()

        self.projects = {}
        self.nodes = {}
        self.events = deque(maxlen=EVENT_KEEP)
        self.merges = deque(maxlen=200)
        self.cond = threading.Condition()
        self.merge_lock = threading.Lock()
        self.stop = threading.Event()
        self._dirty = False
        self._snap_index = {}     # project id -> {(path,size,mtime): sha}
        self._snap_lock = threading.RLock()
        self._ignore_cache = {}   # project id -> (loaded_at, IgnoreRules)
        self.stats = {"claimed": 0, "done": 0, "failed": 0, "conflicts": 0,
                      "blob_in": 0, "blob_out": 0}
        self._stats_lock = threading.Lock()
        self._save_lock = threading.Lock()
        self._tokens_lock = threading.RLock()
        self._sessions_lock = threading.Lock()
        self._seen_lock = threading.Lock()
        self.sessions = {}
        self._seen_sigs = {}
        self._requests = {}
        self._requests_lock = threading.Lock()
        self.tokens = self._load_tokens()
        self._server = None
        self._threads = []
        self.started_at = time.time()
        self._load_state()

    # ------------------------------------------------------------------
    # 基础
    # ------------------------------------------------------------------
    def log(self, msg, level="info"):
        item = {"t": time.time(), "lvl": level, "msg": str(msg)[:600]}
        self.events.append(item)
        if self.verbose:
            sys.stderr.write("[hub %s] %s\n" % (level, msg))
            sys.stderr.flush()

    def _bump(self, key, n=1):
        with self._stats_lock:
            self.stats[key] = self.stats.get(key, 0) + n

    def _add_blob_bytes(self, key, n):
        with self._stats_lock:
            self.stats[key] = self.stats.get(key, 0) + int(n)

    # ------------------------------------------------------------------
    # 令牌 / 会话 / 防重放
    # ------------------------------------------------------------------
    def _load_token(self):
        path = os.path.join(self.root, "token")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    t = f.read().strip()
                if t:
                    os.chmod(path, 0o600)
                    return t
            except OSError:
                pass
        token = sha256_bytes(os.urandom(32))
        with open(path, "w", encoding="utf-8") as f:
            f.write(token + "\n")
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        return token

    def _tokens_path(self):
        return os.path.join(self.root, "tokens.json")

    def _load_tokens(self):
        path = self._tokens_path()
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            self.log("tokens.json 损坏，忽略：%s" % exc, "warn")
            return []
        out = []
        for raw in data.get("tokens") or []:
            if not isinstance(raw, dict) or not raw.get("token") or not raw.get("name"):
                continue
            out.append({
                "name": str(raw["name"])[:80],
                "token": str(raw["token"]),
                "node": (str(raw.get("node")).strip() or None) if raw.get("node") else None,
                "created": float(raw.get("created") or time.time()),
                "revoked": bool(raw.get("revoked")),
            })
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        return out

    def _save_tokens_locked(self):
        data = {"proto": PROTO, "tokens": self.tokens}
        path = self._tokens_path()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        os.replace(tmp, path)

    def active_tokens(self):
        with self._tokens_lock:
            return [e for e in self.tokens if not e.get("revoked")]

    def token_view(self):
        with self._tokens_lock:
            out = []
            for e in self.tokens:
                out.append({
                    "name": e["name"],
                    "node": e.get("node"),
                    "created": e.get("created"),
                    "revoked": bool(e.get("revoked")),
                    "hint": e["token"][:8] + "…",
                })
            return out

    def create_node_token(self, name, node=None):
        name = str(name or "").strip()
        if not name:
            raise SwarmError("令牌要有名字")
        with self._tokens_lock:
            if any(e["name"] == name and not e.get("revoked") for e in self.tokens):
                raise SwarmError("已存在同名令牌：%s" % name)
            entry = {
                "name": name[:80],
                "token": os.urandom(24).hex(),
                "node": str(node).strip() or None if node else None,
                "created": time.time(),
                "revoked": False,
            }
            self.tokens.append(entry)
            self._save_tokens_locked()
            return dict(entry)

    def revoke_token(self, key):
        key = str(key or "").strip()
        if not key:
            raise SwarmError("要给令牌名字、完整值或前 8 位")
        with self._tokens_lock:
            hit = None
            for e in self.tokens:
                if e.get("revoked"):
                    continue
                if key in (e["name"], e["token"]) or e["token"].startswith(key):
                    hit = e
                    break
            if hit is None:
                raise SwarmError("没有找到这个令牌")
            hit["revoked"] = True
            self._save_tokens_locked()
            self.log("节点令牌已吊销：%s" % hit["name"], "warn")
            return hit["name"]

    def bind_token(self, entry, nid):
        if not entry:
            return
        with self._tokens_lock:
            bound = entry.get("node")
            if bound and bound != nid:
                raise ForbiddenError("这个令牌只属于节点 %s" % bound)
            if entry.get("node") != nid:
                entry["node"] = nid
                self._save_tokens_locked()

    # -- 看板会话（随机、可注销、HttpOnly） -----------------------------
    def new_session(self):
        val = os.urandom(24).hex()
        now = time.time()
        with self._sessions_lock:
            for k in [k for k, exp in self.sessions.items() if exp < now]:
                self.sessions.pop(k, None)
            self.sessions[val] = now + self.session_ttl
        return val

    def session_valid(self, val):
        if not val:
            return False
        now = time.time()
        with self._sessions_lock:
            exp = self.sessions.get(val)
            if not exp:
                return False
            if exp < now:
                self.sessions.pop(val, None)
                return False
            return True

    def drop_session(self, val):
        with self._sessions_lock:
            self.sessions.pop(val, None)

    def drop_all_sessions(self):
        with self._sessions_lock:
            self.sessions.clear()

    # -- 幂等（网络重试不会重复建项目） --------------------------------
    def request_result(self, rid):
        rid = str(rid or "").strip()
        if not rid:
            return None
        with self._requests_lock:
            item = self._requests.get(rid)
            if not item:
                return None
            if time.time() - item[0] > 6 * 3600:
                self._requests.pop(rid, None)
                return None
            return dict(item[1])

    def remember_request(self, rid, result):
        rid = str(rid or "").strip()
        if not rid:
            return
        with self._requests_lock:
            if len(self._requests) > 1000:
                now = time.time()
                self._requests = {k: v for k, v in self._requests.items()
                                  if now - v[0] < 6 * 3600}
            self._requests[rid] = (time.time(), dict(result))

    # -- 防重放 ---------------------------------------------------------
    def check_and_remember_sig(self, sig):
        """返回 True 表示这个签名已经见过（重放）。"""
        if not sig:
            return False
        now = time.time()
        with self._seen_lock:
            exp = self._seen_sigs.get(sig)
            if exp and exp > now:
                return True
            if len(self._seen_sigs) > 20000:
                self._seen_sigs = {k: v for k, v in self._seen_sigs.items() if v > now}
            self._seen_sigs[sig] = now + 2 * proto.MAX_SKEW + 60
            return False

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------
    def _state_path(self):
        return os.path.join(self.root, "state.json")

    def _load_state(self):
        path = self._state_path()
        data = None
        for cand in (path, path + ".bak"):
            if not os.path.exists(cand):
                continue
            try:
                with open(cand, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if cand.endswith(".bak"):
                    self.log("主状态文件损坏，已从 state.json.bak 恢复", "warn")
                break
            except Exception as exc:
                self.log("状态文件 %s 损坏：%s" % (os.path.basename(cand), exc), "warn")
                data = None
        if data is None:
            return
        with self.cond:
            raw_projects = data.get("projects") or {}
            if not isinstance(raw_projects, dict):
                raw_projects = {}
            for pid, p in raw_projects.items():
                if not isinstance(p, dict):
                    continue
                if not p.get("tree"):
                    p["tree"] = os.path.join(self.root, "projects", pid, "tree")
                p.setdefault("title", pid)
                p.setdefault("spec", "")
                p.setdefault("created", time.time())
                p.setdefault("order", [])
                p.setdefault("tasks", {})
                p["status"] = str(p.get("status") or "running")
                if p["status"] in ("done", "failed", "review", "cancelled", "paused"):
                    pass
                for tid, t in list(p["tasks"].items()):
                    if not isinstance(t, dict):
                        p["tasks"].pop(tid, None)
                        continue
                    t["id"] = str(t.get("id") or tid)
                    t.setdefault("title", t["id"])
                    t.setdefault("prompt", "")
                    if not isinstance(t.get("read"), list):
                        t["read"] = ["**"]
                    if not isinstance(t.get("write"), list):
                        t["write"] = ["**"]
                    if not isinstance(t.get("deps"), list):
                        t["deps"] = []
                    t.setdefault("read", ["**"])
                    t.setdefault("write", ["**"])
                    t.setdefault("deps", [])
                    t.setdefault("attempts", 0)
                    t.setdefault("fails", 0)
                    t.setdefault("node", None)
                    t.setdefault("lease", 0)
                    t.setdefault("started", 0)
                    t.setdefault("ended", 0)
                    t.setdefault("log", [])
                    t.setdefault("changes", [])
                    t.setdefault("conflicts", [])
                    t.setdefault("stats", {})
                    t.setdefault("applied_record", [])
                    t.setdefault("verify", None)
                    t.setdefault("committing", False)
                    t.setdefault("priority", 0)
                    if t["status"] in ("running", "preparing"):
                        t["status"] = "queued"
                        t["node"] = None
                        t["lease"] = 0
                        t["committing"] = False
                order = [tid for tid in p["order"] if tid in p["tasks"]]
                for tid in p["tasks"]:
                    if tid not in order:
                        order.append(tid)
                p["order"] = order
                p["_order"] = {tid: i for i, tid in enumerate(order)}
                if not os.path.isdir(p["tree"]):
                    p["status"] = "failed"
                    for t in p["tasks"].values():
                        if t["status"] not in ("done", "cancelled"):
                            t["status"] = "failed"
                    self.log("项目 %s 的目录不见了：%s" % (pid, p["tree"]), "error")
                else:
                    self._finalize_locked(p)
            self.projects = raw_projects
        self.log("恢复 %d 个项目的状态" % len(self.projects))

    def _save_state(self):
        with self.cond:
            blob = json.dumps({
                "proto": PROTO,
                "hub": self.name,
                "saved": time.time(),
                "projects": self.projects,
            }, ensure_ascii=False)
        with self._save_lock:
            path = self._state_path()
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(blob)
            if os.path.exists(path):
                try:
                    os.replace(path, path + ".bak")
                except OSError:
                    pass
            os.replace(tmp, path)
        self._dirty = False

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

    def _plan(self, spec, title, plan_obj):
        try:
            plan_obj = plan_obj or planner.plan(spec, title=title)
            plan_obj = planner.validate(plan_obj)
            plan_obj = planner.critical_path_order(plan_obj)
        except (ValueError, TypeError, AttributeError, KeyError) as exc:
            raise SwarmError("计划不合法：%s" % exc)
        return plan_obj

    def create_project(self, title, spec, plan_obj=None, project_id=None, seed_dir=None):
        spec = (spec or "")[:MAX_SPEC]
        plan_obj = self._plan(spec, title, plan_obj)
        if project_id is not None:
            project_id = str(project_id).strip()
            if not re.match(r"^[0-9A-Za-z][0-9A-Za-z._-]{0,63}$", project_id) or ".." in project_id:
                raise SwarmError("项目 id 只能是字母数字._-（不能含 ..）：%r" % project_id)
        pid = None
        with self.cond:
            pid = project_id or self._new_project_id()
            if pid in self.projects:
                raise SwarmError("项目 id 已存在：%s" % pid)
            tree = os.path.join(self.root, "projects", pid, "tree")
            project = {
                "id": pid,
                "title": plan_obj.get("title") or title or pid,
                "spec": spec,
                "tree": tree,
                "status": "creating",
                "created": time.time(),
                "ended": 0,
                "order": [],
                "tasks": {},
                "_order": {},
            }
            # 先占位，避免并发提交拿到同一个 pid
            self.projects[pid] = project
        try:
            os.makedirs(tree, exist_ok=True)
            if seed_dir:
                ignore = workspace.load_ignores(seed_dir)
                copied = workspace.copy_tree(seed_dir, tree, ignore=ignore)
                if copied:
                    self.log("导入已有文件 %d 个到项目 %s" % (copied, pid))
            for name, content in self._starter_files(project["title"], spec):
                full = os.path.join(tree, name)
                os.makedirs(os.path.dirname(full) or tree, exist_ok=True)
                if not os.path.exists(full):
                    with open(full, "w", encoding="utf-8") as f:
                        f.write(content)

            tasks = {}
            order = []
            raw_ids = [str(r["id"]) for r in plan_obj["tasks"]]
            for raw in plan_obj["tasks"]:
                tid = str(raw["id"])
                order.append(tid)
                tasks[tid] = {
                    "id": tid,
                    "title": raw.get("title") or tid,
                    "prompt": raw.get("prompt") or "",
                    "read": list(raw.get("read") or ["**"]),
                    "write": list(raw.get("write") or ["**"]),
                    "deps": [d for d in (raw.get("deps") or []) if d in raw_ids],
                    "agent": raw.get("agent"),
                    "cmd": raw.get("cmd"),
                    "model": raw.get("model"),
                    "verify": raw.get("verify"),
                    "priority": int(raw.get("priority") or 0),
                    "status": "queued",
                    "attempts": 0,
                    "fails": 0,
                    "node": None,
                    "lease": 0,
                    "started": 0,
                    "ended": 0,
                    "log": [],
                    "changes": [],
                    "conflicts": [],
                    "applied_record": [],
                    "committing": False,
                    "verify": raw.get("verify"),
                    "stats": {},
                    "base": {},
                }
        except Exception:
            with self.cond:
                self.projects.pop(pid, None)
                self._touch()
                self.cond.notify_all()
            raise

        with self.cond:
            project["tasks"] = tasks
            project["order"] = order
            project["_order"] = {tid: i for i, tid in enumerate(order)}
            project["status"] = "running"
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
    @staticmethod
    def _norm_node_id(nid):
        nid = str(nid or "").strip()
        if not nid:
            return None
        nid = re.sub(r"[^0-9A-Za-z._-]", "-", nid)[:64]
        return nid or None

    def join(self, info, ident=None):
        info = info or {}
        nid = self._norm_node_id(info.get("node_id"))
        if ident and ident.get("role") == "node":
            entry = ident.get("entry")
            bound = (entry or {}).get("node")
            if bound:
                if nid and nid != bound:
                    raise ForbiddenError("这个令牌只属于节点 %s" % bound)
                nid = bound
            if not nid:
                nid = "n-" + sha256_bytes(os.urandom(8))[:8]
            self.bind_token(entry, nid)
        if not nid:
            nid = "n-" + sha256_bytes(os.urandom(8))[:8]
        try:
            slots = int(info.get("slots") or 1)
        except (TypeError, ValueError):
            raise SwarmError("slots 必须是整数")
        kinds = info.get("kinds") or ["mock"]
        if not isinstance(kinds, list):
            kinds = ["mock"]
        node = {
            "id": nid,
            "name": str(info.get("name") or nid)[:80],
            "addr": str(info.get("addr") or "")[:120],
            "kinds": [str(k)[:32] for k in kinds][:8],
            "slots": max(1, min(64, slots)),
            "running": [],
            "last_seen": time.time(),
            "joined": time.time(),
            "os": str(info.get("os") or "")[:120],
            "version": str(info.get("version") or "")[:32],
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
        self.log("节点加入：%s（%s，%d 槽，%s）" %
                 (node["name"], node["addr"] or "?", node["slots"], ",".join(node["kinds"])))
        return {
            "node_id": nid,
            "hub": self.name,
            "proto": PROTO,
            "version": __version__,
            "lease": self.lease,
            "server_time": time.time(),
        }

    def heartbeat(self, nid, payload, ident=None):
        if ident and ident.get("role") == "node" and ident.get("node") and nid != ident["node"]:
            raise ForbiddenError("这个令牌只属于节点 %s" % ident["node"])
        payload = payload or {}
        now = time.time()
        refs = []
        for item in (payload.get("running") or [])[:64]:
            if isinstance(item, dict):
                pid = str(item.get("project") or "").strip()
                tid = str(item.get("task") or "").strip()
            else:
                pid = ""
                tid = str(item).strip()
            if not tid:
                continue
            refs.append({"project": pid, "task": tid})
        with self.cond:
            node = self.nodes.get(nid)
            if not node:
                raise SwarmError("节点未注册，请重新 join")
            node["last_seen"] = now
            node["_dead"] = False
            try:
                node["slots"] = max(1, min(64, int(payload.get("slots") or node["slots"])))
            except (TypeError, ValueError):
                pass
            if isinstance(payload.get("kinds"), list):
                node["kinds"] = [str(k)[:32] for k in payload["kinds"]][:8]
            for ref in refs:
                t = None
                if ref["project"]:
                    t = self.projects.get(ref["project"], {}).get("tasks", {}).get(ref["task"])
                else:
                    for p in self.projects.values():
                        cand = p["tasks"].get(ref["task"])
                        if cand and cand.get("node") == nid and \
                                cand["status"] in ("preparing", "running"):
                            t = cand
                            break
                if t and t.get("node") == nid and t["status"] in ("preparing", "running"):
                    t["lease"] = now + self.lease
            node["running"] = refs
            self.cond.notify_all()
        return {"ok": True, "server_time": now, "lease": self.lease}

    # ------------------------------------------------------------------
    # 领任务
    # ------------------------------------------------------------------
    def claim(self, nid, wait=25, ident=None):
        if ident and ident.get("role") == "node" and ident.get("node") and nid != ident["node"]:
            raise ForbiddenError("这个令牌只属于节点 %s" % ident["node"])
        try:
            wait = int(wait)
        except (TypeError, ValueError):
            wait = 25
        deadline = time.time() + max(0, min(wait, 120))
        picked = None
        with self.cond:
            node = self.nodes.get(nid)
            if not node:
                raise SwarmError("节点未注册")
            node["last_seen"] = time.time()
            node["_dead"] = False
            while not self.stop.is_set():
                picked = self._pick_locked(nid, node)
                if picked is not None:
                    pid, tid = picked
                    self._reserve_locked(pid, tid, nid)
                    break
                remain = deadline - time.time()
                if remain <= 0:
                    return {"task": None, "server_time": time.time()}
                self.cond.wait(min(remain, 2.0))
        if not picked:
            return {"task": None, "server_time": time.time()}
        return self._prepare_and_start(picked[0], picked[1], nid)

    def _node_busy_locked(self, nid):
        n = 0
        for p in self.projects.values():
            for t in p["tasks"].values():
                if t.get("node") == nid and t["status"] in ("preparing", "running"):
                    n += 1
        return n

    def _pick_locked(self, nid, node):
        if self._node_busy_locked(nid) >= int(node.get("slots") or 1):
            return None
        kinds = set(node.get("kinds") or [])
        best = None
        best_key = None
        for pid in sorted(self.projects, key=lambda x: self.projects[x]["created"]):
            p = self.projects[pid]
            if p["status"] != "running":
                continue
            running = [p["tasks"][tid] for tid in p["order"]
                       if p["tasks"][tid]["status"] in ("preparing", "running")]
            idx = p.get("_order") or {tid: i for i, tid in enumerate(p["order"])}
            for tid in p["order"]:
                t = p["tasks"][tid]
                if t["status"] != "queued":
                    continue
                if any(p["tasks"].get(d, {}).get("status") != "done" for d in t["deps"]):
                    continue
                if t.get("agent") and t["agent"] not in kinds:
                    continue
                if any(workspace.scopes_overlap(t["write"], r["write"]) for r in running):
                    continue
                key = (-int(t.get("priority") or 0), p["created"], idx.get(tid, 0))
                if best_key is None or key < best_key:
                    best_key = key
                    best = (pid, tid)
        return best

    def _reserve_locked(self, pid, tid, nid):
        p = self.projects[pid]
        t = p["tasks"][tid]
        t["status"] = "preparing"
        t["node"] = nid
        t["lease"] = time.time() + self.lease
        t["changes"] = []
        t["conflicts"] = []
        t["log"] = []
        self._touch()
        self.cond.notify_all()

    def _ignores_for(self, pid, tree):
        now = time.time()
        with self._snap_lock:
            cached = self._ignore_cache.get(pid)
            if cached and now - cached[0] < 5.0:
                return cached[1]
            rules = workspace.load_ignores(tree)
            self._ignore_cache[pid] = (now, rules)
            return rules

    def _prepare_and_start(self, pid, tid, nid):
        with self.cond:
            p = self.projects.get(pid)
            if not p or tid not in p["tasks"]:
                raise SwarmError("任务不存在（可能项目被删）")
            t = p["tasks"][tid]
            if t["status"] != "preparing" or t.get("node") != nid:
                raise SwarmError("任务已被回收")
            patterns = []
            for pat in list(t["read"]) + list(t["write"]):
                if pat not in patterns:
                    patterns.append(pat)
            tree = p["tree"]
        ignore = self._ignores_for(pid, tree)
        try:
            with self._snap_lock:
                index = self._snap_index.setdefault(pid, {})
                entries = workspace.snapshot(tree, self.blobs, patterns=patterns,
                                             index=index, ignore=ignore)
                if len(index) > 20000:
                    self._snap_index[pid] = {}
        except Exception as exc:
            self._requeue(pid, tid, "生成任务快照失败：%s" % exc)
            raise SwarmError("生成任务快照失败：%s" % exc)

        now = time.time()
        with self.cond:
            p = self.projects.get(pid)
            if not p or tid not in p["tasks"]:
                raise SwarmError("任务不存在")
            t = p["tasks"][tid]
            if t["status"] != "preparing" or t.get("node") != nid:
                raise SwarmError("任务已被回收")
            t["status"] = "running"
            t["started"] = now
            t["attempts"] = int(t.get("attempts") or 0) + 1
            t["base"] = {e["path"]: e["sha"] for e in entries}
            t["lease"] = now + self.lease
            t["ended"] = 0
            self._bump("claimed")
            node = self.nodes.get(nid)
            if node is not None:
                refs = [r for r in node.get("running") or []
                        if not (r.get("project") == pid and r.get("task") == tid)]
                refs.append({"project": pid, "task": tid})
                node["running"] = refs[-64:]
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
                    "timeout": min(self.lease * 4, 7200),
                    "cmd": t.get("cmd"),
                    "model": t.get("model"),
                    "verify": t.get("verify"),
                },
                "snapshot": entries,
                "server_time": now,
                "lease": self.lease,
            }

    def _dispatch_locked(self, pid, tid, nid):
        """同步派发（测试/内部使用）。"""
        with self.cond:
            p = self.projects.get(pid)
            if not p or tid not in p["tasks"]:
                raise SwarmError("没有这个任务")
            if p["tasks"][tid]["status"] not in ("queued", "blocked"):
                raise SwarmError("任务状态不是 queued")
            self._reserve_locked(pid, tid, nid)
        return self._prepare_and_start(pid, tid, nid)

    def _requeue(self, pid, tid, reason, burn_attempt=True):
        with self.cond:
            p = self.projects.get(pid)
            if not p or tid not in p["tasks"]:
                return
            t = p["tasks"][tid]
            if t["status"] not in ("preparing", "running"):
                return
            nid = t.get("node")
            t["status"] = "queued"
            t["node"] = None
            t["lease"] = 0
            if burn_attempt:
                t.setdefault("log", []).append("!! " + str(reason)[:300])
            if nid and nid in self.nodes:
                self.nodes[nid]["running"] = [
                    r for r in self.nodes[nid].get("running") or []
                    if not (r.get("project") == pid and r.get("task") == tid)]
            self._touch()
            self.cond.notify_all()

    # ------------------------------------------------------------------
    # 回传
    # ------------------------------------------------------------------
    def _locate(self, payload):
        tid = str(payload.get("task_id") or payload.get("task") or "").strip()
        if not tid:
            raise SwarmError("缺少 task_id")
        pid = str(payload.get("project") or "").strip()
        if pid:
            p = self.projects.get(pid)
            if not p:
                raise SwarmError("没有这个项目：%s" % pid)
            t = p["tasks"].get(tid)
            if not t:
                raise SwarmError("项目 %s 里没有任务 %s" % (pid, tid))
            return pid, p, t
        matches = [x for x in ((pid2, p2, p2["tasks"][tid])
                               for pid2, p2 in self.projects.items() if tid in p2["tasks"])]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise SwarmError("没有这个任务：%s" % tid)
        raise SwarmError("任务 id %s 在多个项目里重复，请带上 project 字段" % tid)

    def progress(self, nid, payload, ident=None):
        if ident and ident.get("role") == "node" and ident.get("node") and nid != ident["node"]:
            raise ForbiddenError("这个令牌只属于节点 %s" % ident["node"])
        payload = payload or {}
        lines = payload.get("lines") or []
        with self.cond:
            pid, p, t = self._locate(payload)
            if t.get("node") != nid or t["status"] not in ("preparing", "running"):
                return {"ok": False, "reason": "任务已不在你手上"}
            t["log"].extend(str(x)[:300] for x in list(lines)[:50])
            if len(t["log"]) > LOG_KEEP:
                del t["log"][:len(t["log"]) - LOG_KEEP]
            t["lease"] = time.time() + self.lease
            self._touch()
        return {"ok": True}

    def complete(self, nid, payload, ident=None):
        if ident and ident.get("role") == "node" and ident.get("node") and nid != ident["node"]:
            raise ForbiddenError("这个令牌只属于节点 %s" % ident["node"])
        payload = payload or {}
        changes = payload.get("changes")
        if changes is None:
            changes = []
        if not isinstance(changes, list):
            raise SwarmError("changes 必须是数组")
        try:
            attempt = int(payload["attempt"]) if payload.get("attempt") is not None else None
        except (TypeError, ValueError):
            raise SwarmError("attempt 必须是整数")
        verify = payload.get("verify") if isinstance(payload.get("verify"), dict) else None
        failed = False

        # 合并写与状态锁分开：merge_lock -> cond，顺序全局一致
        with self.merge_lock:
            with self.cond:
                pid, p, t = self._locate(payload)
                if t["status"] == "done":
                    return {"ok": True, "already": True, "applied": 0,
                            "conflicts": 0, "task_status": "done"}
                if t.get("node") != nid:
                    raise SwarmError("任务 %s 不归你（状态 %s）" % (t["id"], t["status"]))
                if t["status"] != "running":
                    raise SwarmError("任务 %s 已不在你手上（状态 %s）" % (t["id"], t["status"]))
                if attempt is not None and attempt != int(t.get("attempts") or 0):
                    raise SwarmError("任务 %s 的第 %s 次尝试已过期（当前 %d）" %
                                     (t["id"], attempt, int(t.get("attempts") or 0)))
                t["log"].extend(str(x)[:300] for x in (payload.get("log") or [])[-40:])
                if len(t["log"]) > LOG_KEEP:
                    del t["log"][:len(t["log"]) - LOG_KEEP]

                t["committing"] = True
                t["committing_since"] = time.time()
                t["lease"] = time.time() + max(self.lease, 300)

                err = None
                if verify is not None and verify.get("ok") is False:
                    out = str(verify.get("out") or "")
                    err = "验收命令失败（%s）" % (verify.get("cmd") or "verify")
                    if out:
                        t["log"].append("!! " + out.replace("\n", " ⏎ ")[:600])
                elif not changes and not self.allow_empty:
                    err = "空交付：agent 没有产生任何改动"
                if not err:
                    for ch in changes[:2000]:
                        if not isinstance(ch, dict):
                            err = "changes 里有非对象条目"
                            break
                if err:
                    self._finish_failed_locked(p, t, nid, err, verify)
                    failed = True

            if failed:
                return {"ok": True, "applied": 0, "conflicts": 0,
                        "task_status": t["status"], "failed": True}

            try:
                applied, conflicts = self._apply_changes(p, t, changes)
            except Exception as exc:
                traceback.print_exc()
                with self.cond:
                    try:
                        pid2, p2, t2 = self._locate(payload)
                    except SwarmError:
                        raise
                    t2["committing"] = False
                    t2["status"] = "conflict"
                    t2["conflicts"] = [{"path": "?", "reason": "合并异常：%s" % exc}]
                    t2["ended"] = time.time()
                    t2["lease"] = 0
                    self._bump("conflicts")
                    self._finalize_locked(p2)
                    self._touch()
                    self.cond.notify_all()
                raise SwarmError("合并异常：%s" % exc)

            with self.cond:
                pid2, p2, t2 = self._locate(payload)
                t2["changes"] = changes
                t2["conflicts"] = conflicts
                t2["verify"] = verify
                t2["stats"] = self._diff_stats(p2, t2, applied)
                t2["applied_record"] = applied
                t2["ended"] = time.time()
                t2["lease"] = 0
                t2["committing"] = False
                t2["base"] = {}   # 任务已终态，保留 base 只会让 state 无限增长
                if conflicts:
                    t2["status"] = "conflict"
                    self._bump("conflicts")
                else:
                    t2["status"] = "done"
                    self._bump("done")
                    self._unblock_dependents_locked(p2)
                    node = self.nodes.get(nid)
                    if node:
                        node["done"] = node.get("done", 0) + 1
                node = self.nodes.get(nid)
                if node:
                    node["running"] = [
                        r for r in node.get("running") or []
                        if not (r.get("project") == pid2 and r.get("task") == t2["id"])]
                self.merges.append({"project": pid2, "task": t2["id"],
                                    "time": time.time(), "applied": len(applied)})
                self._finalize_locked(p2)
                self._touch()
                self.cond.notify_all()

            if self.use_git:
                self._git_commit(p["tree"], "合并任务 %s" % t2["id"])

        if applied or conflicts:
            self.log("任务 %s/%s 完成：%d 文件，冲突 %d" %
                     (pid, t2["id"], len(applied), len(conflicts)),
                     "warn" if conflicts else "info")
        return {"ok": True, "applied": len(applied), "conflicts": len(conflicts),
                "task_status": t2["status"]}

    def fail(self, nid, payload, ident=None):
        if ident and ident.get("role") == "node" and ident.get("node") and nid != ident["node"]:
            raise ForbiddenError("这个令牌只属于节点 %s" % ident["node"])
        payload = payload or {}
        err = str(payload.get("error") or "未知错误")[:2000]
        with self.cond:
            pid, p, t = self._locate(payload)
            if t.get("node") != nid:
                if t["status"] in ("done", "failed", "conflict", "blocked", "queued", "cancelled"):
                    return {"ok": True, "status": t["status"], "stale": True}
                raise SwarmError("任务 %s 不归你" % t["id"])
            if t["status"] not in ("preparing", "running"):
                return {"ok": True, "status": t["status"], "stale": True}
            self._finish_failed_locked(p, t, nid, err, None)
            self._touch()
            self.cond.notify_all()
        return {"ok": True, "status": t["status"]}

    def _finish_failed_locked(self, p, t, nid, err, verify):
        t["log"].append("!! " + str(err).replace("\n", " ⏎ ")[:600])
        if len(t["log"]) > LOG_KEEP:
            del t["log"][:len(t["log"]) - LOG_KEEP]
        if verify is not None:
            t["verify"] = verify
        t["committing"] = False
        t["lease"] = 0
        t["ended"] = time.time()
        node = self.nodes.get(nid)
        if node:
            node["failed"] = node.get("failed", 0) + 1
            node["running"] = [
                r for r in node.get("running") or []
                if not (r.get("project") == p["id"] and r.get("task") == t["id"])]
        t["fails"] = int(t.get("fails") or 0) + 1
        if t["fails"] >= self.max_attempts:
            t["status"] = "failed"
            t["base"] = {}
            self._bump("failed")
            self._block_dependents_locked(p, t["id"])
            self.log("任务 %s/%s 失败（第 %d 次失败）：%s" %
                     (p["id"], t["id"], t["fails"], str(err).splitlines()[0][:160]), "error")
        else:
            t["status"] = "queued"
            t["node"] = None
            self.log("任务 %s/%s 失败，重排队（第 %d 次失败，第 %d 次派发）" %
                     (p["id"], t["id"], t["fails"], int(t["attempts"])), "warn")
        self._finalize_locked(p)

    def _block_dependents_locked(self, p, failed_tid):
        queue = [failed_tid]
        while queue:
            dep = queue.pop(0)
            for tid in p["order"]:
                t = p["tasks"][tid]
                if dep in (t.get("deps") or []) and t["status"] in ("queued", "preparing"):
                    t["status"] = "blocked"
                    t["blocked_by"] = dep
                    t["node"] = None
                    t["lease"] = 0
                    t.setdefault("log", []).append("!! 上游任务 %s 失败，本任务阻塞" % dep)
                    queue.append(tid)

    def _unblock_dependents_locked(self, p):
        """上游重新完成后，把因它而 blocked 的下游放回队列。"""
        changed = False
        for tid in p["order"]:
            t = p["tasks"][tid]
            if t["status"] != "blocked":
                continue
            if all(p["tasks"].get(d, {}).get("status") == "done" for d in t["deps"]):
                t["status"] = "queued"
                t["blocked_by"] = None
                t.setdefault("log", []).append("↻ 上游已恢复，任务重新排队")
                changed = True
        return changed

    def _finalize_locked(self, p):
        if p.get("status") in ("paused", "cancelled"):
            return
        tasks = [p["tasks"][tid] for tid in p["order"]]
        if not tasks:
            p["status"] = "done"
            return
        active = [t for t in tasks if t["status"] in ("running", "preparing")]
        queued = [t for t in tasks if t["status"] == "queued"]
        runnable = [t for t in queued
                    if all(p["tasks"].get(d, {}).get("status") == "done" for d in t["deps"])]
        if active or runnable:
            if p.get("status") != "running":
                p["status"] = "running"
            return
        if any(t["status"] == "conflict" for t in tasks):
            new = "review"
        elif any(t["status"] in ("failed", "blocked") for t in tasks):
            new = "failed"
        elif any(t["status"] == "cancelled" for t in tasks):
            new = "cancelled"
        elif queued:
            # 有排队但依赖永远不满足（数据异常）；当失败处理，避免永远 running
            new = "failed"
        else:
            new = "done"
        if p.get("status") != new:
            p["status"] = new
            p["ended"] = time.time()
            if new == "done":
                self.log("项目 %s 全部任务完成 🎉" % p["id"])
            elif new == "failed":
                self.log("项目 %s 有失败/阻塞的任务，打开看板看原因" % p["id"], "error")
            elif new == "review":
                self.log("项目 %s 有冲突待裁决" % p["id"], "warn")

    # ------------------------------------------------------------------
    # 合并
    # ------------------------------------------------------------------
    @staticmethod
    def _tree_sha(tree, path):
        try:
            return workspace.tree_sha(tree, path)
        except OSError:
            return None

    def _apply_changes(self, p, t, changes):
        tree = p["tree"]
        base = t.get("base") or {}
        try:
            ignore = self._ignores_for(p["id"], tree)
        except Exception:
            ignore = None
        applied = []
        conflicts = []
        seen = set()
        for ch in changes:
            if not isinstance(ch, dict):
                conflicts.append({"path": "?", "reason": "改动条目不是对象"})
                continue
            raw = str(ch.get("path") or "").strip()
            path = raw.replace("\\", "/")
            while path.startswith("./"):
                path = path[2:]
            safe = workspace._safe_rel(path)
            if safe is None:
                conflicts.append({"path": raw or "?", "reason": "非法路径"})
                continue
            path = safe
            if path in seen:
                conflicts.append({"path": path, "reason": "同一交付里重复出现"})
                continue
            seen.add(path)
            if ignore is not None and path != ".gitignore" and ignore.ignores(path):
                conflicts.append({"path": path, "reason": "路径被项目 .gitignore 忽略",
                                  "theirs": ch.get("sha"), "base": base.get(path),
                                  "current": self._tree_sha(tree, path)})
                continue
            if not workspace.match_any(path, t["write"]):
                conflicts.append({
                    "path": path,
                    "reason": "越界修改：不在任务范围内 %s" % ", ".join(t["write"]),
                    "theirs": ch.get("sha"), "base": base.get(path),
                    "current": workspace.tree_sha(tree, path), "out_of_scope": True,
                })
                continue
            cur = self._tree_sha(tree, path)
            old = base.get(path)
            if ch.get("deleted") is True:
                if cur is None:
                    continue
                if old is None:
                    conflicts.append({"path": path, "reason": "文件不在任务快照中，拒绝删除",
                                      "theirs": None, "base": None, "current": cur})
                    continue
                if cur != old:
                    conflicts.append({"path": path, "reason": "删除时文件已被别的任务改过",
                                      "theirs": None, "base": old, "current": cur})
                    continue
                err = workspace.apply_change(tree, {"path": path, "deleted": True}, self.blobs)
                if err:
                    conflicts.append({"path": path, "reason": "删除失败：%s" % err})
                else:
                    applied.append({"path": path, "sha": None, "prev": cur, "deleted": True})
                continue
            sha = ch.get("sha")
            if not is_sha(sha) or not self.blobs.has(sha):
                conflicts.append({"path": path, "reason": "blob 缺失，改动作废"})
                continue
            if cur == sha:
                continue
            if old is None:
                if cur is not None:
                    conflicts.append({"path": path, "reason": "文件在任务派发后出现（不在任务快照中）",
                                      "theirs": sha, "base": None, "current": cur})
                    continue
            elif cur != old:
                conflicts.append({"path": path, "reason": "同一文件被并行改动",
                                  "theirs": sha, "base": old, "current": cur})
                continue
            err = workspace.apply_change(tree, {"path": path, "sha": sha, "deleted": False}, self.blobs)
            if err:
                conflicts.append({"path": path, "reason": "写入失败：%s" % err})
                continue
            applied.append({"path": path, "sha": sha, "prev": old, "deleted": False})
        for a in applied:
            t["log"].append(("删除 " if a.get("deleted") else "合并 ") + a["path"])
        return applied, conflicts

    def _diff_stats(self, p, t, applied):
        added = 0
        for a in applied:
            if a.get("deleted"):
                continue
            try:
                new = self.blobs.get(a.get("sha"))
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

    def resolve(self, payload):
        pid = str(payload.get("project") or "").strip()
        tid = str(payload.get("task") or "").strip()
        path = str(payload.get("path") or "").replace("\\", "/")
        choice = payload.get("choice") or "theirs"
        if choice not in ("theirs", "ours"):
            raise SwarmError("choice 只能是 theirs 或 ours")
        safe = workspace._safe_rel(path)
        if safe is None:
            raise SwarmError("非法路径")
        path = safe
        with self.merge_lock:
            with self.cond:
                p = self.projects.get(pid)
                if not p:
                    raise SwarmError("没有这个项目")
                t = p["tasks"].get(tid)
                if not t:
                    raise SwarmError("没有这个任务")
                item = next((c for c in t.get("conflicts") or [] if c.get("path") == path), None)
                if item is None:
                    raise SwarmError("这个路径没有冲突")
                if item.get("out_of_scope") and choice == "theirs":
                    raise SwarmError("越界内容不能写入主工程")
                if choice == "theirs" and not self.blobs.has(item.get("theirs")):
                    raise SwarmError("节点版本已不存在，无法采用")
                prev = workspace.tree_sha(p["tree"], path)

            if choice == "theirs":
                err = workspace.apply_change(
                    p["tree"], {"path": path, "sha": item["theirs"], "deleted": False}, self.blobs)
                if err:
                    raise SwarmError("写入失败：%s" % err)

            with self.cond:
                p2 = self.projects.get(pid)
                t2 = p2["tasks"].get(tid) if p2 else None
                if t2 is None:
                    raise SwarmError("项目/任务不见了")
                t2["conflicts"] = [c for c in t2.get("conflicts") or [] if c.get("path") != path]
                if choice == "theirs":
                    t2["applied_record"].append({"path": path, "sha": item["theirs"],
                                                 "prev": prev, "deleted": False})
                else:
                    t2.setdefault("discarded", []).append({
                        "path": path, "sha": item.get("theirs"), "time": time.time()})
                t2["log"].append("裁决 %s：%s" % (
                    path, "采用节点版本" if choice == "theirs" else "保留主工程版本"))
                if not t2["conflicts"] and t2["status"] == "conflict":
                    t2["status"] = "done"
                    t2["ended"] = time.time()
                    t2["base"] = {}
                    self._bump("done")
                    self._unblock_dependents_locked(p2)
                self._finalize_locked(p2)
                self._touch()
                self.cond.notify_all()
            if self.use_git:
                self._git_commit(p["tree"], "裁决冲突：%s" % path)
        return {"ok": True}

    def diff(self, pid, tid, path):
        safe = workspace._safe_rel(path)
        if safe is None:
            raise SwarmError("非法路径")
        with self.cond:
            p = self.projects.get(pid)
            if not p:
                raise SwarmError("没有这个项目")
            t = p["tasks"].get(tid)
            if not t:
                raise SwarmError("没有这个任务")
            item = next((c for c in t.get("conflicts") or [] if c.get("path") == safe), None)
            current_sha = self._tree_sha(p["tree"], safe)
            if item:
                old_sha = current_sha
                new_sha = item.get("theirs")
            else:
                old_sha = (t.get("base") or {}).get(safe)
                new_sha = current_sha
        old_text = self._blob_text(old_sha)
        new_text = self._blob_text(new_sha)
        if old_text is not None and new_text is not None and \
                len(old_text) + len(new_text) > 1000000:
            return {"path": safe, "binary": False,
                    "diff": "（文件超过 1MB，省略差异；可用 sha 对比）\n主工程 %s\n节点   %s" %
                            (old_sha or "-", new_sha or "-"),
                    "current": current_sha, "theirs": (item or {}).get("theirs")}
        if old_text is None or new_text is None:
            return {"path": safe, "binary": True, "diff": "（二进制文件，无法显示文本差异）",
                    "current": current_sha, "theirs": (item or {}).get("theirs")}
        diff = "".join(difflib.unified_diff(
            old_text.splitlines(True), new_text.splitlines(True),
            fromfile="主工程/" + safe, tofile="节点/" + safe))
        return {"path": safe, "diff": diff[:40000], "binary": False,
                "current": current_sha, "theirs": (item or {}).get("theirs")}

    def _blob_text(self, sha):
        if not sha:
            return ""
        if not self.blobs.has(sha):
            return ""
        try:
            data = self.blobs.get(sha)
        except OSError:
            return ""
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return None

    def rollback(self, payload):
        pid = str(payload.get("project") or "").strip()
        tid = str(payload.get("task") or "").strip() or None
        force = bool(payload.get("force"))
        with self.merge_lock:
            with self.cond:
                p = self.projects.get(pid)
                if not p:
                    raise SwarmError("没有这个项目")
                t = None
                if tid:
                    t = p["tasks"].get(tid)
                    if not t:
                        raise SwarmError("没有这个任务")
                else:
                    rec = next((m for m in reversed(self.merges) if m["project"] == pid), None)
                    if not rec:
                        raise SwarmError("这个项目还没有可回滚的合并")
                    t = p["tasks"].get(rec["task"])
                if not t:
                    raise SwarmError("找不到回滚目标")
                records = list(t.get("applied_record") or [])
                if not records:
                    raise SwarmError("任务 %s 没有可回滚的合并记录" % t["id"])
                blocked = []
                for r in records:
                    cur = self._tree_sha(p["tree"], r["path"])
                    if cur != r.get("sha"):
                        blocked.append(r["path"])
                if blocked and not force:
                    raise SwarmError(
                        "这些文件之后又被合并过，未回滚：%s（确认强制可加 force）" %
                        ", ".join(blocked[:6]))
            restored = []
            for r in reversed(records):
                path = r["path"]
                if r.get("prev"):
                    err = workspace.apply_change(
                        p["tree"], {"path": path, "sha": r["prev"], "deleted": False}, self.blobs)
                    if err:
                        raise SwarmError("恢复 %s 失败：%s" % (path, err))
                else:
                    dest = os.path.join(p["tree"], path)
                    if os.path.isfile(dest):
                        os.unlink(dest)
                        workspace._prune_empty(os.path.dirname(dest), p["tree"])
                restored.append(path)
            with self.cond:
                p2 = self.projects.get(pid)
                t2 = p2["tasks"].get(t["id"]) if p2 else None
                if t2 is None:
                    raise SwarmError("项目/任务不见了")
                t2["applied_record"] = []
                t2["conflicts"] = []
                t2["changes"] = []
                t2["status"] = "queued"
                t2["node"] = None
                t2["lease"] = 0
                t2["attempts"] = 0
                t2["fails"] = 0
                t2["ended"] = 0
                t2["log"].append("↩ 回滚了 %d 个文件，任务重新排队" % len(restored))
                if p2["status"] not in ("paused", "cancelled"):
                    p2["status"] = "running"
                self._unblock_dependents_locked(p2)
                self._finalize_locked(p2)
                self._touch()
                self.cond.notify_all()
            if self.use_git:
                self._git_commit(p["tree"], "回滚任务 %s" % t["id"])
        self.log("回滚 %s/%s：%d 个文件" % (pid, t["id"], len(restored)), "warn")
        return {"ok": True, "restored": restored}

    # ------------------------------------------------------------------
    # 导入已有仓库 / 垃圾回收
    # ------------------------------------------------------------------
    def import_archive(self, title, spec, plan_obj, archive_b64, project_id=None):
        try:
            raw = base64.b64decode(archive_b64 or "", validate=True)
        except Exception:
            raise SwarmError("archive 不是合法 base64")
        if len(raw) > MAX_IMPORT:
            raise SwarmError("导入包太大（上限 %d MB）" % (MAX_IMPORT >> 20))
        tmp = tempfile.mkdtemp(prefix="swarm-import-", dir=self.root)
        try:
            self._extract_tar(raw, tmp)
            return self.create_project(title, spec, plan_obj, project_id=project_id, seed_dir=tmp)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @staticmethod
    def _extract_tar(data, dest):
        try:
            tar = tarfile.open(fileobj=io.BytesIO(data), mode="r:gz")
        except Exception as exc:
            raise SwarmError("导入包不是合法的 tar.gz：%s" % exc)
        total = 0
        count = 0
        try:
            for member in tar:
                count += 1
                if count > 200000:
                    raise SwarmError("导入包文件太多")
                if not (member.isfile() or member.isdir()):
                    continue
                name = member.name.replace("\\", "/")
                while name.startswith("./"):
                    name = name[2:]
                safe = workspace._safe_rel(name)
                if safe is None:
                    continue
                total += max(0, member.size)
                if total > MAX_IMPORT:
                    raise SwarmError("导入包解压后过大")
                target = os.path.join(dest, safe)
                if not os.path.abspath(target).startswith(os.path.abspath(dest) + os.sep):
                    continue
                if member.isdir():
                    os.makedirs(target, exist_ok=True)
                    continue
                src = tar.extractfile(member)
                if src is None:
                    continue
                parent = os.path.dirname(target)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                if os.path.exists(target):
                    continue
                with open(target, "wb") as out:
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        out.write(chunk)
        finally:
            tar.close()

    def _referenced_shas(self):
        refs = set()
        with self.cond:
            projects = list(self.projects.values())
            for p in projects:
                for t in p["tasks"].values():
                    refs.update(v for v in (t.get("base") or {}).values() if v)
                    for ch in t.get("changes") or []:
                        if isinstance(ch, dict) and ch.get("sha"):
                            refs.add(ch["sha"])
                    for c in t.get("conflicts") or []:
                        for key in ("theirs", "base", "current"):
                            if c.get(key):
                                refs.add(c[key])
                    for r in t.get("applied_record") or []:
                        for key in ("sha", "prev"):
                            if r.get(key):
                                refs.add(r[key])
                    for d in t.get("discarded") or []:
                        if d.get("sha"):
                            refs.add(d["sha"])
        for p in projects:
            tree = p["tree"]
            if not os.path.isdir(tree):
                continue
            for _rel, full in workspace.iter_tree(tree):
                try:
                    refs.add(sha256_file(full))
                except OSError:
                    pass
        return refs

    def gc(self, grace=600):
        try:
            grace = max(0, float(grace))
        except (TypeError, ValueError):
            grace = 600
        refs = self._referenced_shas()
        removed, freed = self.blobs.gc(refs, grace=grace)
        self.log("垃圾回收：删除 %d 个 blob，释放 %.1f MB" % (removed, freed / 1048576.0))
        return {"removed": removed, "freed": freed, "referenced": len(refs),
                "remaining": self.blobs.total_bytes()}

    # ------------------------------------------------------------------
    # git 快照（可选，用于额外一层历史）
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
            self.log("git commit 失败：%s" %
                     res.stderr.decode("utf-8", "replace")[:200], "warn")

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
                    node["running"] = []
                    if not node.get("_dead"):
                        node["_dead"] = True
                        self.log("节点失联：%s，回收它的任务" % node["name"], "warn")
                    for p in self.projects.values():
                        for t in p["tasks"].values():
                            if t.get("node") == nid and t["status"] in ("preparing", "running"):
                                if t.get("committing") and now - t.get("committing_since", now) < 600:
                                    continue
                                t["status"] = "queued"
                                t["node"] = None
                                t["lease"] = 0
                                t.setdefault("log", []).append("!! 节点失联，任务回到队列")
                                changed = True
                # 租约过期：节点还活着，但任务丢了（交付被拒、hub 500、客户端 bug）
                for p in self.projects.values():
                    for t in p["tasks"].values():
                        if t["status"] not in ("preparing", "running"):
                            continue
                        if t.get("committing"):
                            if now - t.get("committing_since", now) < 600:
                                continue
                            t["committing"] = False
                        lease = t.get("lease") or 0
                        if lease and now > lease:
                            nid = t.get("node")
                            if nid and nid in self.nodes:
                                self.nodes[nid]["running"] = [
                                    r for r in self.nodes[nid].get("running") or []
                                    if not (r.get("project") == p["id"] and r.get("task") == t["id"])]
                            t["status"] = "queued"
                            t["node"] = None
                            t["lease"] = 0
                            t.setdefault("log", []).append("!! 租约过期，任务回到队列")
                            self.log("任务 %s/%s 租约过期，重新排队" % (p["id"], t["id"]), "warn")
                            changed = True
                if changed:
                    self._touch()
                    self.cond.notify_all()
            if self._dirty:
                try:
                    self._save_state()
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
        self._dirty = True
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
            "  主令牌 %s" % self.token,
            "  节点令牌   ./bin/swarm tokens add <名字> --hub 127.0.0.1:%d" % self.port,
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

    def shutdown(self, save=True):
        if self.stop.is_set():
            return
        self.stop.set()
        with self.cond:
            self.cond.notify_all()
        if save:
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
        with self.cond:
            nodes = []
            for n in self.nodes.values():
                nodes.append({
                    "id": n["id"], "name": n["name"], "addr": n["addr"],
                    "slots": n["slots"], "running": list(n["running"]),
                    "online": (now - n["last_seen"]) <= self.stale_after and not n.get("_dead"),
                    "last_seen": round(now - n["last_seen"], 1),
                    "kinds": list(n["kinds"]), "done": n.get("done", 0),
                    "failed": n.get("failed", 0), "os": n.get("os", ""),
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
                        "node": t.get("node"), "deps": list(t["deps"]), "write": list(t["write"]),
                        "attempts": t["attempts"], "fails": int(t.get("fails") or 0),
                        "conflicts": list(t.get("conflicts") or []),
                        "stats": dict(t.get("stats") or {}), "started": t.get("started"),
                        "ended": t.get("ended"), "log_tail": (t.get("log") or [])[-6:],
                        "verify": t.get("verify"), "blocked_by": t.get("blocked_by"),
                        "applied": len(t.get("applied_record") or []),
                    })
                projects.append({
                    "id": p["id"], "title": p["title"], "status": p["status"],
                    "created": p["created"], "ended": p.get("ended"),
                    "counts": counts, "tasks": tasks,
                    "spec": (p.get("spec") or "")[:1500],
                })
            stats = dict(self.stats)
            events = list(self.events)[-80:]
        return {
            "hub": {"name": self.name, "proto": PROTO, "version": __version__,
                    "uptime": round(now - self.started_at, 1)},
            "nodes": nodes,
            "projects": projects,
            "stats": stats,
            "events": events,
        }


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

class ApiHandler(BaseHTTPRequestHandler):
    hub = None
    protocol_version = "HTTP/1.1"
    server_version = "SwarmHub/" + __version__
    disable_nagle_algorithm = True

    ADMIN_PATHS = {"/api/state", "/api/submit", "/api/control", "/api/resolve",
                   "/api/tasklog", "/api/diff", "/api/rollback", "/api/tokens",
                   "/api/import", "/api/gc"}
    NODE_PATHS = {"/api/heartbeat", "/api/claim", "/api/progress",
                  "/api/complete", "/api/fail"}

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
        self.send_header("X-Content-Type-Options", "nosniff")
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
    def _cookie_value(self):
        cookie = self.headers.get("Cookie") or ""
        for part in cookie.split(";"):
            name, _, val = part.strip().partition("=")
            if name == "swarm_session":
                return val
        return ""

    def _dashboard_ok(self):
        return self.hub.session_valid(self._cookie_value())

    def _session_cookie(self):
        val = self.hub.new_session()
        return ("swarm_session=%s; Path=/; Max-Age=%d; HttpOnly; SameSite=Lax"
                % (val, self.hub.session_ttl))

    def _clear_cookie(self):
        return "swarm_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"

    def _auth(self, body):
        ts = self.headers.get("X-Swarm-Ts") or ""
        sig = self.headers.get("X-Swarm-Sig") or ""
        nonce = self.headers.get("X-Swarm-Nonce") or ""
        if not ts or not sig:
            raise AuthError("缺少签名")
        ident = None
        try:
            proto.verify(self.hub.token, ts, sig, self.command, self.path, body, nonce)
            ident = {"role": "master", "via": "token", "name": "master"}
        except AuthError:
            for entry in self.hub.active_tokens():
                try:
                    proto.verify(entry["token"], ts, sig, self.command, self.path, body, nonce)
                    ident = {"role": "node", "via": "token", "name": entry["name"],
                             "node": entry.get("node"), "entry": entry}
                    break
                except AuthError:
                    continue
        if ident is None:
            raise AuthError("签名不匹配：token 不对，或者不是同一个 hub")
        if self.hub.check_and_remember_sig(sig):
            raise AuthError("重复请求（防重放）")
        self._check_proto()
        return ident

    def _check_proto(self):
        raw = self.headers.get("X-Swarm-Proto") or ""
        if not raw:
            return
        try:
            ver = int(raw)
        except ValueError:
            raise SwarmError("协议版本非法")
        if ver > PROTO:
            raise SwarmError("节点协议 v%d 比 hub v%d 新，请升级 hub" % (ver, PROTO))

    @staticmethod
    def _require_master(ident):
        if not ident or ident.get("role") != "master":
            raise ForbiddenError("这个操作需要 hub 主令牌")

    def _require_node(self, ident, payload):
        nid = str((payload or {}).get("node_id") or "").strip()
        if not nid:
            raise SwarmError("缺少 node_id")
        if ident.get("role") == "node":
            bound = ident.get("node")
            if not bound or bound != nid:
                raise ForbiddenError("这个令牌只属于节点 %s" % (bound or "（未绑定）"))
        header = self.headers.get("X-Swarm-Node")
        if header and header != nid:
            raise ForbiddenError("请求头 node 与载荷不一致")

    def _body(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except (TypeError, ValueError):
            raise SwarmError("Content-Length 非法")
        if length < 0 or length > proto.BODY_LIMIT:
            raise SwarmError("请求体过大")
        return self.rfile.read(length) if length else b""

    # -- 路由 -----------------------------------------------------------
    def do_HEAD(self):
        self._route("HEAD")

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

            if path in ("/", "/index.html"):
                supplied = (query.get("t") or [""])[0]
                if supplied:
                    if not hmac.compare_digest(supplied, self.hub.token):
                        return self._send(401, "令牌不对。回到命令行执行 ./bin/swarm token --root <hub目录>",
                                          "text/plain; charset=utf-8")
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
            if path == "/api/logout":
                self.hub.drop_session(self._cookie_value())
                return self._send(200, json.dumps({"ok": True}),
                                  headers={"Set-Cookie": self._clear_cookie()})
            if method == "HEAD":
                return self._err(405, "不支持 HEAD")

            if path in self.ADMIN_PATHS and self._dashboard_ok():
                ident = {"role": "master", "via": "session", "name": "dashboard"}
            else:
                ident = self._auth(body)

            if path == "/api/has" and method == "POST":
                payload = self._payload(body)
                shas = payload.get("shas") or []
                if not isinstance(shas, list):
                    return self._err(400, "shas 必须是数组")
                out = {}
                for sha in shas[:5000]:
                    if is_sha(sha):
                        out[sha] = self.hub.blobs.has(sha)
                return self._json(200, {"exists": out})
            if path.startswith("/api/has/"):
                return self._has(path)
            if path.startswith("/api/blob/"):
                return self._blob(method, path, body)

            payload = {}
            if body:
                try:
                    payload = json.loads(body.decode("utf-8"))
                except Exception:
                    return self._err(400, "body 不是合法 JSON")
                if not isinstance(payload, dict):
                    return self._err(400, "body 必须是 JSON 对象")

            if path == "/api/join" and method == "POST":
                payload.setdefault("addr", self.client_address[0] + ":" + str(self.client_address[1]))
                return self._json(200, self.hub.join(payload, ident=ident))

            if path in self.NODE_PATHS:
                self._require_node(ident, payload)

            if path == "/api/heartbeat" and method == "POST":
                return self._json(200, self.hub.heartbeat(payload.get("node_id"), payload, ident=ident))
            if path == "/api/claim" and method == "POST":
                try:
                    wait = int((query.get("wait") or ["25"])[0])
                except ValueError:
                    wait = 25
                return self._json(200, self.hub.claim(payload.get("node_id"), wait=wait, ident=ident))
            if path == "/api/progress" and method == "POST":
                return self._json(200, self.hub.progress(payload.get("node_id"), payload, ident=ident))
            if path == "/api/complete" and method == "POST":
                return self._json(200, self.hub.complete(payload.get("node_id"), payload, ident=ident))
            if path == "/api/fail" and method == "POST":
                return self._json(200, self.hub.fail(payload.get("node_id"), payload, ident=ident))

            self._require_master(ident)

            if path == "/api/state" and method == "GET":
                return self._json(200, self.hub.snapshot_state())
            if path == "/api/submit" and method == "POST":
                rid = payload.get("request_id")
                cached = self.hub.request_result(rid)
                if cached is not None:
                    return self._json(200, cached)
                p = self.hub.create_project(payload.get("title") or "",
                                            payload.get("spec") or "", payload.get("plan"))
                res = {"project": p["id"], "title": p["title"], "tasks": len(p["tasks"])}
                self.hub.remember_request(rid, res)
                return self._json(200, res)
            if path == "/api/import" and method == "POST":
                rid = payload.get("request_id")
                cached = self.hub.request_result(rid)
                if cached is not None:
                    return self._json(200, cached)
                p = self.hub.import_archive(payload.get("title") or "",
                                            payload.get("spec") or "",
                                            payload.get("plan"), payload.get("archive"),
                                            project_id=payload.get("project_id"))
                res = {"project": p["id"], "title": p["title"], "tasks": len(p["tasks"])}
                self.hub.remember_request(rid, res)
                return self._json(200, res)
            if path == "/api/control" and method == "POST":
                return self._json(200, self._control(payload))
            if path == "/api/resolve" and method == "POST":
                return self._json(200, self.hub.resolve(payload))
            if path == "/api/diff" and method == "GET":
                pid = (query.get("project") or [""])[0]
                tid = (query.get("task") or [""])[0]
                pth = (query.get("path") or [""])[0]
                return self._json(200, self.hub.diff(pid, tid, pth))
            if path == "/api/rollback" and method == "POST":
                return self._json(200, self.hub.rollback(payload))
            if path == "/api/gc" and method == "POST":
                return self._json(200, self.hub.gc(grace=payload.get("grace", 600)))
            if path == "/api/tokens" and method == "GET":
                return self._json(200, {"tokens": self.hub.token_view()})
            if path == "/api/tokens" and method == "POST":
                action = payload.get("action") or "create"
                if action == "create":
                    entry = self.hub.create_node_token(payload.get("name"), payload.get("node"))
                    return self._json(200, {"ok": True, "name": entry["name"],
                                            "node": entry.get("node"), "token": entry["token"]})
                if action == "revoke":
                    return self._json(200, {"ok": True,
                                            "name": self.hub.revoke_token(payload.get("key") or payload.get("name"))})
                raise SwarmError("未知令牌动作：%s" % action)
            if path == "/api/tasklog" and method == "GET":
                pid = (query.get("project") or [""])[0]
                tid = (query.get("task") or [""])[0]
                with self.hub.cond:
                    p = self.hub.projects.get(pid)
                    if not p or tid not in p["tasks"]:
                        return self._err(404, "没有这个任务")
                    t = p["tasks"][tid]
                    data = {"id": tid, "title": t["title"], "status": t["status"],
                            "changes": list(t.get("changes") or []),
                            "conflicts": list(t.get("conflicts") or []),
                            "applied": list(t.get("applied_record") or []),
                            "verify": t.get("verify"),
                            "log": list(t.get("log") or [])}
                return self._json(200, data)
            return self._err(404, "没有这个接口：%s" % path)
        except AuthError as exc:
            return self._err(401, str(exc))
        except ForbiddenError as exc:
            return self._err(403, str(exc))
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
                hub._finalize_locked(p)
            elif action == "retry":
                tid = payload.get("task")
                t = p["tasks"].get(tid)
                if not t:
                    raise SwarmError("没有这个任务")
                if t["status"] == "blocked" and t.get("blocked_by"):
                    raise SwarmError("任务 %s 被上游 %s 阻塞，请先重试上游任务" %
                                     (tid, t["blocked_by"]))
                t["status"] = "queued"
                t["attempts"] = 0
                t["fails"] = 0
                t["node"] = None
                t["lease"] = 0
                t["ended"] = 0
                t["blocked_by"] = None
                t["log"] = list(t.get("log") or [])[-50:]
                t["log"].append("↻ 人工重试")
                p["status"] = "running"
                hub._finalize_locked(p)
            elif action == "cancel":
                p["status"] = "cancelled"
                for t in p["tasks"].values():
                    if t["status"] in ("queued", "running", "preparing", "blocked"):
                        t["status"] = "cancelled"
                        t["node"] = None
                        t["lease"] = 0
                        t["base"] = {}
            elif action == "finish":
                for t in p["tasks"].values():
                    if t["status"] in ("queued", "running", "preparing", "blocked"):
                        t["status"] = "cancelled"
                        t["node"] = None
                        t["lease"] = 0
                        t["base"] = {}
                p["status"] = "done"
            else:
                raise SwarmError("未知动作：%s" % action)
            hub._touch()
            hub.cond.notify_all()
        return {"ok": True}

    @staticmethod
    def _payload(body):
        if not body:
            return {}
        try:
            obj = json.loads(body.decode("utf-8"))
        except Exception:
            raise SwarmError("body 不是合法 JSON")
        if not isinstance(obj, dict):
            raise SwarmError("body 必须是 JSON 对象")
        return obj

    def _has(self, path):
        sha = path.rsplit("/", 1)[-1]
        if not is_sha(sha):
            return self._err(400, "sha 非法")
        return self._json(200, {"sha": sha, "exists": self.hub.blobs.has(sha)})

    def _blob(self, method, path, body):
        sha = path.rsplit("/", 1)[-1]
        if not is_sha(sha):
            return self._err(400, "sha 非法")
        store = self.hub.blobs
        if method == "GET":
            if not store.has(sha):
                return self._err(404, "没有这个 blob")
            data = store.get(sha)
            headers = {}
            accepted = self.headers.get("X-Swarm-Accept") or ""
            if "gzip" in accepted:
                packed, did = proto.compress(data)
                if did:
                    data = packed
                    headers["X-Swarm-Encoding"] = "gzip"
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            for k, v in headers.items():
                self.send_header(k, v)
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True
            self.hub._add_blob_bytes("blob_out", len(data))
            return None
        if method == "POST":
            if (self.headers.get("X-Swarm-Encoding") or "").lower() == "gzip":
                body = proto.decompress_limited(body)
            if len(body) > 16 << 20:
                return self._err(400, "单个文件超过 16MB 上限")
            if not body or sha256_bytes(body) != sha:
                return self._err(400, "内容与 sha 不符")
            store.put_bytes(body)
            self.hub._add_blob_bytes("blob_in", len(body))
            return self._json(200, {"ok": True})
        return self._err(405, "不支持的方法")

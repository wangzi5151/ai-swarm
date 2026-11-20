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
import socket
import ssl
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
LOG_PERSIST = 120          # state.json 里每个任务最多留这么多行，避免文件无限膨胀
MAX_SPEC = 500 << 10
MAX_IMPORT = 128 << 20
MAX_CHANGES = 5000
MERGE_KEEP = 200


class Hub(object):
    def __init__(self, root, name=None, token=None, port=DEFAULT_PORT, bind="0.0.0.0",
                 discovery_port=DEFAULT_DISCOVERY_PORT, lease=180, stale_after=25,
                 max_attempts=2, use_git=True, verbose=True, allow_empty=False,
                 session_ttl=12 * 3600, auto_merge=False, tls=False,
                 tls_cert=None, tls_key=None):
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
        self.auto_merge = bool(auto_merge)
        self.tls = bool(tls) or bool(tls_cert)
        self.tls_cert = os.path.abspath(tls_cert) if tls_cert else None
        self.tls_key = os.path.abspath(tls_key) if tls_key else None
        self.tls_fingerprint = None

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
        self._snap_saved = {}     # project id -> 上次落盘时间
        self._snap_dirty = set()
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
        self.body_slots = threading.BoundedSemaphore(2)   # 大请求体并发上限，防内存被顶爆
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
            for pid, p in list(raw_projects.items()):
                if not isinstance(p, dict):
                    continue
                if not re.match(r"^[0-9A-Za-z][0-9A-Za-z._-]{0,63}$", str(pid)) \
                        or ".." in str(pid):
                    raw_projects.pop(pid, None)
                    self.log("状态里有非法项目 id，已忽略：%r" % (pid,), "warn")
                    continue
                if p.get("status") == "creating":
                    # 建项目的半路上断电：没有任务，直接丢弃，避免重启后出现空项目
                    raw_projects.pop(pid, None)
                    self.log("丢弃未创建完成的项目 %s" % pid, "warn")
                    continue
                # 树路径跟着数据目录走：备份恢复到新机器/新目录后仍然指得对
                p["tree"] = os.path.join(self.root, "projects", pid, "tree")
                p.setdefault("title", pid)
                p.setdefault("spec", "")
                p.setdefault("created", time.time())
                if not isinstance(p.get("order"), list):
                    p["order"] = []
                if not isinstance(p.get("tasks"), dict):
                    p["tasks"] = {}
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
                    t.setdefault("feedback", None)
                    t.setdefault("discarded", [])
                    t.setdefault("status", "queued")
                    t.setdefault("blocked_by", None)
                    t.setdefault("committing_since", 0)
                    for key in ("log", "changes", "conflicts", "applied_record", "discarded"):
                        if not isinstance(t.get(key), list):
                            t[key] = []
                    if not isinstance(t.get("base"), dict):
                        t["base"] = {}
                    if t.get("verify") is not None and not isinstance(t["verify"], dict):
                        t["verify"] = None
                    if t["status"] in ("running", "preparing"):
                        t["status"] = "queued"
                        t["node"] = None
                        t["lease"] = 0
                        t["committing"] = False
                        t["base"] = {}
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
            raw_merges = data.get("merges") or []
            if isinstance(raw_merges, list):
                for item in raw_merges[-MERGE_KEEP:]:
                    if isinstance(item, dict) and item.get("project") and item.get("task"):
                        self.merges.append(item)
        self.log("恢复 %d 个项目的状态" % len(self.projects))

    def _save_state(self):
        with self.cond:
            projects = {}
            for pid, p in self.projects.items():
                tasks = {}
                for tid, t in p["tasks"].items():
                    nt = dict(t)
                    log = t.get("log") or []
                    if len(log) > LOG_PERSIST:
                        nt["log"] = log[-LOG_PERSIST:]
                    changes = t.get("changes") or []
                    if len(changes) > 200:
                        nt["changes"] = changes[-200:]
                    applied = t.get("applied_record") or []
                    if len(applied) > 2000:
                        nt["applied_record"] = applied[-2000:]
                    discarded = t.get("discarded") or []
                    if len(discarded) > 200:
                        nt["discarded"] = discarded[-200:]
                    verify = t.get("verify")
                    if isinstance(verify, dict) and len(str(verify.get("out") or "")) > 2000:
                        nt["verify"] = {"cmd": verify.get("cmd"), "ok": bool(verify.get("ok")),
                                        "out": str(verify.get("out"))[:2000] + "…"}
                    tasks[tid] = nt
                np = dict(p)
                np["tasks"] = tasks
                np.pop("_order", None)
                projects[pid] = np
            blob = json.dumps({
                "proto": PROTO,
                "hub": self.name,
                "saved": time.time(),
                "projects": projects,
                "merges": list(self.merges)[-MERGE_KEEP:],
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
                    "feedback": None,
                    "discarded": [],
                    "stats": {},
                    "base": {},
                }
        except Exception:
            with self.cond:
                self.projects.pop(pid, None)
                self._touch()
                self.cond.notify_all()
            shutil.rmtree(os.path.join(self.root, "projects", pid), ignore_errors=True)
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
        t["base"] = {}
        t["committing"] = False
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

    # -- 快照哈希索引：重启 hub 也不用把整个仓库重新哈希一遍 ------------
    def _index_path(self, pid):
        return os.path.join(self.root, "projects", pid, "snapshot-index.json")

    def _snap_index_for(self, pid):
        with self._snap_lock:
            index = self._snap_index.get(pid)
            if index is not None:
                return index
            index = {}
            path = self._index_path(pid)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
            except (OSError, ValueError):
                raw = {}
            if isinstance(raw, dict):
                for key, sha in raw.items():
                    if not is_sha(sha):
                        continue
                    parts = str(key).split("\x00")
                    if len(parts) != 3:
                        continue
                    try:
                        index[(parts[0], int(parts[1]), int(parts[2]))] = sha
                    except ValueError:
                        continue
            self._snap_index[pid] = index
            return index

    def _save_snap_index(self, pid):
        with self._snap_lock:
            index = self._snap_index.get(pid)
            if not index:
                return
            raw = {}
            for (rel, size, mtime), sha in index.items():
                raw["%s\x00%d\x00%d" % (rel, size, mtime)] = sha
        path = self._index_path(pid)
        tmp = path + ".tmp"
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(raw, f, ensure_ascii=False)
            os.replace(tmp, path)
            self._snap_saved[pid] = time.time()
            self._snap_dirty.discard(pid)
        except OSError:
            pass

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
            # 用一份索引副本算哈希（不在锁里做 IO），算完再把新条目合并回去，
            # 避免和 janitor 落盘索引并发修改同一个 dict。
            with self._snap_lock:
                index = dict(self._snap_index_for(pid))
            entries = workspace.snapshot(tree, self.blobs, patterns=patterns,
                                         index=index, ignore=ignore)
            with self._snap_lock:
                target = self._snap_index.setdefault(pid, {})
                target.update(index)
                if len(target) > 20000:
                    self._snap_index[pid] = {}
                self._snap_dirty.add(pid)
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
                    "feedback": t.get("feedback"),
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
            t["base"] = {}
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
        if not isinstance(lines, list):
            raise SwarmError("lines 必须是数组")
        try:
            attempt = int(payload["attempt"]) if payload.get("attempt") is not None else None
        except (TypeError, ValueError):
            raise SwarmError("attempt 必须是整数")
        with self.cond:
            pid, p, t = self._locate(payload)
            if t.get("node") != nid or t["status"] not in ("preparing", "running"):
                return {"ok": False, "reason": "任务已不在你手上"}
            if attempt is not None and attempt != int(t.get("attempts") or 0):
                return {"ok": False, "reason": "这次尝试已过期"}
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
        if len(changes) > MAX_CHANGES:
            raise SwarmError("单次交付的文件太多（上限 %d 个）" % MAX_CHANGES)
        try:
            attempt = int(payload["attempt"]) if payload.get("attempt") is not None else None
        except (TypeError, ValueError):
            raise SwarmError("attempt 必须是整数")
        verify = payload.get("verify") if isinstance(payload.get("verify"), dict) else None
        if verify is not None:
            verify = {
                "cmd": str(verify.get("cmd") or "")[:500],
                "ok": bool(verify.get("ok")),
                "out": str(verify.get("out") or "")[:8000],
            }
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
                    for ch in changes:
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
                    records = list(t2.get("applied_record") or [])
                    t2["applied_record"] = []
                    restored = self._restore_records(p2["tree"], records)
                    if restored:
                        t2.setdefault("log", []).append(
                            "↩ 合并异常，已撤销本次已写入的 %d 个文件" % restored)
                    self._finish_failed_locked(p2, t2, nid, "合并异常：%s" % exc, None)
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
                    t2["feedback"] = None
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
                if applied:
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
        try:
            attempt = int(payload["attempt"]) if payload.get("attempt") is not None else None
        except (TypeError, ValueError):
            raise SwarmError("attempt 必须是整数")
        with self.cond:
            pid, p, t = self._locate(payload)
            if attempt is not None and attempt != int(t.get("attempts") or 0):
                # 迟到的失败回报不能把已经重派的新一次尝试打掉
                return {"ok": True, "status": t["status"], "stale": True}
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
        feedback = str(err)
        if isinstance(verify, dict) and verify.get("ok") is False:
            out = str(verify.get("out") or "").strip()
            if out:
                feedback += "\n\n验收命令输出（%s）：\n%s" % (verify.get("cmd") or "verify", out)
        t["feedback"] = feedback[:6000]
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
            t["base"] = {}
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

    def _cancel_tasks_locked(self, p):
        for t in p["tasks"].values():
            if t["status"] not in ("queued", "running", "preparing", "blocked"):
                continue
            nid = t.get("node")
            if nid and nid in self.nodes:
                self.nodes[nid]["running"] = [
                    r for r in self.nodes[nid].get("running") or []
                    if not (r.get("project") == p["id"] and r.get("task") == t["id"])]
            t["status"] = "cancelled"
            t["node"] = None
            t["lease"] = 0
            t["committing"] = False
            t["base"] = {}

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

    def _restore_records(self, tree, records):
        """按 applied_record 反向恢复文件（合并异常/回滚共用）。"""
        restored = 0
        for r in reversed(list(records or [])):
            path = workspace._safe_rel(r.get("path"))
            if not path:
                continue
            err = None
            if r.get("prev"):
                err = workspace.apply_change(
                    tree, {"path": path, "sha": r["prev"], "deleted": False,
                           "mode": r.get("prev_mode")}, self.blobs)
            else:
                dest = os.path.join(tree, path)
                try:
                    if os.path.isfile(dest):
                        os.unlink(dest)
                        workspace._prune_empty(os.path.dirname(dest), tree)
                except OSError as exc:
                    err = str(exc)
            if not err:
                restored += 1
        return restored

    def _blob_bytes(self, sha):
        if not sha or not self.blobs.has(sha):
            return None
        try:
            return self.blobs.get(sha)
        except OSError:
            return None

    @staticmethod
    def _tree_mode(tree, path):
        try:
            return os.stat(os.path.join(tree, path)).st_mode & 0o777
        except OSError:
            return 0o644

    def _file_bytes(self, tree, path):
        full = os.path.join(tree, path)
        try:
            if not os.path.isfile(full):
                return None
            with open(full, "rb") as f:
                return f.read()
        except OSError:
            return None

    def _try_auto_merge(self, tree, path, base_sha, cur_sha, their_sha):
        """两边改了同一文件的不同区域时自动合并；重叠就返回 None 交给人。"""
        limit = 4 << 20
        if self.blobs.size(base_sha) > limit or self.blobs.size(their_sha) > limit:
            return None
        full = os.path.join(tree, path)
        try:
            if os.path.getsize(full) > limit:
                return None
        except OSError:
            return None
        b = self._blob_bytes(base_sha)
        o = self._file_bytes(tree, path)
        t = self._blob_bytes(their_sha)
        if b is None or o is None or t is None:
            return None
        if workspace.is_binary(b) or workspace.is_binary(o) or workspace.is_binary(t):
            return None
        try:
            merged = workspace.merge3(b.decode("utf-8"), o.decode("utf-8"), t.decode("utf-8"))
        except UnicodeDecodeError:
            return None
        if merged is None:
            return None
        new_sha = self.blobs.put_bytes(merged.encode("utf-8"))
        if new_sha == cur_sha:
            return None
        err = workspace.apply_change(tree, {"path": path, "sha": new_sha}, self.blobs)
        if err:
            return None
        return new_sha

    def _apply_changes(self, p, t, changes):
        tree = p["tree"]
        base = dict(t.get("base") or {})
        try:
            ignore = self._ignores_for(p["id"], tree)
        except Exception:
            ignore = None
        applied = []
        conflicts = []
        seen = set()

        def record(item):
            applied.append(item)
            # 边应用边记账：即使后面某条改动抛异常，前面的也能被 rollback 恢复
            t.setdefault("applied_record", []).append(item)

        def conflict(path, reason, theirs=None, old=None, cur=None, **extra):
            item = {"path": path, "reason": reason, "theirs": theirs,
                    "base": old, "current": cur}
            item.update(extra)
            conflicts.append(item)

        for ch in changes:
            try:
                if not isinstance(ch, dict):
                    conflict("?", "改动条目不是对象")
                    continue
                raw = str(ch.get("path") or "").strip()
                path = raw.replace("\\", "/")
                while path.startswith("./"):
                    path = path[2:]
                safe = workspace._safe_rel(path)
                if safe is None:
                    conflict(raw or "?", "非法路径")
                    continue
                path = safe
                if path in seen:
                    conflict(path, "同一交付里重复出现")
                    continue
                seen.add(path)
                deleted = ch.get("deleted") is True
                sha = ch.get("sha")
                mode = ch.get("mode") if isinstance(ch.get("mode"), int) else None
                cur = self._tree_sha(tree, path)
                old = base.get(path)
                prev_mode = self._tree_mode(tree, path)
                if ignore is not None and path != ".gitignore" and ignore.ignores(path):
                    conflict(path, "路径被项目 .gitignore 忽略", theirs=sha, old=old,
                             cur=cur, deleted=deleted, mode=mode)
                    continue
                if not workspace.match_any(path, t["write"]):
                    conflict(path, "越界修改：不在任务范围内 %s" % ", ".join(t["write"]),
                             theirs=sha, old=old, cur=cur, out_of_scope=True,
                             deleted=deleted, mode=mode)
                    continue
                if deleted:
                    if cur is None:
                        continue
                    if old is None:
                        conflict(path, "文件不在任务快照中，拒绝删除",
                                 old=None, cur=cur, deleted=True)
                        continue
                    if cur != old:
                        conflict(path, "删除时文件已被别的任务改过",
                                 old=old, cur=cur, deleted=True)
                        continue
                    err = workspace.apply_change(tree, {"path": path, "deleted": True}, self.blobs)
                    if err:
                        conflict(path, "删除失败：%s" % err)
                    else:
                        record({"path": path, "sha": None, "prev": cur,
                                "deleted": True, "prev_mode": prev_mode})
                    continue
                if not is_sha(sha) or not self.blobs.has(sha):
                    conflict(path, "blob 缺失，改动作废")
                    continue
                if cur == sha:
                    continue
                if old is None:
                    if cur is not None:
                        conflict(path, "文件在任务派发后出现（不在任务快照中）",
                                 theirs=sha, old=None, cur=cur, mode=mode)
                        continue
                elif cur != old:
                    if self.auto_merge:
                        merged_sha = self._try_auto_merge(tree, path, old, cur, sha)
                        if merged_sha:
                            record({"path": path, "sha": merged_sha, "prev": cur,
                                    "deleted": False, "auto_merged": True,
                                    "prev_mode": prev_mode})
                            continue
                    conflict(path, "同一文件被并行改动", theirs=sha, old=old, cur=cur,
                             auto_merge_failed=bool(self.auto_merge), mode=mode)
                    continue
                err = workspace.apply_change(
                    tree, {"path": path, "sha": sha, "deleted": False, "mode": mode},
                    self.blobs)
                if err:
                    conflict(path, "写入失败：%s" % err)
                    continue
                record({"path": path, "sha": sha, "prev": old, "deleted": False,
                        "prev_mode": prev_mode})
            except Exception as exc:
                conflict(str(ch.get("path") or "?") if isinstance(ch, dict) else "?",
                         "处理这条改动时出错：%s" % exc)
        for a in applied:
            t["log"].append(("删除 " if a.get("deleted") else "合并 ") + a["path"])
            if a.get("auto_merged"):
                t["log"].append("⇄ 自动三方合并 " + a["path"])
        return applied, conflicts

    def _diff_stats(self, p, t, applied):
        added = 0
        for a in applied:
            if a.get("deleted"):
                continue
            if self.blobs.size(a.get("sha")) > 300000:
                continue
            try:
                new = self.blobs.get(a.get("sha"))
            except OSError:
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
                deleting = bool(item.get("deleted")) and not item.get("theirs")
                if choice == "theirs" and not deleting and not self.blobs.has(item.get("theirs")):
                    raise SwarmError("节点版本已不存在，无法采用")
                prev = workspace.tree_sha(p["tree"], path)
                prev_mode = self._tree_mode(p["tree"], path)

            if choice == "theirs":
                if deleting:
                    err = workspace.apply_change(
                        p["tree"], {"path": path, "deleted": True}, self.blobs)
                else:
                    err = workspace.apply_change(
                        p["tree"], {"path": path, "sha": item["theirs"], "deleted": False,
                                    "mode": item.get("mode")}, self.blobs)
                if err:
                    raise SwarmError("写入失败：%s" % err)

            with self.cond:
                p2 = self.projects.get(pid)
                t2 = p2["tasks"].get(tid) if p2 else None
                if t2 is None:
                    raise SwarmError("项目/任务不见了")
                t2["conflicts"] = [c for c in t2.get("conflicts") or [] if c.get("path") != path]
                if choice == "theirs":
                    t2["applied_record"].append({
                        "path": path, "sha": None if deleting else item["theirs"],
                        "prev": prev, "deleted": bool(deleting),
                        "prev_mode": prev_mode})
                else:
                    t2.setdefault("discarded", []).append({
                        "path": path, "sha": item.get("theirs"), "time": time.time()})
                if choice == "theirs":
                    verdict = "采用节点版本（删除文件）" if deleting else "采用节点版本"
                else:
                    verdict = "保留主工程版本"
                t2["log"].append("裁决 %s：%s" % (path, verdict))
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
        if item and item.get("deleted") and not item.get("theirs"):
            return {"path": safe, "binary": False,
                    "diff": "节点删除了这个文件。\n主工程内容保留在 %s；"
                            "选择‘用节点的版本’即接受删除，选择‘保留主工程’即忽略删除。"
                            % (current_sha or "-"),
                    "current": current_sha, "theirs": None, "deleted": True}
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
        if self.blobs.size(sha) > (2 << 20):
            return None
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
                    rec = None
                    for m in reversed(self.merges):
                        if m.get("project") != pid:
                            continue
                        cand = p["tasks"].get(m.get("task"))
                        if cand and cand.get("applied_record"):
                            rec = m
                            t = cand
                            break
                    if not rec:
                        raise SwarmError("这个项目还没有可回滚的合并")
                if not t:
                    raise SwarmError("找不到回滚目标")
                if t["status"] in ("running", "preparing") and not force:
                    raise SwarmError(
                        "任务 %s 正在跑，不能回滚；先暂停/取消，或者加 --force" % t["id"])
                if force:
                    nid = t.get("node")
                    if nid and nid in self.nodes:
                        self.nodes[nid]["running"] = [
                            r for r in self.nodes[nid].get("running") or []
                            if not (r.get("project") == pid and r.get("task") == t["id"])]
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
                path = workspace._safe_rel(r.get("path"))
                if path is None:
                    continue
                if r.get("prev"):
                    err = workspace.apply_change(
                        p["tree"], {"path": path, "sha": r["prev"], "deleted": False,
                                    "mode": r.get("prev_mode")}, self.blobs)
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
                t2["committing"] = False
                t2["feedback"] = None
                t2["verify"] = None
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
                try:
                    os.chmod(target, (member.mode or 0o644) & 0o777)
                except OSError:
                    pass
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
    # 删除 / 导出 / 备份
    # ------------------------------------------------------------------
    def delete_project(self, pid):
        with self.merge_lock:
            with self.cond:
                p = self.projects.get(pid)
                if not p:
                    raise SwarmError("没有这个项目：%s" % pid)
                for n in self.nodes.values():
                    n["running"] = [r for r in n.get("running") or []
                                    if r.get("project") != pid]
                for t in p["tasks"].values():
                    t["status"] = "cancelled"
                    t["node"] = None
                    t["lease"] = 0
                    t["committing"] = False
                self.projects.pop(pid, None)
                self._snap_index.pop(pid, None)
                self._ignore_cache.pop(pid, None)
                self.merges = deque(
                    [m for m in self.merges if m.get("project") != pid],
                    maxlen=MERGE_KEEP)
                self._touch()
                self.cond.notify_all()
            shutil.rmtree(os.path.join(self.root, "projects", pid), ignore_errors=True)
        self.log("删除项目 %s" % pid, "warn")
        return {"ok": True, "project": pid}

    def export_archive(self, pid, dest):
        with self.cond:
            p = self.projects.get(pid)
            if not p:
                raise SwarmError("没有这个项目：%s" % pid)
            tree = p["tree"]
        if not os.path.isdir(tree):
            raise SwarmError("项目目录不存在：%s" % tree)
        count = 0
        with tarfile.open(dest, "w:gz") as tar:
            for rel, full in workspace.iter_tree(tree):
                tar.add(full, arcname=rel, recursive=False)
                count += 1
        return {"files": count, "size": os.path.getsize(dest)}

    def backup_archive(self, dest, include_blobs=True):
        """给主备/迁移用：state + 令牌 + 项目树（+ blob）。

        整个备份过程持有 merge_lock：备份期间不会有新合并写进树里，
        恢复出来的 state 和文件内容才是对得上的（代价是备份时合并会短暂排队）。
        """
        with self.merge_lock:
            try:
                self._save_state()
            except Exception:
                pass
            count = 0
            with self.cond:
                pids = list(self.projects)
            with tarfile.open(dest, "w:gz") as tar:
                for name in ("state.json", "state.json.bak", "token", "tokens.json"):
                    full = os.path.join(self.root, name)
                    if os.path.isfile(full):
                        tar.add(full, arcname=name, recursive=False)
                        count += 1
                for name in ("tls", "projects"):
                    full = os.path.join(self.root, name)
                    if os.path.isdir(full):
                        for dirpath, dirnames, filenames in os.walk(full):
                            dirnames[:] = [d for d in dirnames
                                           if d not in ("__pycache__",)
                                           and not d.startswith(".tmp-")]
                            for fn in filenames:
                                if fn.startswith(".tmp-"):
                                    continue
                                path = os.path.join(dirpath, fn)
                                tar.add(path, arcname=os.path.relpath(path, self.root),
                                        recursive=False)
                                count += 1
                if include_blobs and os.path.isdir(self.blobs.root):
                    for dirpath, dirnames, filenames in os.walk(self.blobs.root):
                        dirnames[:] = [d for d in dirnames if not d.startswith(".tmp-")]
                        for fn in filenames:
                            if fn.startswith(".tmp-"):
                                continue
                            path = os.path.join(dirpath, fn)
                            tar.add(path, arcname=os.path.relpath(path, self.root),
                                    recursive=False)
                            count += 1
            try:
                os.chmod(dest, 0o600)     # 备份里有令牌
            except OSError:
                pass
        self.log("备份完成：%d 个文件，%.1f MB" % (count, os.path.getsize(dest) / 1048576.0))
        return {"files": count, "size": os.path.getsize(dest), "projects": len(pids)}

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
                                if t.get("committing") and \
                                        now - t.get("committing_since", now) < max(600, self.lease * 4):
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
                            if now - t.get("committing_since", now) < max(600, self.lease * 4):
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
            # 快照索引：最多每 30 秒落盘一次，重启后不用重新哈希大仓库
            if self._snap_dirty:
                now = time.time()
                for pid in list(self._snap_dirty):
                    if now - self._snap_saved.get(pid, 0) >= 30:
                        try:
                            self._save_snap_index(pid)
                        except Exception:
                            self._snap_dirty.discard(pid)

    # ------------------------------------------------------------------
    # TLS（可选）
    # ------------------------------------------------------------------
    def _tls_paths(self):
        base = os.path.join(self.root, "tls")
        return (self.tls_cert or os.path.join(base, "cert.pem"),
                self.tls_key or os.path.join(base, "key.pem"))

    @staticmethod
    def cert_fingerprint(cert_path):
        with open(cert_path, "r", encoding="utf-8") as f:
            pem = f.read()
        try:
            der = ssl.PEM_cert_to_DER_cert(pem)
        except ValueError:
            raise SwarmError("证书不是 PEM 格式：%s" % cert_path)
        return sha256_bytes(der)

    def ensure_tls(self):
        """准备证书。没给证书就用 openssl 在 ~/.swarm/tls 生成一张自签的。"""
        if not self.tls:
            return None
        cert, key = self._tls_paths()
        if not (os.path.exists(cert) and os.path.exists(key)):
            if self.tls_cert or self.tls_key:
                raise SwarmError("指定的证书/私钥不存在：%s / %s" % (cert, key))
            self._generate_cert(cert, key)
        self.tls_cert, self.tls_key = cert, key
        self.tls_fingerprint = self.cert_fingerprint(cert)
        try:
            os.chmod(key, 0o600)
        except OSError:
            pass
        return cert, key, self.tls_fingerprint

    def _generate_cert(self, cert, key):
        openssl = shutil.which("openssl")
        if not openssl:
            raise SwarmError(
                "没有 openssl，无法自动生成自签证书。两种办法：\n"
                "  1) 安装 openssl（Termux: pkg install openssl）后重试 --tls；\n"
                "  2) 自己生成证书，然后用 --tls --tls-cert cert.pem --tls-key key.pem")
        base_dir = os.path.dirname(cert) or "."
        os.makedirs(base_dir, exist_ok=True)
        try:
            os.chmod(base_dir, 0o700)
        except OSError:
            pass
        names = ["IP:127.0.0.1", "DNS:localhost"]
        lan = proto.lan_ip()
        if lan and lan != "127.0.0.1":
            names.append("IP:" + lan)
        try:
            host = socket.gethostname()
            if host and host not in ("localhost",):
                names.append("DNS:" + host)
        except Exception:
            pass
        extra = os.environ.get("SWARM_TLS_SAN") or ""
        names.extend(n.strip() for n in extra.split(",") if n.strip())
        san = ",".join(dict.fromkeys(names))
        base = [openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", key, "-out", cert, "-days", "3650",
                "-subj", "/CN=swarm-hub"]
        attempts = [base + ["-addext", "subjectAltName=" + san], base]
        last = None
        for args in attempts:
            try:
                res = subprocess.run(args, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, timeout=60)
            except Exception as exc:
                last = exc
                continue
            if res.returncode == 0 and os.path.exists(cert) and os.path.exists(key):
                self.log("已生成自签 TLS 证书：%s（指纹 %s…）" %
                         (cert, self.cert_fingerprint(cert)[:16]))
                return
            last = res.stderr.decode("utf-8", "replace")[:300]
        raise SwarmError("生成证书失败：%s" % last)

    def start_background(self):
        self.stop.clear()
        if self.tls:
            self.ensure_tls()
        t1 = threading.Thread(target=self._janitor, name="swarm-janitor", daemon=True)
        t1.start()
        info = {"name": self.name, "port": self.port, "addr": proto.lan_ip()}
        if self.tls and self.tls_fingerprint:
            info["tls"] = self.tls_fingerprint
        t2 = threading.Thread(
            target=discover.serve_responder,
            args=(self.discovery_port, info, self.stop, self.log),
            name="swarm-discover", daemon=True)
        t2.start()
        self._threads = [t1, t2]
        self._dirty = True
        self._save_state()
        return self

    def make_server(self):
        hub = self
        if self.tls:
            self.ensure_tls()

        class Handler(ApiHandler):
            pass

        class Server(ThreadingHTTPServer):
            allow_reuse_address = True
            daemon_threads = True
            request_queue_size = 64
            max_connections = 64

            def __init__(self, *args, **kwargs):
                ThreadingHTTPServer.__init__(self, *args, **kwargs)
                self._conn_slots = threading.BoundedSemaphore(self.max_connections)

            def process_request(self, request, client_address):
                if not self._conn_slots.acquire(blocking=False):
                    # 连接数打满：直接关掉，防止无上限建线程
                    try:
                        request.close()
                    except OSError:
                        pass
                    return
                try:
                    ThreadingHTTPServer.process_request(self, request, client_address)
                except Exception:
                    self._conn_slots.release()
                    raise

            def process_request_thread(self, request, client_address):
                try:
                    ThreadingHTTPServer.process_request_thread(self, request, client_address)
                finally:
                    try:
                        self._conn_slots.release()
                    except ValueError:
                        pass

            def handle_error(self, request, client_address):
                exc = sys.exc_info()[1]
                if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
                    return
                ThreadingHTTPServer.handle_error(self, request, client_address)

        Handler.hub = hub
        srv = Server((self.bind, self.port), Handler)
        if self.tls:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            try:
                ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            except (AttributeError, ValueError):
                pass
            ctx.load_cert_chain(self.tls_cert, self.tls_key)
            srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
            # 握手放到工作线程里做，慢客户端不会卡住 accept 循环
            srv.socket.do_handshake_on_connect = False
        self._server = srv
        return srv

    def serve_forever(self):
        srv = self.make_server()
        self.start_background()
        scheme = "https" if self.tls else "http"
        prints = [
            "",
            "  ╭──────────────────────────────────────────────╮",
            "  │  Swarm 调度中心已启动                        │",
            "  ╰──────────────────────────────────────────────╯",
            "  看板   %s://%s:%d/   （手机浏览器直接开）" % (scheme, proto.lan_ip(), self.port),
            "  地址   %s:%d" % (proto.lan_ip(), self.port),
            "  主令牌 %s" % self.token,
            "  节点令牌   ./bin/swarm tokens add <名字> --hub %s://127.0.0.1:%d"
            % (scheme, self.port),
            "  自动发现 UDP %d（节点用 --discover 即可，不用填 IP）" % self.discovery_port,
        ]
        if self.tls:
            prints.append("  TLS    已启用，证书指纹 %s" % self.tls_fingerprint)
            prints.append("         节点用 --discover 会自动带上指纹；手动连接加 --tls 并核对指纹")
        prints.append("")
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
            for pid in list(self._snap_dirty):
                try:
                    self._save_snap_index(pid)
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
                    verify = t.get("verify")
                    if isinstance(verify, dict):
                        # 状态轮询不传大段验收输出（完整内容走 /api/tasklog）
                        verify = {"ok": bool(verify.get("ok")),
                                  "cmd": str(verify.get("cmd") or "")[:300]}
                    tasks.append({
                        "id": t["id"], "title": t["title"], "status": t["status"],
                        "node": t.get("node"), "deps": list(t["deps"]), "write": list(t["write"]),
                        "attempts": t["attempts"], "fails": int(t.get("fails") or 0),
                        "conflicts": list(t.get("conflicts") or []),
                        "stats": dict(t.get("stats") or {}), "started": t.get("started"),
                        "ended": t.get("ended"), "log_tail": (t.get("log") or [])[-4:],
                        "verify": verify, "blocked_by": t.get("blocked_by"),
                        "applied": len(t.get("applied_record") or []),
                        "feedback": (t.get("feedback") or "")[:300],
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
                    "uptime": round(now - self.started_at, 1),
                    "tls": bool(self.tls),
                    "tls_fingerprint": self.tls_fingerprint,
                    "auto_merge": bool(self.auto_merge)},
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
    timeout = 60          # 慢客户端 / 半开连接不会永久占线程

    ADMIN_PATHS = {"/api/state", "/api/submit", "/api/control", "/api/resolve",
                   "/api/tasklog", "/api/diff", "/api/rollback", "/api/tokens",
                   "/api/import", "/api/gc", "/api/delete", "/api/export",
                   "/api/backup"}
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
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _send_file(self, path, filename, ctype="application/gzip"):
        """把临时文件分块发出去，避免把大项目整个读进内存。"""
        size = os.path.getsize(path)
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Disposition",
                         'attachment; filename="%s"' % filename.replace('"', ""))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        try:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(1 << 20)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
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
        secure = "; Secure" if self.hub.tls else ""
        return ("swarm_session=%s; Path=/; Max-Age=%d; HttpOnly; SameSite=Lax%s"
                % (val, self.hub.session_ttl, secure))

    def _clear_cookie(self):
        secure = "; Secure" if self.hub.tls else ""
        return "swarm_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax" + secure

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

    def _body(self, limit=None):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except (TypeError, ValueError):
            raise SwarmError("Content-Length 非法")
        cap = proto.BODY_LIMIT if limit is None else min(int(limit), proto.BODY_LIMIT)
        if length < 0 or length > cap:
            raise SwarmError("请求体过大")
        if not length:
            return b""
        big = length > (8 << 20)
        if big and not self.hub.body_slots.acquire(timeout=30):
            raise SwarmError("服务器正忙（同时上传的大请求太多），稍后重试")
        try:
            return self.rfile.read(length)
        finally:
            if big:
                try:
                    self.hub.body_slots.release()
                except ValueError:
                    pass

    def _preauth_ok(self, path):
        """读 body 之前先做零成本的检查：要么有看板会话，要么带看起来合理的签名头。

        真正的签名校验仍要等 body 读完；这里只防止完全匿名的大 body 打进来。
        """
        if (path in self.ADMIN_PATHS or path == "/api/logout") and self._dashboard_ok():
            return True
        ts = self.headers.get("X-Swarm-Ts") or ""
        sig = self.headers.get("X-Swarm-Sig") or ""
        if not ts or not sig:
            return False
        try:
            ts_i = int(ts)
        except (TypeError, ValueError):
            return False
        return abs(time.time() - ts_i) <= (proto.MAX_SKEW + 5)

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
                if not self._preauth_ok(path):
                    return self._err(401, "缺少签名，或时间戳偏差过大（检查两台机器的时钟）")
                # 按接口收紧请求体上限：普通 API 8MB，blob 单文件上限，import 才放到 64MB
                if path == "/api/import":
                    limit = proto.BODY_LIMIT
                elif path.startswith("/api/blob/"):
                    limit = workspace.MAX_FILE + (1 << 20)
                else:
                    limit = 8 << 20
                body = self._body(limit)

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
                return self._send(200, DASHBOARD_HTML, "text/html; charset=utf-8",
                                  headers={"Content-Security-Policy":
                                           "default-src 'self'; script-src 'self' 'unsafe-inline'; "
                                           "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
                                           "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"})
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
            if path == "/api/delete" and method == "POST":
                return self._json(200, self.hub.delete_project(str(payload.get("project") or "")))
            if path == "/api/export" and method == "GET":
                pid = (query.get("project") or [""])[0]
                tmpdir = tempfile.mkdtemp(prefix="swarm-export-", dir=self.hub.root)
                dest = os.path.join(tmpdir, "export.tar.gz")
                try:
                    self.hub.export_archive(pid, dest)
                    return self._send_file(dest, "%s.tar.gz" % pid)
                finally:
                    shutil.rmtree(tmpdir, ignore_errors=True)
            if path == "/api/backup" and method == "GET":
                include = (query.get("blobs") or ["1"])[0] not in ("0", "false", "no")
                tmpdir = tempfile.mkdtemp(prefix="swarm-backup-", dir=self.hub.root)
                dest = os.path.join(tmpdir, "backup.tar.gz")
                try:
                    self.hub.backup_archive(dest, include_blobs=include)
                    return self._send_file(dest, "swarm-backup.tar.gz")
                finally:
                    shutil.rmtree(tmpdir, ignore_errors=True)
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
                            "feedback": t.get("feedback"),
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
                if t["status"] in ("running", "preparing"):
                    raise SwarmError("任务 %s 正在某个节点上跑，先暂停/取消再重试" % tid)
                if t["status"] == "done":
                    raise SwarmError("任务 %s 已完成；要重做请先 rollback 再重试" % tid)
                nid = t.get("node")
                if nid and nid in hub.nodes:
                    hub.nodes[nid]["running"] = [
                        r for r in hub.nodes[nid].get("running") or []
                        if not (r.get("project") == pid and r.get("task") == tid)]
                t["status"] = "queued"
                t["attempts"] = 0
                t["fails"] = 0
                t["node"] = None
                t["lease"] = 0
                t["ended"] = 0
                t["blocked_by"] = None
                t["committing"] = False
                t["feedback"] = None
                t["conflicts"] = []
                t["base"] = {}
                t["log"] = list(t.get("log") or [])[-50:]
                t["log"].append("↻ 人工重试")
                p["status"] = "running"
                hub._finalize_locked(p)
            elif action == "cancel":
                p["status"] = "cancelled"
                hub._cancel_tasks_locked(p)
            elif action == "finish":
                hub._cancel_tasks_locked(p)
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
            size = store.size(sha)
            headers = {}
            data = None
            accepted = self.headers.get("X-Swarm-Accept") or ""
            # 小文件才在内存里压；大文件直接流式发，避免 hub 吃掉整块内存
            if "gzip" in accepted and 0 <= size <= (2 << 20):
                packed, did = proto.compress(store.get(sha))
                if did:
                    data = packed
                    headers["X-Swarm-Encoding"] = "gzip"
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data) if data is not None else size))
            for k, v in headers.items():
                self.send_header(k, v)
            self.end_headers()
            sent = 0
            try:
                if data is not None:
                    self.wfile.write(data)
                    sent = len(data)
                else:
                    with store.open(sha) as f:
                        while True:
                            chunk = f.read(1 << 20)
                            if not chunk:
                                break
                            self.wfile.write(chunk)
                            sent += len(chunk)
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True
            self.hub._add_blob_bytes("blob_out", sent)
            return None
        if method == "POST":
            if (self.headers.get("X-Swarm-Encoding") or "").lower() == "gzip":
                body = proto.decompress_limited(body)
            if len(body) > workspace.MAX_FILE:
                return self._err(400, "单个文件超过 %dMB 上限" % (workspace.MAX_FILE >> 20))
            if not body or sha256_bytes(body) != sha:
                return self._err(400, "内容与 sha 不符")
            store.put_bytes(body)
            self.hub._add_blob_bytes("blob_in", len(body))
            return self._json(200, {"ok": True})
        return self._err(405, "不支持的方法")

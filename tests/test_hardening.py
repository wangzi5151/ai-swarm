"""针对已修复问题的回归测试：多项目、租约、失败传播、空交付、验收、
冲突回滚、diff、令牌、防重放、路径安全、导入、GC、并发落盘等。

    python3 -m unittest tests.test_hardening -v
"""

from __future__ import annotations

import base64
import gzip
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_swarm import HubFixture, free_port, wait_for          # noqa: E402
from swarm import agents, planner, proto, workspace             # noqa: E402
from swarm.hub import Hub                                       # noqa: E402
from swarm.node import Node                                     # noqa: E402
from swarm.proto import AuthError, ForbiddenError, HubClient, SwarmError  # noqa: E402
from swarm.store import BlobStore, sha256_bytes                 # noqa: E402


def one_task_plan(tid="t1", write=None, deps=None, verify=None, read=None):
    task = {"id": tid, "title": tid, "prompt": "finish " + tid,
            "read": read or ["**"], "write": write or ["src/%s/**" % tid],
            "deps": deps or []}
    if verify:
        task["verify"] = verify
    return {"title": "plan-" + tid, "tasks": [task]}


def tree_files(root):
    out = []
    for dirpath, _dirs, names in os.walk(root):
        if ".git" in dirpath.split(os.sep):
            continue
        for n in names:
            out.append(os.path.relpath(os.path.join(dirpath, n), root).replace(os.sep, "/"))
    return sorted(out)


class TestTaskKeyIsolation(unittest.TestCase):
    """不同项目里同名任务不能串（旧版 hub 用 t["id"] 全局查找）。"""

    def setUp(self):
        self.fx = HubFixture(use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_same_task_id_in_two_projects(self):
        p1 = self.fx.hub.create_project("项目一", "", one_task_plan("t-core"))
        p2 = self.fx.hub.create_project("项目二", "", one_task_plan("t-core"))
        a = self.fx.join_fake("A")
        b = self.fx.join_fake("B")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p1["id"], "t-core", a.node_id)
            self.fx.hub._dispatch_locked(p2["id"], "t-core", b.node_id)
        sha1 = self.fx.hub.blobs.put_bytes(b"one\n")
        sha2 = self.fx.hub.blobs.put_bytes(b"two\n")
        path = "src/t-core/out.txt"
        r1 = self.fx.hub.complete(a.node_id, {
            "project": p1["id"], "task_id": "t-core", "attempt": 1,
            "changes": [{"path": path, "sha": sha1}]})
        r2 = self.fx.hub.complete(b.node_id, {
            "project": p2["id"], "task_id": "t-core", "attempt": 1,
            "changes": [{"path": path, "sha": sha2}]})
        self.assertEqual(r1["applied"], 1)
        self.assertEqual(r2["applied"], 1)
        with open(os.path.join(p1["tree"], path)) as f:
            self.assertEqual(f.read(), "one\n")
        with open(os.path.join(p2["tree"], path)) as f:
            self.assertEqual(f.read(), "two\n")

    def test_ambiguous_task_id_requires_project(self):
        p1 = self.fx.hub.create_project("项目一", "", one_task_plan("dup"))
        self.fx.hub.create_project("项目二", "", one_task_plan("dup"))
        a = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p1["id"], "dup", a.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"x\n")
        with self.assertRaises(SwarmError) as ctx:
            self.fx.hub.complete(a.node_id, {
                "task_id": "dup", "attempt": 1,
                "changes": [{"path": "src/dup/x.txt", "sha": sha}]})
        self.assertIn("project", str(ctx.exception))


class TestLeaseAndIdempotency(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(lease=5, stale_after=60, use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_lease_expiry_requeues_even_if_node_alive(self):
        p = self.fx.hub.create_project("租约", "", one_task_plan("t1"))
        fake = self.fx.join_fake("alive-but-lost")
        got = fake.claim({"node_id": fake.node_id, "slots": 1}, wait=1)
        self.assertEqual(got["task"]["id"], "t1")
        wait_for(lambda: p["tasks"]["t1"]["status"] == "queued", timeout=20,
                 message="租约过期后任务没有回队列")
        other = self.fx.join_fake("other")
        got2 = other.claim({"node_id": other.node_id, "slots": 1}, wait=1)
        self.assertEqual(got2["task"]["id"], "t1")
        self.assertEqual(got2["task"]["attempt"], 2)

    def test_heartbeat_refreshes_lease(self):
        p = self.fx.hub.create_project("心跳", "", one_task_plan("t1"))
        fake = self.fx.join_fake("hb")
        fake.claim({"node_id": fake.node_id, "slots": 1}, wait=1)
        end = time.time() + 7
        while time.time() < end:
            fake.heartbeat({"node_id": fake.node_id, "running": [
                {"project": p["id"], "task": "t1"}], "slots": 1})
            time.sleep(1.5)
        self.assertEqual(p["tasks"]["t1"]["status"], "running",
                         "持续心跳时任务不应被回收")

    def test_complete_is_idempotent(self):
        p = self.fx.hub.create_project("幂等", "", one_task_plan("t1"))
        fake = self.fx.join_fake("A")
        fake.claim({"node_id": fake.node_id, "slots": 1}, wait=1)
        sha = self.fx.hub.blobs.put_bytes(b"v1\n")
        payload = {"project": p["id"], "task_id": "t1", "attempt": 1,
                   "changes": [{"path": "src/t1/x.txt", "sha": sha}]}
        r1 = self.fx.hub.complete(fake.node_id, payload)
        self.assertEqual(r1["applied"], 1)
        r2 = self.fx.hub.complete(fake.node_id, payload)
        self.assertTrue(r2.get("already"))
        self.assertEqual(self.fx.hub.projects[p["id"]]["tasks"]["t1"]["status"], "done")
        self.assertEqual(self.fx.hub.stats["done"], 1)

    def test_stale_attempt_rejected(self):
        p = self.fx.hub.create_project("过期", "", one_task_plan("t1"))
        fake = self.fx.join_fake("A")
        fake.claim({"node_id": fake.node_id, "slots": 1}, wait=1)
        sha = self.fx.hub.blobs.put_bytes(b"v1\n")
        with self.assertRaises(SwarmError):
            self.fx.hub.complete(fake.node_id, {
                "project": p["id"], "task_id": "t1", "attempt": 99,
                "changes": [{"path": "src/t1/x.txt", "sha": sha}]})
        self.assertEqual(p["tasks"]["t1"]["status"], "running")


class TestFailurePropagation(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(max_attempts=2, use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_empty_delivery_is_failure_not_done(self):
        plan = {"tasks": [
            {"id": "a", "title": "a", "write": ["src/a/**"], "deps": []},
            {"id": "b", "title": "b", "write": ["src/b/**"], "deps": ["a"]},
        ]}
        p = self.fx.hub.create_project("空交付", "", plan)
        fake = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "a", fake.node_id)
        res = self.fx.hub.complete(fake.node_id, {
            "project": p["id"], "task_id": "a", "attempt": 1, "changes": []})
        self.assertTrue(res.get("failed"))
        self.assertNotEqual(p["tasks"]["a"]["status"], "done")
        # max_attempts=2：第一次失败回队列，第二次仍空交付才终态
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "a", fake.node_id)
        res = self.fx.hub.complete(fake.node_id, {
            "project": p["id"], "task_id": "a", "attempt": 2, "changes": []})
        self.assertTrue(res.get("failed"))
        self.assertEqual(p["tasks"]["a"]["status"], "failed")
        self.assertEqual(p["tasks"]["b"]["status"], "blocked")
        self.assertEqual(p["status"], "failed")

    def test_verify_failure_blocks_merge(self):
        plan = one_task_plan("t1", verify="exit 1")
        p = self.fx.hub.create_project("验收失败", "", plan)
        fake = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "t1", fake.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"should-not-land\n")
        res = self.fx.hub.complete(fake.node_id, {
            "project": p["id"], "task_id": "t1", "attempt": 1,
            "changes": [{"path": "src/t1/x.txt", "sha": sha}],
            "verify": {"cmd": "exit 1", "ok": False, "out": "boom"}})
        self.assertTrue(res.get("failed"))
        self.assertFalse(os.path.exists(os.path.join(p["tree"], "src/t1/x.txt")))
        self.assertIn("验收", "\n".join(p["tasks"]["t1"]["log"]))

    def test_failure_retries_then_fails(self):
        plan = one_task_plan("t1")
        p = self.fx.hub.create_project("重试", "", plan)
        fake = self.fx.join_fake("A")
        for attempt in (1, 2):
            with self.fx.hub.cond:
                self.fx.hub._dispatch_locked(p["id"], "t1", fake.node_id)
            self.fx.hub.fail(fake.node_id, {"project": p["id"], "task_id": "t1",
                                            "attempt": attempt, "error": "boom"})
        self.assertEqual(p["tasks"]["t1"]["status"], "failed")
        self.assertEqual(p["tasks"]["t1"]["fails"], 2)

    def test_retry_upstream_unblocks_downstream(self):
        plan = {"tasks": [
            {"id": "a", "title": "a", "write": ["src/a/**"], "deps": []},
            {"id": "b", "title": "b", "write": ["src/b/**"], "deps": ["a"]},
        ]}
        p = self.fx.hub.create_project("解阻塞", "", plan)
        fake = self.fx.join_fake("A")
        for _ in range(2):
            with self.fx.hub.cond:
                self.fx.hub._dispatch_locked(p["id"], "a", fake.node_id)
            self.fx.hub.fail(fake.node_id, {"project": p["id"], "task_id": "a",
                                            "attempt": p["tasks"]["a"]["attempts"],
                                            "error": "boom"})
        self.assertEqual(p["tasks"]["a"]["status"], "failed")
        self.assertEqual(p["tasks"]["b"]["status"], "blocked")
        with self.assertRaises(SwarmError):
            self.fx.client.call("POST", "/api/control",
                                {"project": p["id"], "action": "retry", "task": "b"})
        self.fx.client.call("POST", "/api/control",
                            {"project": p["id"], "action": "retry", "task": "a"})
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "a", fake.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"ok\n")
        self.fx.hub.complete(fake.node_id, {
            "project": p["id"], "task_id": "a", "attempt": p["tasks"]["a"]["attempts"],
            "changes": [{"path": "src/a/x.txt", "sha": sha}]})
        self.assertEqual(p["tasks"]["b"]["status"], "queued")
        self.assertEqual(p["status"], "running")

    def test_conflict_then_resolve_resumes_project(self):
        plan = {"tasks": [
            {"id": "a", "title": "a", "write": ["src/shared/**"], "deps": []},
            {"id": "b", "title": "b", "write": ["src/shared/**"], "deps": []},
            {"id": "c", "title": "c", "write": ["src/c/**"], "deps": ["b"]},
        ]}
        p = self.fx.hub.create_project("冲突恢复", "", plan)
        a = self.fx.join_fake("A")
        b = self.fx.join_fake("B")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "a", a.node_id)
            self.fx.hub._dispatch_locked(p["id"], "b", b.node_id)
        path = "src/shared/x.txt"
        sha_a = self.fx.hub.blobs.put_bytes(b"a\n")
        sha_b = self.fx.hub.blobs.put_bytes(b"b\n")
        self.fx.hub.complete(a.node_id, {"project": p["id"], "task_id": "a",
                                         "attempt": 1,
                                         "changes": [{"path": path, "sha": sha_a}]})
        self.fx.hub.complete(b.node_id, {"project": p["id"], "task_id": "b",
                                         "attempt": 1,
                                         "changes": [{"path": path, "sha": sha_b}]})
        self.assertEqual(p["status"], "review")
        self.assertEqual(p["tasks"]["c"]["status"], "queued")
        self.fx.hub.resolve({"project": p["id"], "task": "b", "path": path,
                             "choice": "theirs"})
        self.assertEqual(p["tasks"]["b"]["status"], "done")
        self.assertEqual(p["status"], "running", "下游还有任务时应回到 running")


class TestScopesAndPaths(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_existing_file_outside_read_scope_not_false_conflict(self):
        p = self.fx.hub.create_project("范围", "", {
            "tasks": [{"id": "t1", "title": "t1", "deps": [],
                       "read": ["src/core/**"], "write": ["src/api/**"]}]})
        os.makedirs(os.path.join(p["tree"], "src", "api"), exist_ok=True)
        with open(os.path.join(p["tree"], "src", "api", "a.py"), "w") as f:
            f.write("v0\n")
        fake = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "t1", fake.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"v1\n")
        res = self.fx.hub.complete(fake.node_id, {
            "project": p["id"], "task_id": "t1", "attempt": 1,
            "changes": [{"path": "src/api/a.py", "sha": sha}]})
        self.assertEqual(res["applied"], 1, p["tasks"]["t1"]["conflicts"])
        self.assertEqual(res["conflicts"], 0)
        with open(os.path.join(p["tree"], "src/api/a.py")) as f:
            self.assertEqual(f.read(), "v1\n")

    def test_planner_expands_read_with_write(self):
        plan = planner.validate({"tasks": [
            {"id": "x", "read": ["src/core/**"], "write": ["src/api/**"]}]})
        self.assertIn("src/api/**", plan["tasks"][0]["read"])

    def test_traversal_change_rejected(self):
        p = self.fx.hub.create_project("越界路径", "", one_task_plan("t1"))
        fake = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "t1", fake.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"evil\n")
        res = self.fx.hub.complete(fake.node_id, {
            "project": p["id"], "task_id": "t1", "attempt": 1,
            "changes": [{"path": "../../etc/passwd", "sha": sha}]})
        self.assertEqual(res["applied"], 0)
        self.assertIn("非法路径", p["tasks"]["t1"]["conflicts"][0]["reason"])

    def test_node_work_dirs_do_not_collide_after_sanitizing(self):
        a = Node._work_path("/tmp/w", "p1", "a:b", 1)
        b = Node._work_path("/tmp/w", "p1", "a_b", 1)
        self.assertNotEqual(a, b)
        self.assertTrue(a.startswith("/tmp/w/p1/"))
        self.assertNotIn(":", os.path.basename(a))

    def test_materialize_rejects_traversal(self):
        d = tempfile.mkdtemp()
        try:
            store = BlobStore(os.path.join(d, "blobs"))
            sha = store.put_bytes(b"x")
            with self.assertRaises(IOError):
                workspace.materialize(os.path.join(d, "w"), store,
                                      [{"path": "../escape", "sha": sha}])
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_diff_work_honours_gitignore(self):
        d = tempfile.mkdtemp()
        try:
            with open(os.path.join(d, ".gitignore"), "w") as f:
                f.write("build/\n*.log\n")
            os.makedirs(os.path.join(d, "src"))
            os.makedirs(os.path.join(d, "build"))
            with open(os.path.join(d, "src", "a.py"), "w") as f:
                f.write("ok")
            with open(os.path.join(d, "build", "x.bin"), "w") as f:
                f.write("noise")
            with open(os.path.join(d, "debug.log"), "w") as f:
                f.write("noise")
            rules = workspace.load_ignores(d)
            changes = workspace.diff_work(d, [], ignore=rules)
            paths = [c["path"] for c in changes]
            self.assertIn("src/a.py", paths)
            self.assertNotIn("build/x.bin", paths)
            self.assertNotIn("debug.log", paths)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_hub_rejects_gitignored_path(self):
        p = self.fx.hub.create_project("忽略路径", "", one_task_plan("t1", write=["**"]))
        with open(os.path.join(p["tree"], ".gitignore"), "w") as f:
            f.write("build/\n")
        fake = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "t1", fake.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"artifact\n")
        res = self.fx.hub.complete(fake.node_id, {
            "project": p["id"], "task_id": "t1", "attempt": 1,
            "changes": [{"path": "build/out.bin", "sha": sha}]})
        self.assertEqual(res["applied"], 0)
        self.assertIn(".gitignore", p["tasks"]["t1"]["conflicts"][0]["reason"])
        self.assertFalse(os.path.exists(os.path.join(p["tree"], "build/out.bin")))

    def test_gitignore_and_symlinks_skipped(self):
        d = tempfile.mkdtemp()
        try:
            store = BlobStore(os.path.join(d, "blobs"))
            os.makedirs(os.path.join(d, "build"))
            os.makedirs(os.path.join(d, "src"))
            with open(os.path.join(d, ".gitignore"), "w") as f:
                f.write("build/\n*.log\n")
            with open(os.path.join(d, "build", "x.bin"), "w") as f:
                f.write("noise")
            with open(os.path.join(d, "debug.log"), "w") as f:
                f.write("noise")
            with open(os.path.join(d, "src", "a.py"), "w") as f:
                f.write("ok")
            try:
                os.symlink("/etc/passwd", os.path.join(d, "link"))
            except OSError:
                pass
            rules = workspace.load_ignores(d)
            snap = workspace.snapshot(d, store, ignore=rules)
            paths = [e["path"] for e in snap]
            self.assertIn("src/a.py", paths)
            self.assertNotIn("build/x.bin", paths)
            self.assertNotIn("debug.log", paths)
            self.assertNotIn("link", paths)
        finally:
            shutil.rmtree(d, ignore_errors=True)


class TestCliHardTimeout(unittest.TestCase):
    def _ctx(self, d, cmd, timeout):
        return {"workdir": d, "prompt": "x", "files": [], "file_list": [],
                "task_id": "t", "title": "t", "node": "n",
                "log": lambda *_a: None, "cmd": cmd, "timeout": timeout}

    def test_timeout_with_no_output(self):
        d = tempfile.mkdtemp()
        try:
            t0 = time.time()
            with self.assertRaises(agents.AgentError) as ctx:
                agents.run_cli(self._ctx(d, "sleep 30", 2))
            elapsed = time.time() - t0
            self.assertIn("超时", str(ctx.exception))
            self.assertLess(elapsed, 8, "硬超时没有生效（旧实现会永远阻塞）")
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_timeout_with_partial_output(self):
        d = tempfile.mkdtemp()
        try:
            t0 = time.time()
            with self.assertRaises(agents.AgentError):
                agents.run_cli(self._ctx(d, "echo hello; sleep 30", 2))
            self.assertLess(time.time() - t0, 8)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_run_shell_verify(self):
        d = tempfile.mkdtemp()
        try:
            self.assertTrue(agents.run_shell("echo ok", d, timeout=5)[0])
            self.assertFalse(agents.run_shell("exit 1", d, timeout=5)[0])
        finally:
            shutil.rmtree(d, ignore_errors=True)


class TestHttpAgentSafety(unittest.TestCase):
    def test_http_agent_rejects_traversal(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        body = json.dumps({"files": {"../../evil.txt": "x"}}).encode()

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                payload = json.dumps({"choices": [{"message": {"content": body.decode()}}]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_a):
                pass

        srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        th = threading.Thread(target=srv.serve_forever, daemon=True)
        th.start()
        d = tempfile.mkdtemp()
        try:
            with self.assertRaises(agents.AgentError):
                agents.run_http({"workdir": d, "prompt": "x", "files": ["**"],
                                 "file_list": [], "task_id": "t", "title": "t",
                                 "log": lambda *_a: None,
                                 "base_url": "http://127.0.0.1:%d/v1" % srv.server_address[1],
                                 "model": "m", "timeout": 10})
        finally:
            srv.shutdown()
            srv.server_close()
            shutil.rmtree(d, ignore_errors=True)


class TestAuthAndTokens(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False)

    def tearDown(self):
        self.fx.close()

    def _raw_get(self, path, headers):
        import http.client as hc
        conn = hc.HTTPConnection("127.0.0.1", self.fx.port, timeout=10)
        conn.request("GET", path, headers=headers)
        r = conn.getresponse()
        data = r.read()
        status = r.status
        conn.close()
        return status, data

    def test_replayed_signature_rejected(self):
        ts = str(int(time.time()))
        nonce = "deadbeefdeadbeef"
        sig = proto.sign(self.fx.hub.token, ts, "GET", "/api/state", b"", nonce)
        headers = {"X-Swarm-Ts": ts, "X-Swarm-Nonce": nonce,
                   "X-Swarm-Sig": sig, "X-Swarm-Proto": "2"}
        s1, _ = self._raw_get("/api/state", headers)
        s2, body = self._raw_get("/api/state", headers)
        self.assertEqual(s1, 200)
        self.assertEqual(s2, 401)
        self.assertIn("重放", body.decode("utf-8"))

    def test_node_token_scoped_and_revocable(self):
        res = self.fx.client.call("POST", "/api/tokens",
                                  {"action": "create", "name": "phone-b"})
        token = res["token"]
        node = HubClient("127.0.0.1", self.fx.port, token, timeout=10)
        joined = node.join({"name": "phone-b", "kinds": ["mock"], "slots": 1})
        node.node_id = joined["node_id"]
        self.assertIn(joined["node_id"], [r["node"] for r in
                                          self.fx.client.get_json("/api/tokens")["tokens"]
                                          if r["name"] == "phone-b"])
        with self.assertRaises(ForbiddenError):
            node.get_json("/api/state")
        node.heartbeat({"node_id": node.node_id, "slots": 1, "running": []})
        self.fx.client.call("POST", "/api/tokens",
                            {"action": "revoke", "key": "phone-b"})
        with self.assertRaises(AuthError):
            node.heartbeat({"node_id": node.node_id, "slots": 1, "running": []})
        node.close_all()

    def test_node_token_binds_on_first_join(self):
        res = self.fx.client.call("POST", "/api/tokens",
                                  {"action": "create", "name": "loose"})
        c1 = HubClient("127.0.0.1", self.fx.port, res["token"], timeout=10)
        joined = c1.join({"node_id": "n-fixed", "name": "x"})
        self.assertEqual(joined["node_id"], "n-fixed")
        c2 = HubClient("127.0.0.1", self.fx.port, res["token"], timeout=10)
        with self.assertRaises(ForbiddenError):
            c2.join({"node_id": "n-other", "name": "y"})
        c1.close_all()
        c2.close_all()

    def test_dashboard_session_random_and_revocable(self):
        import http.client as hc
        cookies = []
        for _ in range(2):
            conn = hc.HTTPConnection("127.0.0.1", self.fx.port, timeout=10)
            conn.request("GET", "/?t=" + self.fx.hub.token)
            r = conn.getresponse()
            r.read()
            cookie = r.getheader("Set-Cookie") or ""
            self.assertIn("HttpOnly", cookie)
            cookies.append(cookie.split(";")[0])
            conn.close()
        self.assertNotEqual(cookies[0], cookies[1])
        conn = hc.HTTPConnection("127.0.0.1", self.fx.port, timeout=10)
        conn.request("GET", "/api/state", headers={"Cookie": cookies[0]})
        r = conn.getresponse()
        r.read()
        self.assertEqual(r.status, 200)
        conn.close()
        conn = hc.HTTPConnection("127.0.0.1", self.fx.port, timeout=10)
        conn.request("POST", "/api/logout", headers={"Cookie": cookies[0],
                                                     "Content-Length": "0"})
        r = conn.getresponse()
        r.read()
        self.assertEqual(r.status, 200)
        self.assertIn("Max-Age=0", r.getheader("Set-Cookie") or "")
        conn.close()
        conn = hc.HTTPConnection("127.0.0.1", self.fx.port, timeout=10)
        conn.request("GET", "/api/state", headers={"Cookie": cookies[0]})
        r = conn.getresponse()
        r.read()
        self.assertEqual(r.status, 401)
        conn.close()


class TestBlobTransport(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_gzip_push_fetch_and_has(self):
        data = (b"hello swarm\n" * 2000)
        sha = sha256_bytes(data)
        self.assertFalse(self.fx.client.has_blob(sha))
        self.fx.client.push_blob(sha, data)
        self.assertTrue(self.fx.client.has_blob(sha))
        self.assertEqual(self.fx.client.fetch_blob(sha), data)
        status, _ = self.fx.client.raw("POST", "/api/blob/" + sha, b"wrong")
        self.assertEqual(status, 400)

    def test_node_skips_already_uploaded_blob(self):
        data = b"dedup-me\n"
        srv = self.fx.start_node("n1", slots=1)
        sha = sha256_bytes(data)
        self.fx.client.push_blob(sha, data)
        self.assertTrue(srv._hub_has(self.fx.client, sha))
        self.fx.hub.stats["blob_in"] = 0
        srv._push_blobs(self.fx.client, [{"path": "a", "sha": sha, "deleted": False}])
        self.assertEqual(self.fx.hub.stats["blob_in"], 0, "hub 已有 blob 时不应重复上传")


class TestRollbackAndDiff(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_rollback_restores_previous_content(self):
        p = self.fx.hub.create_project("回滚", "", one_task_plan("t1"))
        path = "src/t1/x.txt"
        os.makedirs(os.path.dirname(os.path.join(p["tree"], path)), exist_ok=True)
        with open(os.path.join(p["tree"], path), "w") as f:
            f.write("v0\n")
        fake = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "t1", fake.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"v1\n")
        self.fx.hub.complete(fake.node_id, {"project": p["id"], "task_id": "t1",
                                            "attempt": 1,
                                            "changes": [{"path": path, "sha": sha}]})
        with open(os.path.join(p["tree"], path)) as f:
            self.assertEqual(f.read(), "v1\n")
        self.fx.client.call("POST", "/api/rollback", {"project": p["id"], "task": "t1"})
        with open(os.path.join(p["tree"], path)) as f:
            self.assertEqual(f.read(), "v0\n")
        self.assertEqual(p["tasks"]["t1"]["status"], "queued")

    def test_diff_shows_both_sides(self):
        p = self.fx.hub.create_project("差异", "", {
            "tasks": [
                {"id": "a", "title": "a", "write": ["src/shared/**"], "deps": []},
                {"id": "b", "title": "b", "write": ["src/shared/**"], "deps": []},
            ]})
        path = "src/shared/x.txt"
        os.makedirs(os.path.join(p["tree"], "src", "shared"), exist_ok=True)
        with open(os.path.join(p["tree"], path), "w") as f:
            f.write("v0\n")
        a = self.fx.join_fake("A")
        b = self.fx.join_fake("B")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "a", a.node_id)
            self.fx.hub._dispatch_locked(p["id"], "b", b.node_id)
        sa = self.fx.hub.blobs.put_bytes(b"v-a\n")
        sb = self.fx.hub.blobs.put_bytes(b"v-b\n")
        self.fx.hub.complete(a.node_id, {"project": p["id"], "task_id": "a",
                                         "attempt": 1,
                                         "changes": [{"path": path, "sha": sa}]})
        self.fx.hub.complete(b.node_id, {"project": p["id"], "task_id": "b",
                                         "attempt": 1,
                                         "changes": [{"path": path, "sha": sb}]})
        res = self.fx.hub.diff(p["id"], "b", path)
        self.assertFalse(res["binary"])
        self.assertIn("-v-a", res["diff"])
        self.assertIn("+v-b", res["diff"])


class TestSchedulingLimits(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_node_slots_are_enforced(self):
        plan = {"tasks": [
            {"id": "a", "title": "a", "write": ["src/a/**"], "deps": []},
            {"id": "b", "title": "b", "write": ["src/b/**"], "deps": []},
        ]}
        self.fx.hub.create_project("槽位", "", plan)
        fake = self.fx.join_fake("one-slot")
        got = fake.claim({"node_id": fake.node_id, "slots": 1}, wait=1)
        self.assertIsNotNone(got.get("task"))
        again = fake.claim({"node_id": fake.node_id, "slots": 1}, wait=0)
        self.assertIsNone(again.get("task"))

    def test_concurrent_state_save_and_create(self):
        hub = self.fx.hub
        errors = []

        def writer(i):
            try:
                for n in range(4):
                    hub.create_project("并发%d-%d" % (i, n), "",
                                       one_task_plan("t%d" % n))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def saver():
            try:
                end = time.time() + 2.0
                while time.time() < end:
                    hub.snapshot_state()
                    hub._save_state()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        threads.append(threading.Thread(target=saver))
        for t in threads:
            t.start()
        for t in threads:
            t.join(20)
        self.assertEqual(errors, [], "并发落盘/建项目不应抛异常")


class TestImportAndGc(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="swarm-import-test-")
        self.hub = Hub(os.path.join(self.dir, "hub"), use_git=False, verbose=False)

    def tearDown(self):
        self.hub.stop.set()
        shutil.rmtree(self.dir, ignore_errors=True)

    def _archive(self, files):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for name, data in files.items():
                raw = data.encode()
                info = tarfile.TarInfo(name)
                info.size = len(raw)
                tar.addfile(info, io.BytesIO(raw))
        return base64.b64encode(buf.getvalue()).decode()

    def test_import_respects_gitignore(self):
        archive = self._archive({
            ".gitignore": "build/\n",
            "build/x.bin": "noise",
            "src/a.py": "print(1)\n",
        })
        p = self.hub.import_archive("导入", "", None, archive)
        files = tree_files(p["tree"])
        self.assertIn("src/a.py", files)
        self.assertNotIn("build/x.bin", files)

    def test_import_skips_traversal_members(self):
        archive = self._archive({"../evil.txt": "x", "/abs.txt": "y", "ok.txt": "z"})
        p = self.hub.import_archive("恶意包", "", None, archive)
        self.assertIn("ok.txt", tree_files(p["tree"]))
        self.assertFalse(os.path.exists(os.path.join(self.dir, "evil.txt")))
        self.assertFalse(os.path.exists("/abs.txt"))

    def test_gc_removes_only_unreferenced(self):
        orphan = self.hub.blobs.put_bytes(b"orphan")
        p = self.hub.create_project("gc", "", one_task_plan("t1"))
        keep = self.hub.blobs.put_bytes(b"keep\n")
        with self.hub.cond:
            self.hub._dispatch_locked(p["id"], "t1", "n-gc")
        self.hub.complete("n-gc", {"project": p["id"], "task_id": "t1",
                                   "attempt": 1,
                                   "changes": [{"path": "src/t1/x.txt", "sha": keep}]})
        res = self.hub.gc(grace=0)
        self.assertFalse(self.hub.blobs.has(orphan))
        self.assertTrue(self.hub.blobs.has(keep))
        self.assertGreaterEqual(res["removed"], 1)

    def test_bad_plan_returns_clear_error(self):
        with self.assertRaises(SwarmError):
            self.hub.create_project("坏计划", "", {"tasks": [
                {"id": "x"}, {"id": "x"}]})

    def test_bad_priority_returns_clear_error(self):
        with self.assertRaises(SwarmError):
            self.hub.create_project("坏优先级", "", {"tasks": [
                {"id": "x", "priority": "high"}]})


class TestEndToEndHardening(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fx = HubFixture(max_attempts=1, use_git=False)
        cls.fx.start_node("n1", slots=2)
        cls.fx.start_node("n2", slots=2)
        wait_for(lambda: len(cls.fx.hub.nodes) >= 2, timeout=15, message="节点没上线")

    @classmethod
    def tearDownClass(cls):
        cls.fx.close()

    def _submit(self, title, plan):
        res = self.fx.client.call("POST", "/api/submit", {"title": title, "spec": "", "plan": plan})
        pid = res["project"]

        def finished():
            st = self.fx.state()
            proj = next((x for x in st["projects"] if x["id"] == pid), None)
            if proj and proj["status"] in ("done", "failed", "review", "cancelled"):
                return proj
            return None

        return pid, wait_for(finished, timeout=90, message="项目没跑完")

    def test_two_projects_same_task_ids_in_parallel(self):
        plan_a = one_task_plan("t-core", write=["src/a/**"])
        plan_b = one_task_plan("t-core", write=["src/b/**"])
        pa, proja = self._submit("并行A", plan_a)
        pb, projb = self._submit("并行B", plan_b)
        self.assertEqual(proja["status"], "done", proja)
        self.assertEqual(projb["status"], "done", projb)
        ta = self.fx.hub.projects[pa]["tree"]
        tb = self.fx.hub.projects[pb]["tree"]
        self.assertTrue(any(f.startswith("src/a/") for f in tree_files(ta)))
        self.assertTrue(any(f.startswith("src/b/") for f in tree_files(tb)))

    def test_verify_success_is_recorded(self):
        plan = one_task_plan("t1", verify="python3 -c \"import sys; sys.exit(0)\"")
        pid, proj = self._submit("验收通过", plan)
        self.assertEqual(proj["status"], "done", proj)
        task = proj["tasks"][0]
        self.assertIsNotNone(task.get("verify"))
        self.assertTrue(task["verify"]["ok"])

    def test_verify_failure_never_lands(self):
        plan = one_task_plan("t1", verify="exit 1")
        pid, proj = self._submit("验收失败", plan)
        self.assertEqual(proj["status"], "failed", proj)
        tree = self.fx.hub.projects[pid]["tree"]
        self.assertFalse(any(f.startswith("src/t1/") for f in tree_files(tree)))


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestMiscHardening(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_project_id_traversal_rejected(self):
        with self.assertRaises(SwarmError):
            self.fx.hub.create_project("x", "", one_task_plan("t1"),
                                       project_id="../../escape")
        with self.assertRaises(SwarmError):
            self.fx.hub.create_project("x", "", one_task_plan("t1"), project_id="..")

    def test_join_bad_slots_is_client_error(self):
        bad = HubClient("127.0.0.1", self.fx.port, self.fx.hub.token, timeout=10)
        with self.assertRaises(SwarmError):
            bad.join({"name": "bad", "slots": "many"})
        bad.close_all()

    def test_planner_dedupes_core_section(self):
        spec = "## Core (src/core2/**)\n实现核心\n"
        plan = planner.critical_path_order(planner.validate(planner.plan(spec)))
        ids = [t["id"] for t in plan["tasks"]]
        self.assertEqual(len(ids), len(set(ids)), ids)
        self.assertIn("t-core", ids)
        self.assertGreaterEqual(len(ids), 4)

    def test_string_deleted_flag_is_not_a_delete(self):
        p = self.fx.hub.create_project("删除标志", "", one_task_plan("t1"))
        path = "src/t1/x.txt"
        os.makedirs(os.path.dirname(os.path.join(p["tree"], path)), exist_ok=True)
        with open(os.path.join(p["tree"], path), "w") as f:
            f.write("v0\n")
        fake = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "t1", fake.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"v1\n")
        res = self.fx.hub.complete(fake.node_id, {
            "project": p["id"], "task_id": "t1", "attempt": 1,
            "changes": [{"path": path, "deleted": "yes", "sha": sha}]})
        self.assertEqual(res["applied"], 1)
        with open(os.path.join(p["tree"], path)) as f:
            self.assertEqual(f.read(), "v1\n")

    def test_submit_request_id_is_idempotent(self):
        payload = {"title": "幂等项目", "spec": "## 模块 (src/m/**)\n做点事\n",
                   "request_id": "fixed-request-1"}
        r1 = self.fx.client.call("POST", "/api/submit", payload)
        r2 = self.fx.client.call("POST", "/api/submit", payload)
        self.assertEqual(r1["project"], r2["project"])
        st = self.fx.state()
        self.assertEqual(len([p for p in st["projects"]
                              if p["id"] == r1["project"]]), 1)

    def test_submit_uses_spec_title_when_empty(self):
        res = self.fx.client.call("POST", "/api/submit", {
            "title": "", "spec": "# 我的项目\n\n## 模块 (src/m/**)\n做点事\n"})
        self.assertEqual(res["title"], "我的项目")

    def test_finish_cancels_pending_work(self):
        p = self.fx.hub.create_project("强制结束", "", one_task_plan("t1"))
        fake = self.fx.join_fake("A")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "t1", fake.node_id)
        self.fx.client.call("POST", "/api/control",
                            {"project": p["id"], "action": "finish"})
        self.assertEqual(p["status"], "done")
        self.assertEqual(p["tasks"]["t1"]["status"], "cancelled")
        sha = self.fx.hub.blobs.put_bytes(b"late\n")
        with self.assertRaises(SwarmError):
            self.fx.hub.complete(fake.node_id, {
                "project": p["id"], "task_id": "t1", "attempt": 1,
                "changes": [{"path": "src/t1/x.txt", "sha": sha}]})


class TestStateRecovery(unittest.TestCase):
    def test_corrupt_state_falls_back_to_backup(self):
        root = tempfile.mkdtemp(prefix="swarm-state-")
        try:
            h1 = Hub(os.path.join(root, "hub"), use_git=False, verbose=False)
            h1.create_project("状态", "", one_task_plan("t1"))
            h1.shutdown()
            path = os.path.join(root, "hub", "state.json")
            self.assertTrue(os.path.exists(path))
            shutil.copyfile(path, path + ".bak")
            with open(path, "w") as f:
                f.write("{not json")
            h2 = Hub(os.path.join(root, "hub"), use_git=False, verbose=False)
            self.assertEqual(len(h2.projects), 1)
            h2.shutdown(save=False)
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestHttpDeleteShape(unittest.TestCase):
    def test_delete_must_be_list(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        payload = json.dumps({"delete": "important.txt"}).encode()

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                out = json.dumps({"choices": [{"message": {"content": payload.decode()}}]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

            def log_message(self, *_a):
                pass

        srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        d = tempfile.mkdtemp()
        try:
            with open(os.path.join(d, "important.txt"), "w") as f:
                f.write("keep")
            with self.assertRaises(agents.AgentError):
                agents.run_http({"workdir": d, "prompt": "x", "files": ["**"],
                                 "file_list": [], "task_id": "t", "title": "t",
                                 "log": lambda *_a: None,
                                 "base_url": "http://127.0.0.1:%d/v1" % srv.server_address[1],
                                 "model": "m", "timeout": 10})
            self.assertTrue(os.path.exists(os.path.join(d, "important.txt")))
        finally:
            srv.shutdown()
            srv.server_close()
            shutil.rmtree(d, ignore_errors=True)

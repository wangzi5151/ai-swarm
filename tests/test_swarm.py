"""端到端与单元测试（只用标准库 unittest）。

    python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm import planner, proto, workspace                      # noqa: E402
from swarm.hub import Hub                                        # noqa: E402
from swarm.node import Node                                      # noqa: E402
from swarm.proto import AuthError, HubClient                     # noqa: E402
from swarm.store import BlobStore                                # noqa: E402


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def wait_for(fn, timeout=60, interval=0.1, message="条件未满足"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = fn()
        if val:
            return val
        time.sleep(interval)
    raise AssertionError(message)


class HubFixture(object):
    def __init__(self, **kw):
        self.dir = tempfile.mkdtemp(prefix="swarm-test-")
        self.port = free_port()
        self.dport = free_port()
        self.hub = Hub(os.path.join(self.dir, "hub"), port=self.port,
                       discovery_port=self.dport, verbose=False, **kw)
        self.srv = self.hub.make_server()
        self.hub.start_background()
        self.thread = threading.Thread(target=self.srv.serve_forever,
                                       kwargs={"poll_interval": 0.2}, daemon=True)
        self.thread.start()
        self.client = HubClient("127.0.0.1", self.port, self.hub.token, timeout=30)
        self.nodes = []
        self.fakes = []

    def join_fake(self, name="fake"):
        c = HubClient("127.0.0.1", self.port, self.hub.token, timeout=30)
        res = c.join({"name": name, "kinds": ["mock"], "slots": 1})
        c.node_id = res["node_id"]
        self.fakes.append(c)
        return c

    def start_node(self, name, slots=1, **kw):
        n = Node(root=os.path.join(self.dir, name), hub="127.0.0.1:%d" % self.port,
                 token=self.hub.token, name=name, slots=slots, verbose=False,
                 mock_delay=0.05, **kw)
        t = threading.Thread(target=n.run, daemon=True)
        t.start()
        self.nodes.append(n)
        return n

    def state(self):
        return self.client.get_json("/api/state")

    def close(self):
        for n in self.nodes:
            try:
                n.shutdown()
            except Exception:
                pass
        self.hub.shutdown()
        try:
            self.srv.shutdown()
        except Exception:
            pass
        for c in self.fakes:
            try:
                c.close_all()
            except Exception:
                pass
        self.client.close_all()
        shutil.rmtree(self.dir, ignore_errors=True)


# --------------------------------------------------------------------------
class TestWorkspace(unittest.TestCase):
    def test_globs(self):
        self.assertTrue(workspace.match_any("src/a/b.py", ["src/**"]))
        self.assertFalse(workspace.match_any("docs/a.md", ["src/**"]))
        self.assertTrue(workspace.match_any("a.py", ["**/*.py"]))
        self.assertTrue(workspace.match_any("src/x/y.py", ["src/**/*.py"]))
        self.assertTrue(workspace.match_any("README.md", ["*.md"]))
        self.assertFalse(workspace.match_any("src/x/y.py", ["*.md"]))

    def test_scope_overlap(self):
        self.assertTrue(workspace.scopes_overlap(["src/a/**"], ["src/a/x.py"]))
        self.assertFalse(workspace.scopes_overlap(["src/a/**"], ["src/b/**"]))
        self.assertTrue(workspace.scopes_overlap(["**"], ["src/b/**"]))

    def test_diff_and_materialize(self):
        d = tempfile.mkdtemp()
        try:
            store = BlobStore(os.path.join(d, "blobs"))
            tree = os.path.join(d, "tree")
            os.makedirs(os.path.join(tree, "src"))
            with open(os.path.join(tree, "src", "a.py"), "w") as f:
                f.write("x = 1\n")
            snap = workspace.snapshot(tree, store)
            self.assertEqual([e["path"] for e in snap], ["src/a.py"])
            work = os.path.join(d, "work")
            workspace.materialize(work, store, snap)
            with open(os.path.join(work, "src", "a.py"), "a") as f:
                f.write("y = 2\n")
            with open(os.path.join(work, "src", "b.py"), "w") as f:
                f.write("z = 3\n")
            changes = workspace.diff_work(work, snap)
            paths = sorted(c["path"] for c in changes)
            self.assertEqual(paths, ["src/a.py", "src/b.py"])
            self.assertEqual(workspace.out_of_scope(changes, ["src/a.py"]), ["src/b.py"])
        finally:
            shutil.rmtree(d, ignore_errors=True)


class TestPlanner(unittest.TestCase):
    def test_plan_and_validate(self):
        spec = "## 数据层 (src/data/**)\nSQLite\n\n## 界面 (src/ui/**)\n列表\n"
        p = planner.validate(planner.plan(spec))
        ids = [t["id"] for t in p["tasks"]]
        self.assertIn("t-core", ids)
        self.assertEqual(len(set(ids)), len(ids))
        for t in p["tasks"]:
            for d in t["deps"]:
                self.assertIn(d, ids)
        self.assertTrue(all(t["priority"] >= 0 for t in p["tasks"]))

    def test_cycle_detected(self):
        with self.assertRaises(ValueError):
            planner.validate({"tasks": [
                {"id": "a", "deps": ["b"]},
                {"id": "b", "deps": ["a"]},
            ]})

    def test_explicit_plan(self):
        spec = json.dumps({"title": "x", "tasks": [{"id": "only", "title": "一", "deps": []}]})
        p = planner.plan(spec)
        self.assertEqual([t["id"] for t in p["tasks"]], ["only"])


class TestCliAgent(unittest.TestCase):
    def test_external_command_writes_files(self):
        from swarm import agents
        d = tempfile.mkdtemp()
        try:
            script = os.path.join(d, "fake_agent.sh")
            with open(script, "w") as f:
                f.write("#!/bin/sh\nmkdir -p src/demo\nprintf 'from:%s\\n' \"$1\" > src/demo/out.py\n")
            logs = []
            agents.run_cli({
                "workdir": d,
                "prompt": "写个演示",
                "files": ["src/demo/**"],
                "file_list": [],
                "task_id": "t1",
                "title": "演示",
                "node": "test",
                "log": logs.append,
                "cmd": "sh " + script + " {prompt}",
                "timeout": 30,
            })
            with open(os.path.join(d, "src", "demo", "out.py")) as f:
                self.assertIn("from:写个演示", f.read())
            self.assertTrue(os.path.exists(os.path.join(d, ".swarm-task.md")))
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_failing_command_raises(self):
        from swarm import agents
        d = tempfile.mkdtemp()
        try:
            with self.assertRaises(agents.AgentError):
                agents.run_cli({
                    "workdir": d, "prompt": "x", "files": [], "file_list": [],
                    "task_id": "t", "title": "t", "node": "n",
                    "log": lambda *_a: None, "cmd": "exit 3", "timeout": 10,
                })
        finally:
            shutil.rmtree(d, ignore_errors=True)


# --------------------------------------------------------------------------
class TestEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fx = HubFixture()
        for i in range(2):
            cls.fx.start_node("node-%d" % (i + 1), slots=2)
        wait_for(lambda: len(cls.fx.hub.nodes) >= 2, timeout=15, message="节点没上线")

    @classmethod
    def tearDownClass(cls):
        cls.fx.close()

    def test_1_auth_required(self):
        bad = HubClient("127.0.0.1", self.fx.port, "wrong-token", timeout=10)
        with self.assertRaises(AuthError):
            bad.get_json("/api/state")

    def test_2_run_project(self):
        spec = ("## 数据层 (src/data/**)\nSQLite 存储。\n\n"
                "## 界面 (src/ui/**)\n账单列表。\n")
        res = self.fx.client.call("POST", "/api/submit", {"title": "测试项目", "spec": spec})
        pid = res["project"]
        self.assertGreaterEqual(res["tasks"], 4)

        def finished():
            st = self.fx.state()
            p = next((x for x in st["projects"] if x["id"] == pid), None)
            if p and p["status"] in ("done", "failed", "review"):
                return p
            return None

        proj = wait_for(finished, timeout=90, message="项目没跑完")
        self.assertEqual(proj["status"], "done", proj)
        tree = os.path.join(self.fx.hub.projects[pid]["tree"])
        files = []
        for dirpath, _dirs, names in os.walk(tree):
            if ".git" in dirpath:
                continue
            for n in names:
                files.append(os.path.relpath(os.path.join(dirpath, n), tree))
        self.assertIn("README.md", files)
        self.assertTrue(any(f.startswith("src/data/") for f in files), files)
        self.assertTrue(any(f.startswith("src/ui/") for f in files), files)
        self.assertTrue(os.path.isdir(os.path.join(tree, ".git")))
        # 状态里能拿到每个任务的日志
        st = self.fx.state()
        p = next(x for x in st["projects"] if x["id"] == pid)
        self.assertTrue(all(t["status"] == "done" for t in p["tasks"]))
        self.assertGreater(self.fx.hub.stats["done"], 0)

    def test_3_dashboard(self):
        # 令牌换 cookie，之后看板接口免签名
        import http.client as hc
        conn = hc.HTTPConnection("127.0.0.1", self.fx.port, timeout=10)
        conn.request("GET", "/?t=" + self.fx.hub.token)
        r = conn.getresponse()
        r.read()
        self.assertEqual(r.status, 302)
        cookie = r.getheader("Set-Cookie") or ""
        self.assertIn("swarm_session=", cookie)
        conn.close()
        conn = hc.HTTPConnection("127.0.0.1", self.fx.port, timeout=10)
        conn.request("GET", "/api/state", headers={"Cookie": cookie.split(";")[0]})
        r = conn.getresponse()
        body = json.loads(r.read().decode("utf-8"))
        conn.close()
        self.assertEqual(r.status, 200)
        self.assertIn("projects", body)


# --------------------------------------------------------------------------
class TestLeaseAndConflicts(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(stale_after=2, lease=4)

    def tearDown(self):
        self.fx.close()

    def test_dead_node_task_is_requeued(self):
        spec = "## 唯一模块 (src/only/**)\n做点事\n"
        res = self.fx.client.call("POST", "/api/submit", {"title": "掉线测试", "spec": spec})
        pid = res["project"]

        # 假节点抢一个任务，然后"死掉"（不心跳）
        fake = self.fx.join_fake("zombie")
        got = fake.claim({"node_id": fake.node_id, "slots": 1}, wait=10)
        self.assertIsNotNone(got.get("task"))
        tid = got["task"]["id"]

        # 真节点上线，应当接手
        self.fx.start_node("savior", slots=1)

        def done():
            st = self.fx.state()
            p = next(x for x in st["projects"] if x["id"] == pid)
            if p["status"] in ("done", "failed", "review"):
                return p
            return None

        p = wait_for(done, timeout=90, message="任务没有被重新派发")
        self.assertEqual(p["status"], "done")
        task = next(t for t in p["tasks"] if t["id"] == tid)
        self.assertGreaterEqual(task["attempts"], 2)

    def test_conflict_then_resolve(self):
        hub = self.fx.hub
        plan = {"title": "冲突测试", "tasks": [
            {"id": "t1", "title": "A", "read": ["**"], "write": ["src/shared/**"], "deps": []},
            {"id": "t2", "title": "B", "read": ["**"], "write": ["src/shared/**"], "deps": []},
        ]}
        p = hub.create_project("冲突", "", plan)
        pid = p["id"]
        tree = p["tree"]
        os.makedirs(os.path.join(tree, "src", "shared"), exist_ok=True)
        with open(os.path.join(tree, "src", "shared", "x.py"), "w") as f:
            f.write("v0\n")

        a = self.fx.join_fake("A")
        b = self.fx.join_fake("B")
        with hub.cond:
            got_a = hub._dispatch_locked(pid, "t1", a.node_id)
        self.assertEqual(got_a["task"]["id"], "t1")

        # 绕开"范围不重叠"的调度约束，强行让 B 也拿到同一文件
        with hub.cond:
            got_b = hub._dispatch_locked(pid, "t2", b.node_id)
        self.assertEqual(got_b["task"]["id"], "t2")

        sha_a = self.fx.hub.blobs.put_bytes(b"v-a\n")
        sha_b = self.fx.hub.blobs.put_bytes(b"v-b\n")
        hub.complete(a.node_id, {"task_id": "t1",
                                 "changes": [{"path": "src/shared/x.py", "sha": sha_a}]})
        res = hub.complete(b.node_id, {"task_id": "t2",
                                       "changes": [{"path": "src/shared/x.py", "sha": sha_b}]})
        self.assertEqual(res["conflicts"], 1)
        self.assertEqual(hub.projects[pid]["tasks"]["t2"]["status"], "conflict")
        with open(os.path.join(tree, "src", "shared", "x.py")) as f:
            self.assertEqual(f.read(), "v-a\n")

        hub.resolve({"project": pid, "task": "t2", "path": "src/shared/x.py", "choice": "theirs"})
        with open(os.path.join(tree, "src", "shared", "x.py")) as f:
            self.assertEqual(f.read(), "v-b\n")
        self.assertEqual(hub.projects[pid]["tasks"]["t2"]["status"], "done")

    def test_out_of_scope_rejected(self):
        hub = self.fx.hub
        plan = {"title": "越界", "tasks": [
            {"id": "t1", "title": "A", "read": ["**"], "write": ["src/a/**"], "deps": []},
        ]}
        p = hub.create_project("越界", "", plan)
        a = self.fx.join_fake("C")
        with hub.cond:
            hub._dispatch_locked(p["id"], "t1", a.node_id)
        sha = hub.blobs.put_bytes(b"hacked\n")
        res = hub.complete(a.node_id, {"task_id": "t1",
                                       "changes": [{"path": "src/secret.txt", "sha": sha}]})
        self.assertEqual(res["applied"], 0)
        self.assertEqual(res["conflicts"], 1)
        self.assertFalse(os.path.exists(os.path.join(p["tree"], "src", "secret.txt")))
        reason = hub.projects[p["id"]]["tasks"]["t1"]["conflicts"][0]["reason"]
        self.assertIn("越界", reason)


if __name__ == "__main__":
    unittest.main(verbosity=2)

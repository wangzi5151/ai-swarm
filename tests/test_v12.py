"""v1.2 新增能力与加固的回归测试：TLS、自动三方合并、删除冲突、重试反馈、
过期尝试、备份恢复、快照索引、导出/删除接口、日志裁剪、HTTP 安全头。"""

from __future__ import annotations

import http.client
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
from swarm import agents, cli, proto, workspace                 # noqa: E402
from swarm.hub import LOG_PERSIST, Hub                          # noqa: E402
from swarm.proto import HubClient, SwarmError                   # noqa: E402


def plan_one(tid="t1", write=None, verify=None):
    task = {"id": tid, "title": tid, "prompt": "p", "read": ["**"],
            "write": write or ["**"], "deps": []}
    if verify:
        task["verify"] = verify
    return {"title": "p", "tasks": [task]}


class TestMerge3(unittest.TestCase):
    def test_disjoint_edits_merge(self):
        base = "a\nb\nc\nd\n"
        self.assertEqual(workspace.merge3(base, "A\nb\nc\nd\n", "a\nb\nc\nD\n"),
                         "A\nb\nc\nD\n")

    def test_overlapping_edits_conflict(self):
        self.assertIsNone(workspace.merge3("a\nb\n", "X\nb\n", "Y\nb\n"))

    def test_identical_change_not_conflict(self):
        self.assertEqual(workspace.merge3("a\nb\n", "X\nb\n", "X\nb\n"), "X\nb\n")
        # 两边都改了同一行、其中一边还多改了一处：相同部分不能算冲突
        self.assertEqual(workspace.merge3("a\nb\nc\n", "a\nB\nc\n", "a\nB\nc\nd\n"),
                         "a\nB\nc\nd\n")

    def test_same_position_inserts(self):
        self.assertEqual(workspace.merge3("a\n", "a\nx\n", "a\ny\n"), "a\nx\ny\n")
        self.assertEqual(workspace.merge3("a\n", "x\na\n", "y\na\n"), "x\ny\na\n")

    def test_one_side_unchanged(self):
        self.assertEqual(workspace.merge3("a\n", "a\n", "b\n"), "b\n")


class TestHubV12(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False, auto_merge=False)
        self.pid = self.fx.hub.create_project("v12", "", plan_one())["id"]

    def tearDown(self):
        self.fx.close()

    def _write(self, rel, text):
        full = os.path.join(self.fx.hub.projects[self.pid]["tree"], rel)
        os.makedirs(os.path.dirname(full) or ".", exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(text)
        return full

    def test_auto_merge_disjoint_same_file(self):
        self._write("doc.txt", "a\nb\nc\n")
        node = self.fx.join_fake("auto")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", node.node_id)
        self._write("doc.txt", "A\nb\nc\n")     # 模拟另一个任务先合并了第一行
        sha = self.fx.hub.blobs.put_bytes(b"a\nb\nC\n")
        self.fx.hub.auto_merge = True
        res = self.fx.hub.complete(node.node_id, {
            "project": self.pid, "task_id": "t1", "attempt": 1,
            "changes": [{"path": "doc.txt", "sha": sha}]})
        self.assertEqual(res["conflicts"], 0)
        self.assertEqual(res["task_status"], "done")
        with open(os.path.join(self.fx.hub.projects[self.pid]["tree"], "doc.txt")) as f:
            self.assertEqual(f.read(), "A\nb\nC\n")
        task = self.fx.hub.projects[self.pid]["tasks"]["t1"]
        self.assertTrue(task["applied_record"][0].get("auto_merged"))

    def test_auto_merge_overlap_still_conflicts(self):
        self._write("doc.txt", "a\nb\n")
        node = self.fx.join_fake("auto2")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", node.node_id)
        self._write("doc.txt", "A\nb\n")
        sha = self.fx.hub.blobs.put_bytes(b"B\nb\n")
        self.fx.hub.auto_merge = True
        res = self.fx.hub.complete(node.node_id, {
            "project": self.pid, "task_id": "t1", "attempt": 1,
            "changes": [{"path": "doc.txt", "sha": sha}]})
        self.assertEqual(res["task_status"], "conflict")

    def test_deleted_file_conflict_can_be_resolved(self):
        self._write("gone.txt", "old\n")
        node = self.fx.join_fake("del")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", node.node_id)
        self._write("gone.txt", "changed by other\n")
        res = self.fx.hub.complete(node.node_id, {
            "project": self.pid, "task_id": "t1", "attempt": 1,
            "changes": [{"path": "gone.txt", "sha": None, "deleted": True}]})
        self.assertEqual(res["task_status"], "conflict")
        item = self.fx.hub.projects[self.pid]["tasks"]["t1"]["conflicts"][0]
        self.assertTrue(item.get("deleted"))
        self.fx.hub.resolve({"project": self.pid, "task": "t1",
                             "path": "gone.txt", "choice": "theirs"})
        self.assertFalse(os.path.exists(
            os.path.join(self.fx.hub.projects[self.pid]["tree"], "gone.txt")))
        self.assertEqual(self.fx.hub.projects[self.pid]["tasks"]["t1"]["status"], "done")

    def test_stale_attempt_cannot_fail_new_run(self):
        node = self.fx.join_fake("stale")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", node.node_id)
        self.fx.hub._requeue(self.pid, "t1", "模拟丢失")
        other = self.fx.join_fake("stale2")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", other.node_id)
        late = self.fx.hub.fail(node.node_id, {
            "project": self.pid, "task_id": "t1", "attempt": 1, "error": "late"})
        self.assertTrue(late.get("stale"))
        self.assertEqual(self.fx.hub.projects[self.pid]["tasks"]["t1"]["status"], "running")
        self.assertEqual(self.fx.hub.projects[self.pid]["tasks"]["t1"]["node"], other.node_id)

    def test_stale_progress_is_ignored(self):
        node = self.fx.join_fake("prog")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", node.node_id)
        res = self.fx.hub.progress(node.node_id, {
            "project": self.pid, "task_id": "t1", "attempt": 99, "lines": ["x"]})
        self.assertFalse(res["ok"])
        self.assertNotIn("x", self.fx.hub.projects[self.pid]["tasks"]["t1"]["log"])

    def test_verify_failure_feedback_reaches_next_attempt(self):
        # 先把 setUp 里的 t1 清掉，避免 claim 先拿到它
        pre = self.fx.join_fake("pre")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", pre.node_id)
        self.fx.hub.complete(pre.node_id, {
            "project": self.pid, "task_id": "t1", "attempt": 1,
            "changes": [{"path": "pre.txt", "sha": self.fx.hub.blobs.put_bytes(b"pre\n")}]})
        p = self.fx.hub.create_project("fb", "", plan_one("tv", verify="false"))
        node = self.fx.join_fake("fb1")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "tv", node.node_id)
        res = self.fx.hub.complete(node.node_id, {
            "project": p["id"], "task_id": "tv", "attempt": 1,
            "changes": [{"path": "x.txt", "sha": self.fx.hub.blobs.put_bytes(b"x\n")}],
            "verify": {"cmd": "false", "ok": False, "out": "boom-detail"}})
        self.assertTrue(res.get("failed"))
        fb = p["tasks"]["tv"].get("feedback") or ""
        self.assertIn("boom-detail", fb)
        other = self.fx.join_fake("fb2")
        got = other.claim({"node_id": other.node_id, "slots": 1}, wait=2)
        self.assertEqual(got["task"]["id"], "tv")
        self.assertIn("boom-detail", got["task"].get("feedback") or "")

    def test_persisted_state_trims_logs(self):
        task = self.fx.hub.projects[self.pid]["tasks"]["t1"]
        task["log"] = ["line-%d" % i for i in range(LOG_PERSIST + 80)]
        self.fx.hub._save_state()
        with open(os.path.join(self.fx.hub.root, "state.json"), encoding="utf-8") as f:
            data = json.load(f)
        saved = data["projects"][self.pid]["tasks"]["t1"]["log"]
        self.assertLessEqual(len(saved), LOG_PERSIST)
        self.assertEqual(saved[-1], "line-%d" % (LOG_PERSIST + 79))

    def test_delete_project_clears_node_running(self):
        node = self.fx.join_fake("delnode")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", node.node_id)
        self.fx.hub.delete_project(self.pid)
        self.assertNotIn(self.pid, self.fx.hub.projects)
        self.assertEqual(self.fx.hub.nodes[node.node_id]["running"], [])
        self.assertFalse(os.path.isdir(
            os.path.join(self.fx.hub.root, "projects", self.pid)))

    def test_rollback_latest_skips_conflict_only_delivery(self):
        plan = {"title": "rb", "tasks": [
            {"id": "a", "title": "a", "prompt": "p", "read": ["**"],
             "write": ["src/a/**"], "deps": []},
            {"id": "b", "title": "b", "prompt": "p", "read": ["**"],
             "write": ["src/b/**"], "deps": []},
        ]}
        p = self.fx.hub.create_project("rb", "", plan)
        n1 = self.fx.join_fake("rb1")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "a", n1.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"first\n")
        self.fx.hub.complete(n1.node_id, {
            "project": p["id"], "task_id": "a", "attempt": 1,
            "changes": [{"path": "src/a/x.txt", "sha": sha}]})
        n2 = self.fx.join_fake("rb2")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(p["id"], "b", n2.node_id)
        bad = self.fx.hub.blobs.put_bytes(b"nope\n")
        r = self.fx.hub.complete(n2.node_id, {
            "project": p["id"], "task_id": "b", "attempt": 1,
            "changes": [{"path": "src/a/y.txt", "sha": bad}]})
        self.assertEqual(r["task_status"], "conflict")
        self.assertEqual(r["applied"], 0)
        res = self.fx.hub.rollback({"project": p["id"]})
        self.assertEqual(res["restored"], ["src/a/x.txt"])

    def test_retry_rejects_done_task(self):
        node = self.fx.join_fake("done")
        with self.fx.hub.cond:
            self.fx.hub._dispatch_locked(self.pid, "t1", node.node_id)
        sha = self.fx.hub.blobs.put_bytes(b"ok\n")
        self.fx.hub.complete(node.node_id, {
            "project": self.pid, "task_id": "t1", "attempt": 1,
            "changes": [{"path": "ok.txt", "sha": sha}]})
        res = self.fx.client.raw("POST", "/api/control",
                                 json.dumps({"project": self.pid, "action": "retry",
                                             "task": "t1"}).encode())
        self.assertEqual(res[0], 400)


class TestRestartRecovery(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="swarm-v12-")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_rollback_latest_works_after_restart(self):
        hub = Hub(os.path.join(self.root, "hub"), port=free_port(),
                  discovery_port=free_port(), verbose=False, use_git=False)
        pid = hub.create_project("r", "", plan_one())["id"]
        # 直接走内部派发，不需要起 server
        nid = "fake1"
        hub.join({"node_id": nid, "name": "fake", "kinds": ["mock"], "slots": 1})
        with hub.cond:
            hub._dispatch_locked(pid, "t1", nid)
        sha = hub.blobs.put_bytes(b"new\n")
        hub.complete(nid, {"project": pid, "task_id": "t1", "attempt": 1,
                           "changes": [{"path": "file.txt", "sha": sha}]})
        tree = os.path.join(hub.root, "projects", pid, "tree")
        self.assertTrue(os.path.exists(os.path.join(tree, "file.txt")))
        hub.shutdown()

        hub2 = Hub(os.path.join(self.root, "hub"), port=free_port(),
                   discovery_port=free_port(), verbose=False, use_git=False)
        self.assertIn(pid, hub2.projects)
        res = hub2.rollback({"project": pid})
        self.assertEqual(res["restored"], ["file.txt"])
        self.assertFalse(os.path.exists(os.path.join(tree, "file.txt")))
        hub2.shutdown()

    def test_snapshot_index_survives_restart(self):
        hub = Hub(os.path.join(self.root, "hub"), port=free_port(),
                  discovery_port=free_port(), verbose=False, use_git=False)
        pid = hub.create_project("idx", "", plan_one())["id"]
        nid = "fake1"
        hub.join({"node_id": nid, "name": "fake", "kinds": ["mock"], "slots": 1})
        with hub.cond:
            hub._dispatch_locked(pid, "t1", nid)
        hub._save_snap_index(pid)
        hub.shutdown()

        hub2 = Hub(os.path.join(self.root, "hub"), port=free_port(),
                   discovery_port=free_port(), verbose=False, use_git=False)
        index = hub2._snap_index_for(pid)
        self.assertTrue(index, "重启后没有加载快照索引")
        key = next(iter(index))
        self.assertIsInstance(key, tuple)
        self.assertEqual(len(key), 3)
        hub2.shutdown()

    def test_backup_restore_roundtrip(self):
        hub = Hub(os.path.join(self.root, "hub"), port=free_port(),
                  discovery_port=free_port(), verbose=False, use_git=False)
        pid = hub.create_project("bk", "", plan_one())["id"]
        nid = "fake1"
        hub.join({"node_id": nid, "name": "fake", "kinds": ["mock"], "slots": 1})
        with hub.cond:
            hub._dispatch_locked(pid, "t1", nid)
        sha = hub.blobs.put_bytes(b"backup-me\n")
        hub.complete(nid, {"project": pid, "task_id": "t1", "attempt": 1,
                           "changes": [{"path": "out.txt", "sha": sha}]})
        token = hub.token
        dest = os.path.join(self.root, "backup.tar.gz")
        hub.backup_archive(dest)
        hub.shutdown()

        new_root = os.path.join(self.root, "restored")
        count = cli._safe_extract(dest, new_root)
        self.assertGreater(count, 0)
        hub2 = Hub(new_root, port=free_port(), discovery_port=free_port(),
                   verbose=False, use_git=False)
        self.assertEqual(hub2.token, token)
        self.assertIn(pid, hub2.projects)
        with open(os.path.join(new_root, "projects", pid, "tree", "out.txt")) as f:
            self.assertEqual(f.read(), "backup-me\n")
        hub2.shutdown()

    def test_restore_rejects_traversal(self):
        dest = os.path.join(self.root, "split")
        archive = os.path.join(self.root, "evil.tar.gz")
        with tarfile.open(archive, "w:gz") as tar:
            data = b"pwn\n"
            info = tarfile.TarInfo("../outside.txt")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        cli._safe_extract(archive, dest)
        self.assertFalse(os.path.exists(os.path.join(self.root, "outside.txt")))


class TestTls(unittest.TestCase):
    def setUp(self):
        if not shutil.which("openssl"):
            self.skipTest("没有 openssl，跳过 TLS 用例")
        self.fx = HubFixture(tls=True, use_git=False)

    def tearDown(self):
        self.fx.close()

    def test_https_roundtrip_with_pin(self):
        pin = self.fx.hub.tls_fingerprint
        self.assertTrue(pin)
        c = HubClient("127.0.0.1", self.fx.hub.port, self.fx.hub.token,
                      scheme="https", tls_pin=pin, timeout=15)
        try:
            self.assertTrue(c.ping()["ok"])
            res = c.join({"name": "tlsnode", "kinds": ["mock"], "slots": 1})
            self.assertTrue(res["node_id"])
        finally:
            c.close_all()

    def test_wrong_pin_is_rejected(self):
        c = HubClient("127.0.0.1", self.fx.hub.port, self.fx.hub.token,
                      scheme="https", tls_pin="00" * 32, timeout=10)
        try:
            with self.assertRaises(SwarmError):
                c.ping()
        finally:
            c.close_all()


class TestHttpEndpoints(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False)
        self.pid = self.fx.hub.create_project("http", "", plan_one())["id"]

    def tearDown(self):
        self.fx.close()

    def test_export_returns_tar(self):
        status, data = self.fx.client.raw_full(
            "GET", "/api/export?project=%s" % self.pid, timeout=30)[:2]
        self.assertEqual(status, 200)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            names = tar.getnames()
        self.assertIn("README.md", names)

    def test_delete_endpoint(self):
        res = self.fx.client.call("POST", "/api/delete", {"project": self.pid})
        self.assertTrue(res["ok"])
        self.assertNotIn(self.pid, self.fx.hub.projects)

    def test_unauth_post_rejected_before_body(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.fx.hub.port, timeout=10)
        try:
            conn.request("POST", "/api/submit", body=b"{}",
                         headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            resp.read()
            self.assertEqual(resp.status, 401)
        finally:
            conn.close()

    def test_dashboard_has_security_headers(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.fx.hub.port, timeout=10)
        try:
            conn.request("GET", "/")
            resp = conn.getresponse()
            resp.read()
            self.assertEqual(resp.status, 200)
            headers = {k.lower(): v for k, v in resp.getheaders()}
            self.assertIn("content-security-policy", headers)
            self.assertEqual(headers.get("x-frame-options"), "DENY")
            self.assertEqual(headers.get("referrer-policy"), "no-referrer")
        finally:
            conn.close()


class TestPathAndLimits(unittest.TestCase):
    def test_apply_change_keeps_exec_bit(self):
        root = tempfile.mkdtemp()
        blobs = tempfile.mkdtemp()
        try:
            from swarm.store import BlobStore
            store = BlobStore(blobs)
            sha = store.put_bytes(b"#!/bin/sh\necho hi\n")
            err = workspace.apply_change(root, {"path": "run.sh", "sha": sha,
                                                "mode": 0o755}, store)
            self.assertIsNone(err)
            mode = os.stat(os.path.join(root, "run.sh")).st_mode & 0o777
            self.assertEqual(mode, 0o755)
        finally:
            shutil.rmtree(root, ignore_errors=True)
            shutil.rmtree(blobs, ignore_errors=True)

    def test_gitignore_change_does_not_fake_delete(self):
        root = tempfile.mkdtemp()
        blobs = tempfile.mkdtemp()
        try:
            from swarm.store import BlobStore
            os.makedirs(os.path.join(root, "src"))
            with open(os.path.join(root, "src", "keep.py"), "w") as f:
                f.write("keep\n")
            base = workspace.snapshot(root, BlobStore(blobs), patterns=["**"])
            with open(os.path.join(root, ".gitignore"), "w") as f:
                f.write("src/keep.py\n")
            changes = workspace.diff_work(root, base, ignore=workspace.load_ignores(root))
            self.assertFalse(any(c["path"] == "src/keep.py" and c.get("deleted")
                                 for c in changes), changes)
        finally:
            shutil.rmtree(root, ignore_errors=True)
            shutil.rmtree(blobs, ignore_errors=True)

    def test_symlink_parent_rejected(self):
        root = tempfile.mkdtemp()
        outside = tempfile.mkdtemp()
        try:
            os.symlink(outside, os.path.join(root, "link"))
            err = workspace.apply_change(root, {"path": "link/pwn.txt",
                                                "sha": "0" * 64}, None)
            self.assertEqual(err, "路径经过符号链接")
            self.assertFalse(os.path.exists(os.path.join(outside, "pwn.txt")))
        finally:
            shutil.rmtree(root, ignore_errors=True)
            shutil.rmtree(outside, ignore_errors=True)

    def test_plan_rejects_too_many_tasks(self):
        plan = {"tasks": [{"id": "t%d" % i} for i in range(2100)]}
        with self.assertRaises(ValueError):
            from swarm import planner
            planner.validate(plan)

    def test_deep_chain_does_not_blow_recursion(self):
        from swarm import planner
        n = 1500
        plan = {"tasks": [{"id": "t%d" % i, "deps": ([] if i == 0 else ["t%d" % (i - 1)])}
                          for i in range(n)]}
        planner.validate(plan)
        planner.critical_path_order(plan)
        self.assertEqual(plan["tasks"][-1]["priority"], n - 1)

    def test_plan_rejects_huge_prompt(self):
        from swarm import planner
        with self.assertRaises(ValueError):
            planner.validate({"tasks": [{"id": "t", "prompt": "x" * 300000}]})

    def test_split_hub_addr_schemes(self):
        self.assertEqual(proto.split_hub_addr("https://1.2.3.4:9", 47770),
                         ("https", "1.2.3.4", 9))
        self.assertEqual(proto.split_hub_addr("1.2.3.4", 47770),
                         ("http", "1.2.3.4", 47770))
        with self.assertRaises(SwarmError):
            proto.split_hub_addr("ftp://1.2.3.4", 47770)
        with self.assertRaises(SwarmError):
            proto.split_hub_addr("1.2.3.4:notaport", 47770)


class TestBackupEndpoint(unittest.TestCase):
    def setUp(self):
        self.fx = HubFixture(use_git=False)
        self.pid = self.fx.hub.create_project("bk", "", plan_one())["id"]

    def tearDown(self):
        self.fx.close()

    def test_backup_endpoint_returns_archive(self):
        status, data = self.fx.client.raw_full(
            "GET", "/api/backup?blobs=0", timeout=60)[:2]
        self.assertEqual(status, 200)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            names = tar.getnames()
        self.assertIn("state.json", names)
        self.assertIn("token", names)
        self.assertFalse(any(n.startswith("blobs/") for n in names))


class TestCliWiring(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="swarm-cli-")
        self.hub_dir = os.path.join(self.root, "hub")
        hub = Hub(self.hub_dir, port=free_port(), discovery_port=free_port(),
                  verbose=False, use_git=False)
        self.pid = hub.create_project("cli", "", plan_one())["id"]
        hub.shutdown()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_backup_and_restore_commands(self):
        dest = os.path.join(self.root, "b.tar.gz")
        rc = cli.main(["backup", "--root", self.hub_dir, "-o", dest, "--no-blobs"])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.exists(dest))
        new_root = os.path.join(self.root, "restored")
        rc = cli.main(["restore", dest, "--root", new_root])
        self.assertEqual(rc, 0)
        hub2 = Hub(new_root, port=free_port(), discovery_port=free_port(),
                   verbose=False, use_git=False)
        try:
            self.assertIn(self.pid, hub2.projects)
        finally:
            hub2.shutdown()

    def test_offline_delete_is_persisted(self):
        rc = cli.main(["delete", self.pid, "--root", self.hub_dir])
        self.assertEqual(rc, 0)
        hub2 = Hub(self.hub_dir, port=free_port(), discovery_port=free_port(),
                   verbose=False, use_git=False)
        try:
            self.assertNotIn(self.pid, hub2.projects)
        finally:
            hub2.shutdown()


class TestFeedbackCli(unittest.TestCase):
    def test_run_cli_writes_feedback_file_and_placeholder(self):
        d = tempfile.mkdtemp()
        try:
            out = os.path.join(d, "result.txt")
            ctx = {
                "workdir": d, "prompt": "do it", "files": ["**"], "file_list": [],
                "task_id": "t", "title": "t", "log": lambda *_a: None,
                "cmd": "cat .swarm-feedback.md > %s && printf '%%s' '{feedback}' >> %s"
                       % (out, out),
                "timeout": 30, "feedback": "上次没通过：ImportError",
            }
            agents.run_cli(ctx)
            with open(out, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("ImportError", text)
            self.assertTrue(text.count("ImportError") >= 2)
            ignored = {c["path"] for c in workspace.diff_work(
                d, [], ignore=workspace.load_ignores(d))}
            self.assertNotIn(".swarm-feedback.md", ignored)
            self.assertNotIn(".swarm-task.md", ignored)
        finally:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

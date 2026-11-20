"""局域网自动发现（UDP）的测试。

CI 里只跑"单播一问一答"：这一条是确定性的，不依赖广播能不能出网卡。
真正的广播发现默认跳过——有些容器 / 防火墙 / 云主机会拦 255.255.255.255，
那种环境里跑它只会误报红。本地想看完整效果：

    SWARM_TEST_UDP=1 python3 -m unittest tests.test_discover -v
"""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from swarm import discover  # noqa: E402


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class TestDiscovery(unittest.TestCase):
    def setUp(self):
        self.port = free_udp_port()
        self.stop = threading.Event()
        self.thread = discover.serve_responder(
            self.port, {"name": "ci-hub", "port": 47770}, self.stop)

    def tearDown(self):
        self.stop.set()
        self.thread.join(timeout=2)

    def test_responder_answers_unicast_probe(self):
        """节点单播问一声，hub 必须回自己的名字和端口。"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(0.2)
        payload = json.dumps(discover.QUERY).encode("utf-8")
        msg = None
        try:
            deadline = time.time() + 3.0
            while time.time() < deadline and msg is None:
                try:
                    sock.sendto(payload, ("127.0.0.1", self.port))
                    data, _addr = sock.recvfrom(4096)
                except socket.timeout:
                    continue          # responder 线程可能还没 bind 好
                except OSError as exc:  # 沙箱里禁 UDP，跳过而不是报错
                    self.skipTest("本机 UDP 不可用：%s" % exc)
                msg = json.loads(data.decode("utf-8"))
        finally:
            sock.close()
        self.assertIsNotNone(msg, "3 秒内没有收到发现应答")
        self.assertEqual(msg.get("a"), "hub")
        self.assertEqual(msg.get("name"), "ci-hub")
        self.assertEqual(msg.get("port"), 47770)
        self.assertEqual(msg.get("proto"), discover.PROTO)

    def test_responder_ignores_unrelated_packets(self):
        """别的程序乱发的 UDP 包不能让 responder 崩溃/乱回。"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(0.5)
        try:
            time.sleep(0.3)  # 等 responder bind 好
            for junk in (b"", b"not json", json.dumps({"q": "other"}).encode()):
                sock.sendto(junk, ("127.0.0.1", self.port))
            with self.assertRaises(socket.timeout):
                sock.recvfrom(4096)
        finally:
            sock.close()
        self.assertTrue(self.thread.is_alive(), "responder 不该因为垃圾包退出")

    @unittest.skipUnless(
        os.environ.get("SWARM_TEST_UDP") == "1",
        "广播发现在部分容器/CI 会被拦；本地用 SWARM_TEST_UDP=1 打开",
    )
    def test_find_hub_via_broadcast(self):
        """节点侧广播三次，能找到同一网段里的 hub（真实链路）。"""
        found = discover.find_hub(self.port, timeout=3.0)
        self.assertIsNotNone(found, "广播没找到 hub")
        _host, port, info = found
        self.assertEqual(port, 47770)
        self.assertEqual(info.get("name"), "ci-hub")


if __name__ == "__main__":
    unittest.main(verbosity=2)

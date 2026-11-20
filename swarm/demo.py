"""一条命令在单机上演示整套流程（真实网络栈：HTTP + 多线程 + 多节点）。"""

from __future__ import annotations

import os
import shutil
import socket
import sys
import tempfile
import threading
import time

from .hub import Hub
from .node import Node

DEMO_SPEC = """# 局域网记账 App

## 需求
两台手机互通，一起记一笔账；离线可用，回到同一 Wi-Fi 自动同步。

## 数据层 (src/data/**)
SQLite 表结构、迁移、聚合查询。

## 界面 (src/ui/**)
账单列表、记一笔、月度统计。

## 同步 (src/sync/**)
mDNS 发现 + 冲突合并。

## 预算 (src/budget/**)
月度预算与超支提醒。
"""


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def run_demo(nodes=3, keep=False, root=None):
    base = root or tempfile.mkdtemp(prefix="swarm-demo-")
    os.makedirs(base, exist_ok=True)
    port = _free_port()
    dport = _free_port()
    hub = Hub(os.path.join(base, "hub"), name="demo-hub", port=port,
              discovery_port=dport, verbose=False, use_git=True)
    srv = hub.make_server()
    hub.start_background()
    threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.3}, daemon=True).start()

    print("=" * 62)
    print(" Swarm 演示：hub 在 127.0.0.1:%d，%d 个模拟节点各自独立工作" % (port, nodes))
    print(" 看板可以用浏览器打开  http://127.0.0.1:%d/?t=%s" % (port, hub.token[:8] + "…"))
    print("=" * 62)

    node_objs = []
    for i in range(nodes):
        nd = Node(
            root=os.path.join(base, "node%d" % i),
            hub="127.0.0.1:%d" % port,
            token=hub.token,
            name="node-%d" % (i + 1),
            slots=2,
            mock_delay=0.25 + 0.15 * i,
            verbose=False,
        )
        threading.Thread(target=nd.run, daemon=True).start()
        node_objs.append(nd)

    deadline = time.time() + 15
    while len(hub.nodes) < nodes and time.time() < deadline:
        time.sleep(0.1)
    print("在线节点：%s" % ", ".join(n["name"] for n in hub.nodes.values()))

    hub.log = lambda msg, level="info": None  # 演示时用下面的自绘时间线
    project = hub.create_project("局域网记账 App", DEMO_SPEC)

    seen = {}
    t0 = time.time()
    last_pid = None
    deadline = time.time() + 180
    while time.time() < deadline:
        state = hub.snapshot_state()
        proj = state["projects"][0]
        for t in proj["tasks"]:
            key = (t["id"], t["status"])
            if key != seen.get(t["id"]):
                seen[t["id"]] = key
                who = t["node"] or ""
                line = " %5.1fs  %-16s %-9s %s %s" % (
                    time.time() - t0, t["id"], t["status"], t["title"], who)
                print(line)
                last_pid = t["id"]
        if proj["status"] in ("done", "failed", "review", "cancelled"):
            break
        time.sleep(0.15)

    state = hub.snapshot_state()
    proj = state["projects"][0]
    print("-" * 62)
    print(" 项目状态：%s   用时 %.1fs" % (proj["status"], time.time() - t0))
    for t in proj["tasks"]:
        st = t.get("stats") or {}
        print("   %-16s %-8s 文件 %d" % (t["id"], t["status"], st.get("files", 0)))

    tree = os.path.join(base, "hub", "projects", proj["id"], "tree")
    print("\n 产出目录：%s" % tree)
    for rel in sorted(_walk(tree))[:20]:
        print("   " + rel)

    for nd in node_objs:
        nd.shutdown()
    hub.shutdown()
    try:
        srv.shutdown()
    except Exception:
        pass

    if keep:
        print("\n数据保留在：%s" % base)
    else:
        shutil.rmtree(base, ignore_errors=True)
        print("\n演示目录已清理（--keep 可保留）")
    ok = proj["status"] == "done"
    print("演示%s" % ("成功 ✅" if ok else "未完成 ❌"))
    return 0 if ok else 1


def _walk(root):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d != ".git"]
        for name in filenames:
            yield os.path.relpath(os.path.join(dirpath, name), root).replace(os.sep, "/")

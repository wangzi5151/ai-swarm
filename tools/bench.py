#!/usr/bin/env python3
"""真网络栈吞吐基准：本机起 1 个 hub + N 个节点，提交 K 个独立任务。

    python3 tools/bench.py --tasks 24 --nodes 3
    python3 tools/bench.py --tasks 100 --nodes 4 --json

输出任务/秒、平均/中位/95 分位任务耗时、blob 上传下载字节数。
这些都是可复现的数字，不用"感觉很快"。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import statistics
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from swarm.hub import Hub          # noqa: E402
from swarm.node import Node        # noqa: E402


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def run(tasks=24, nodes=3, delay=0.05, slots=2, timeout=180):
    base = tempfile.mkdtemp(prefix="swarm-bench-")
    port = free_port()
    dport = free_port()
    hub = Hub(os.path.join(base, "hub"), name="bench", port=port,
              discovery_port=dport, verbose=False, use_git=False)
    srv = hub.make_server()
    hub.start_background()
    threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.2},
                     daemon=True).start()

    node_objs = []
    for i in range(nodes):
        nd = Node(root=os.path.join(base, "node%d" % i),
                  hub="127.0.0.1:%d" % port, token=hub.token,
                  name="bench-%d" % (i + 1), slots=slots,
                  mock_delay=delay, verbose=False)
        threading.Thread(target=nd.run, daemon=True).start()
        node_objs.append(nd)
    deadline = time.time() + 15
    while len(hub.nodes) < nodes and time.time() < deadline:
        time.sleep(0.05)
    if len(hub.nodes) < nodes:
        raise SystemExit("节点没能在 15 秒内全部连上：%d/%d" % (len(hub.nodes), nodes))

    plan = {
        "title": "bench",
        "tasks": [{
            "id": "bench-%03d" % i,
            "title": "bench-%03d" % i,
            "prompt": "写一个模块",
            "read": ["README.md"],
            "write": ["src/mod%03d/**" % i],
            "deps": [],
            "priority": i % 3,
        } for i in range(tasks)],
    }
    t0 = time.time()
    project = hub.create_project("bench", "", plan)
    pid = project["id"]
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = hub.projects.get(pid)
        if p and p["status"] in ("done", "failed", "review", "cancelled"):
            break
        time.sleep(0.05)
    elapsed = time.time() - t0
    p = hub.projects[pid]
    durations = [(t.get("ended") or 0) - (t.get("started") or 0)
                 for t in p["tasks"].values() if t.get("started") and t.get("ended")]
    durations.sort()
    stats = dict(hub.stats)
    result = {
        "tasks": tasks,
        "nodes": nodes,
        "slots": slots,
        "status": p["status"],
        "seconds": round(elapsed, 3),
        "tasks_per_sec": round(tasks / elapsed, 2) if elapsed else None,
        "task_ms_mean": round(statistics.mean(durations) * 1000, 1) if durations else None,
        "task_ms_p50": round(durations[len(durations) // 2] * 1000, 1) if durations else None,
        "task_ms_p95": round(durations[min(len(durations) - 1,
                                          int(len(durations) * 0.95))] * 1000, 1) if durations else None,
        "blob_in_kb": round(stats.get("blob_in", 0) / 1024.0, 1),
        "blob_out_kb": round(stats.get("blob_out", 0) / 1024.0, 1),
    }

    for nd in node_objs:
        nd.shutdown()
    hub.shutdown()
    try:
        srv.shutdown()
    except Exception:
        pass
    shutil.rmtree(base, ignore_errors=True)
    return result


def main():
    ap = argparse.ArgumentParser(description="Swarm 吞吐基准")
    ap.add_argument("--tasks", type=int, default=24)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--slots", type=int, default=2)
    ap.add_argument("--delay", type=float, default=0.05, help="mock agent 的模拟耗时")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    res = run(tasks=args.tasks, nodes=args.nodes, slots=args.slots, delay=args.delay)
    if args.json:
        print(json.dumps(res, ensure_ascii=False))
    else:
        print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res["status"] == "done" else 1


if __name__ == "__main__":
    sys.exit(main())

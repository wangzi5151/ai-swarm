"""命令行入口。

    ./swarm hub                          # 在"调度机"上
    ./swarm node --discover              # 在每台干活的 Linux 上
    ./swarm submit -f spec.md --watch    # 交一个项目
    ./swarm status                       # 看进度
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tarfile
import time

from . import DEFAULT_DISCOVERY_PORT, DEFAULT_PORT, __version__
from . import planner, proto
from .proto import HubClient, SwarmError


# --------------------------------------------------------------------------
# 连接
# --------------------------------------------------------------------------

def _client(args, need_token=True):
    hub = getattr(args, "hub", None) or os.environ.get("SWARM_HUB")
    token = getattr(args, "token", None) or os.environ.get("SWARM_TOKEN")
    if not hub:
        raise SwarmError("没指定 hub：用 --hub <ip:port> 或设 SWARM_HUB")
    host, port = proto.parse_hub_addr(hub, DEFAULT_PORT)
    if not token:
        root = getattr(args, "root", None)
        if root:
            path = os.path.join(os.path.abspath(root), "token")
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    token = f.read().strip()
    if need_token and not token:
        raise SwarmError("没指定令牌：用 --token 或设 SWARM_TOKEN（hub 启动时会打印）")
    return HubClient(host, port, token or "", timeout=30)


def _read_spec(path):
    if not path or path == "-":
        data = sys.stdin.read()
        return data
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# --------------------------------------------------------------------------
# hub / node
# --------------------------------------------------------------------------

def cmd_hub(args):
    from .hub import Hub
    hub = Hub(
        root=args.root, name=args.name, token=args.token, port=args.port,
        bind=args.bind, discovery_port=args.discovery_port, lease=args.lease,
        stale_after=args.stale, max_attempts=args.max_attempts,
        use_git=not args.no_git, verbose=not args.quiet,
    )
    hub.serve_forever()
    return 0


def cmd_node(args):
    from .node import Node
    if args.agent == "cli" and not (args.cmd or os.environ.get("SWARM_AGENT_CMD")):
        sys.stderr.write("提示：--agent cli 需要 --cmd '你的 agent 命令'，例如\n"
                         "  --cmd 'aider --yes --no-auto-commits --message {prompt}'\n")
    node = Node(
        root=args.root, hub=None if args.discover else args.hub,
        token=args.token, name=args.name, slots=args.slots, kind=args.agent,
        cmd=args.cmd, base_url=args.llm_base, model=args.model, api_key=args.api_key,
        mock_delay=args.mock_delay, discovery_port=args.discovery_port,
        keep_work=args.keep_work,
    )
    return node.run()


# --------------------------------------------------------------------------
# submit / status / control
# --------------------------------------------------------------------------

def cmd_submit(args):
    spec = _read_spec(args.file)
    payload = {"title": args.title or "", "spec": spec}
    if args.plan:
        with open(args.plan, "r", encoding="utf-8") as f:
            payload["plan"] = json.load(f)
    client = _client(args)
    res = client.call("POST", "/api/submit", payload)
    print("项目 %s「%s」已建立：%d 个任务，开始派发。" % (res["project"], res["title"], res["tasks"]))
    if args.watch:
        return _watch(client, res["project"])
    return 0


def _watch(client, pid):
    last = {}
    deadline = time.time() + 24 * 3600
    try:
        while time.time() < deadline:
            state = client.get_json("/api/state")
            proj = next((p for p in state["projects"] if p["id"] == pid), None)
            if proj is None:
                print("项目不见了")
                return 1
            for t in proj["tasks"]:
                if last.get(t["id"]) != t["status"]:
                    last[t["id"]] = t["status"]
                    print("  %-16s %-10s %s" % (t["id"], t["status"], t["title"]))
            if proj["status"] in ("done", "failed", "review", "cancelled"):
                print("项目状态：%s" % proj["status"])
                if proj["status"] == "review":
                    print("有待裁决的冲突，打开看板处理。")
                return 0 if proj["status"] == "done" else 2
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("停止观察（项目仍在后台跑）")
        return 0
    return 3


def cmd_status(args):
    client = _client(args)
    state = client.get_json("/api/state")
    if args.json:
        print(json.dumps(state, ensure_ascii=False, indent=2))
        return 0
    print("hub: %s  v%s  运行 %.0fs" % (state["hub"]["name"], state["hub"]["version"], state["hub"]["uptime"]))
    nodes = state["nodes"]
    if not nodes:
        print("节点：无（在其它 Linux 上运行 ./swarm node --discover）")
    for n in nodes:
        print("  节点 %-14s %-18s %s  槽 %d  在跑 %d  完成 %d" % (
            n["name"], n["addr"], "在线" if n["online"] else "失联",
            n["slots"], len(n["running"]), n["done"]))
    for p in state["projects"]:
        print("\n项目 %s「%s」状态 %s  %d 个任务" % (
            p["id"], p["title"], p["status"], len(p["tasks"])))
        for t in p["tasks"]:
            marks = {"done": "✔", "running": "▶", "queued": "·", "failed": "✘",
                     "conflict": "!", "cancelled": "-"}.get(t["status"], "?")
            extra = ""
            if t["conflicts"]:
                extra = "  冲突 %d" % len(t["conflicts"])
            print("  %s %-16s %-9s %s%s" % (marks, t["id"], t["status"], t["title"], extra))
    return 0


def cmd_control(args):
    client = _client(args)
    payload = {"project": args.project, "action": args.action, "task": args.task}
    print(client.call("POST", "/api/control", payload))
    return 0


def cmd_logs(args):
    client = _client(args)
    res = client.get_json("/api/tasklog?project=%s&task=%s" % (proto.quote(args.project), proto.quote(args.task)))
    print("任务 %s「%s」状态 %s" % (res["id"], res["title"], res["status"]))
    for c in res.get("changes") or []:
        print("  改动 %s%s" % (c["path"], "（删除）" if c.get("deleted") else ""))
    for c in res.get("conflicts") or []:
        print("  ⚠ %s：%s" % (c.get("path"), c.get("reason")))
    print("--- 日志 ---")
    for line in res.get("log") or []:
        print("  " + line)
    return 0


def cmd_plan(args):
    spec = _read_spec(args.file)
    obj = planner.plan(spec, title=args.title, max_tasks=args.max_tasks)
    obj = planner.critical_path_order(planner.validate(obj))
    text = json.dumps(obj, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        print("已写入 %s（%d 个任务）" % (args.out, len(obj["tasks"])))
    else:
        print(text)
    return 0


def cmd_token(args):
    path = os.path.join(os.path.abspath(args.root), "token")
    if not os.path.exists(path):
        raise SwarmError("这个目录不是 hub：%s" % path)
    with open(path, "r", encoding="utf-8") as f:
        print(f.read().strip())
    return 0


def cmd_export(args):
    root = os.path.abspath(args.root)
    tree = os.path.join(root, "projects", args.project, "tree")
    if not os.path.isdir(tree):
        raise SwarmError("没有这个项目：%s" % args.project)
    out = args.out or ("%s.tar.gz" % args.project)
    with tarfile.open(out, "w:gz") as tar:
        tar.add(tree, arcname=os.path.basename(tree))
    print("已导出 %s（%.1f KB）" % (out, os.path.getsize(out) / 1024.0))
    return 0


def cmd_demo(args):
    from .demo import run_demo
    return run_demo(nodes=args.nodes, keep=args.keep, root=args.root)


# --------------------------------------------------------------------------
def build_parser():
    p = argparse.ArgumentParser(
        prog="swarm",
        description="局域网多 agent 协同调度：把大项目拆开，交给不同机器上的 AI 一起干。",
    )
    p.add_argument("--version", action="version", version="swarm " + __version__)
    sub = p.add_subparsers(dest="command")

    # hub
    h = sub.add_parser("hub", help="启动调度中心")
    h.add_argument("--root", default=os.path.join(os.path.expanduser("~"), ".swarm"), help="数据目录")
    h.add_argument("--name", default=None)
    h.add_argument("--port", type=int, default=DEFAULT_PORT)
    h.add_argument("--bind", default="0.0.0.0")
    h.add_argument("--discovery-port", type=int, default=DEFAULT_DISCOVERY_PORT)
    h.add_argument("--lease", type=int, default=180, help="任务租约秒数")
    h.add_argument("--stale", type=int, default=25, help="节点多久没心跳算掉线")
    h.add_argument("--max-attempts", type=int, default=2)
    h.add_argument("--token", default=None, help="手动指定令牌（默认自动生成保存在 root/token）")
    h.add_argument("--no-git", action="store_true", help="不在项目目录里做 git 提交")
    h.add_argument("--quiet", action="store_true")
    h.set_defaults(func=cmd_hub)

    # node
    n = sub.add_parser("node", help="启动工作节点（在每台 Linux 上跑）")
    n.add_argument("--root", default=os.path.join(os.path.expanduser("~"), ".swarm-node"))
    n.add_argument("--hub", default=None, help="hub 地址 ip:port")
    n.add_argument("--discover", action="store_true", help="UDP 自动发现 hub")
    n.add_argument("--token", default=None)
    n.add_argument("--name", default=None)
    n.add_argument("--slots", type=int, default=2, help="并发任务数")
    n.add_argument("--agent", default="mock", choices=["mock", "cli", "http"])
    n.add_argument("--cmd", default=None, help="cli 模式下执行的命令模板，支持 {prompt} {files} {workdir}")
    n.add_argument("--llm-base", default=None, help="http 模式：OpenAI 兼容地址")
    n.add_argument("--model", default=None)
    n.add_argument("--api-key", default=None)
    n.add_argument("--mock-delay", type=float, default=0.4)
    n.add_argument("--discovery-port", type=int, default=DEFAULT_DISCOVERY_PORT)
    n.add_argument("--keep-work", action="store_true", help="保留任务工作目录，便于排查")
    n.set_defaults(func=cmd_node)

    # submit
    s = sub.add_parser("submit", help="提交项目（自动拆成任务图）")
    s.add_argument("-f", "--file", default="-", help="需求文件，- 表示标准输入")
    s.add_argument("--title", default=None)
    s.add_argument("--plan", default=None, help="直接用一份 plan.json")
    s.add_argument("--hub", default=None)
    s.add_argument("--token", default=None)
    s.add_argument("--root", default=None, help="hub 的数据目录（可从中读令牌）")
    s.add_argument("--watch", action="store_true", help="盯着进度直到结束")
    s.set_defaults(func=cmd_submit)

    # status
    st = sub.add_parser("status", help="查看节点和任务进度")
    st.add_argument("--hub", default=None)
    st.add_argument("--token", default=None)
    st.add_argument("--root", default=None)
    st.add_argument("--json", action="store_true")
    st.set_defaults(func=cmd_status)

    # control
    c = sub.add_parser("control", help="暂停 / 继续 / 重试 / 取消")
    c.add_argument("project")
    c.add_argument("action", choices=["pause", "resume", "retry", "cancel", "finish"])
    c.add_argument("--task", default=None)
    c.add_argument("--hub", default=None)
    c.add_argument("--token", default=None)
    c.add_argument("--root", default=None)
    c.set_defaults(func=cmd_control)

    # logs
    lg = sub.add_parser("logs", help="看某个任务的日志")
    lg.add_argument("project")
    lg.add_argument("task")
    lg.add_argument("--hub", default=None)
    lg.add_argument("--token", default=None)
    lg.add_argument("--root", default=None)
    lg.set_defaults(func=cmd_logs)

    # plan
    pl = sub.add_parser("plan", help="只做拆解，不派发（离线看任务图）")
    pl.add_argument("-f", "--file", default="-")
    pl.add_argument("--title", default=None)
    pl.add_argument("--max-tasks", type=int, default=8)
    pl.add_argument("-o", "--out", default=None)
    pl.set_defaults(func=cmd_plan)

    # token
    tk = sub.add_parser("token", help="打印 hub 的令牌")
    tk.add_argument("--root", default=os.path.join(os.path.expanduser("~"), ".swarm"))
    tk.set_defaults(func=cmd_token)

    # export
    ex = sub.add_parser("export", help="把合并后的项目打包")
    ex.add_argument("project")
    ex.add_argument("--root", default=os.path.join(os.path.expanduser("~"), ".swarm"))
    ex.add_argument("-o", "--out", default=None)
    ex.set_defaults(func=cmd_export)

    # demo
    d = sub.add_parser("demo", help="本机起 hub + 3 个模拟节点，跑一遍完整流程")
    d.add_argument("--nodes", type=int, default=3)
    d.add_argument("--root", default=None)
    d.add_argument("--keep", action="store_true")
    d.set_defaults(func=cmd_demo)
    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    try:
        return args.func(args) or 0
    except SwarmError as exc:
        sys.stderr.write("错误：%s\n" % exc)
        return 1
    except KeyboardInterrupt:
        return 130

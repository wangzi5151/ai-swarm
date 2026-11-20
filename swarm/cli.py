"""命令行入口。

    ./bin/swarm hub                          # 在"调度机"上
    ./bin/swarm node --discover              # 在每台干活的 Linux 上
    ./bin/swarm submit -f spec.md --watch    # 交一个项目
    ./bin/swarm import ./已有仓库 --title X  # 从已有项目开始
    ./bin/swarm status                       # 看进度
    ./bin/swarm tokens add 手机B             # 给每个节点单独发令牌
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import sys
import tarfile
import time

from . import DEFAULT_DISCOVERY_PORT, DEFAULT_PORT, __version__
from . import planner, proto, workspace
from .proto import HubClient, SwarmError

TERMINAL = ("done", "failed", "review", "cancelled")


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
        return sys.stdin.read()
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_plan(path):
    if not path:
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _local_hub(root, **kw):
    from .hub import Hub
    return Hub(root=root, verbose=False, **kw)


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
        allow_empty=args.allow_empty, session_ttl=args.session_ttl,
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
        keep_work=args.keep_work, verify=args.verify,
    )
    return node.run()


# --------------------------------------------------------------------------
# submit / status / control
# --------------------------------------------------------------------------

def cmd_submit(args):
    spec = _read_spec(args.file)
    payload = {"title": args.title or "", "spec": spec,
               "request_id": os.urandom(8).hex()}
    plan = _read_plan(args.plan)
    if plan is not None:
        payload["plan"] = plan
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
                    extra = ""
                    if t["status"] == "blocked" and t.get("blocked_by"):
                        extra = "（上游 %s 失败）" % t["blocked_by"]
                    elif t["status"] == "conflict":
                        extra = "（%d 个文件待裁决）" % len(t["conflicts"])
                    print("  %-16s %-10s %s%s" % (t["id"], t["status"], t["title"], extra))
            if proj["status"] in TERMINAL:
                print("项目状态：%s" % proj["status"])
                if proj["status"] == "review":
                    print("有待裁决的冲突：./bin/swarm diff %s <任务> <文件> 看差异，看板点裁决。" % pid)
                elif proj["status"] == "failed":
                    print("有失败/阻塞的任务：./bin/swarm logs %s <任务> 看原因。" % pid)
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
        print("节点：无（在其它 Linux 上运行 ./bin/swarm node --discover）")
    for n in nodes:
        print("  节点 %-14s %-18s %s  槽 %d  在跑 %d  完成 %d  失败 %d" % (
            n["name"], n["addr"], "在线" if n["online"] else "失联",
            n["slots"], len(n["running"]), n["done"], n["failed"]))
    for p in state["projects"]:
        print("\n项目 %s「%s」状态 %s  %d 个任务" % (
            p["id"], p["title"], p["status"], len(p["tasks"])))
        for t in p["tasks"]:
            marks = {"done": "✔", "running": "▶", "preparing": "…", "queued": "·",
                     "failed": "✘", "blocked": "⏸", "conflict": "!",
                     "cancelled": "-"}.get(t["status"], "?")
            extra = ""
            if t["conflicts"]:
                extra += "  冲突 %d" % len(t["conflicts"])
            if t.get("blocked_by"):
                extra += "  上游 %s" % t["blocked_by"]
            if t.get("verify"):
                extra += "  验收%s" % ("通过" if t["verify"].get("ok") else "失败")
            print("  %s %-16s %-10s %s%s" % (marks, t["id"], t["status"], t["title"], extra))
    print("\n统计：%s" % json.dumps(state.get("stats") or {}, ensure_ascii=False))
    return 0


def cmd_control(args):
    client = _client(args)
    payload = {"project": args.project, "action": args.action, "task": args.task}
    print(client.call("POST", "/api/control", payload))
    return 0


def cmd_logs(args):
    client = _client(args)
    res = client.get_json("/api/tasklog?project=%s&task=%s" %
                          (proto.quote(args.project), proto.quote(args.task)))
    print("任务 %s「%s」状态 %s" % (res["id"], res["title"], res["status"]))
    for c in res.get("changes") or []:
        print("  改动 %s%s" % (c["path"], "（删除）" if c.get("deleted") else ""))
    for c in res.get("conflicts") or []:
        print("  ⚠ %s：%s" % (c.get("path"), c.get("reason")))
    if res.get("verify"):
        v = res["verify"]
        print("  验收：%s（%s）" % ("通过" if v.get("ok") else "失败", v.get("cmd")))
    print("--- 日志 ---")
    for line in res.get("log") or []:
        print("  " + line)
    return 0


def cmd_diff(args):
    client = _client(args)
    res = client.get_json("/api/diff?project=%s&task=%s&path=%s" % (
        proto.quote(args.project), proto.quote(args.task), proto.quote(args.path)))
    if res.get("binary"):
        print(res.get("diff") or "（二进制文件）")
    else:
        print(res.get("diff") or "（没有差异）")
    return 0


def cmd_rollback(args):
    client = _client(args)
    payload = {"project": args.project, "task": args.task, "force": args.force}
    res = client.call("POST", "/api/rollback", payload)
    print("已回滚 %d 个文件：%s" % (len(res.get("restored") or []), ", ".join(res.get("restored") or [])))
    return 0


# --------------------------------------------------------------------------
# 导入已有仓库 / 令牌 / GC
# --------------------------------------------------------------------------

def _build_archive(dirpath):
    root = os.path.abspath(dirpath)
    if not os.path.isdir(root):
        raise SwarmError("不是目录：%s" % root)
    ignore = workspace.load_ignores(root)
    buf = io.BytesIO()
    count = 0
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for rel, full in workspace.iter_tree(root, ignore=ignore):
            tar.add(full, arcname=rel, recursive=False)
            count += 1
    data = buf.getvalue()
    return base64.b64encode(data).decode("ascii"), count, len(data)


def cmd_import(args):
    spec = _read_spec(args.file) if args.file else ""
    plan = _read_plan(args.plan)
    title = args.title or os.path.basename(os.path.abspath(args.dir)) or "导入的项目"
    if args.hub:
        archive, count, raw = _build_archive(args.dir)
        if len(archive) > (proto.BODY_LIMIT - 4096):
            raise SwarmError("目录太大（压缩后 %.1f MB，上限约 %d MB）" %
                             (len(archive) / 1048576.0, proto.BODY_LIMIT >> 20))
        payload = {"title": title, "spec": spec, "plan": plan, "archive": archive,
                   "project_id": args.project_id, "request_id": os.urandom(8).hex()}
        client = _client(args)
        res = client.call("POST", "/api/import", payload, timeout=300)
        print("已导入 %d 个文件，项目 %s「%s」：%d 个任务。" %
              (count, res["project"], res["title"], res["tasks"]))
        if args.watch:
            return _watch(client, res["project"])
        return 0
    if not args.root:
        raise SwarmError("离线导入需要 --root <hub 数据目录>；或加 --hub 走运行中的 hub")
    hub = _local_hub(args.root)
    try:
        p = hub.create_project(title, spec, plan, project_id=args.project_id,
                               seed_dir=os.path.abspath(args.dir))
        print("已离线导入项目 %s「%s」：%d 个任务。重启 hub 后生效。" %
              (p["id"], p["title"], len(p["tasks"])))
    finally:
        hub.shutdown()
    return 0


def cmd_tokens(args):
    action = args.action or "list"
    if args.hub:
        client = _client(args)
        if action == "list":
            for t in client.get_json("/api/tokens")["tokens"]:
                print("  %-16s 节点 %-12s %-4s %s" % (
                    t["name"], t.get("node") or "-", "吊销" if t.get("revoked") else "有效",
                    t["hint"]))
            return 0
        if action == "add":
            payload = {"action": "create", "name": args.name, "node": args.node}
            res = client.call("POST", "/api/tokens", payload)
            print("已创建节点令牌 %s（只显示这一次）：\n%s" % (res["name"], res["token"]))
            if res.get("node"):
                print("绑定节点：%s" % res["node"])
            return 0
        if action == "revoke":
            res = client.call("POST", "/api/tokens", {"action": "revoke", "key": args.key or args.name})
            print("已吊销：%s" % res["name"])
            return 0
        raise SwarmError("未知动作：%s" % action)

    if not args.root:
        raise SwarmError("令牌操作需要 --hub（运行中的 hub）或 --root（离线）")
    hub = _local_hub(args.root)
    try:
        if action == "list":
            for t in hub.token_view():
                print("  %-16s 节点 %-12s %-4s %s" % (
                    t["name"], t.get("node") or "-", "吊销" if t.get("revoked") else "有效",
                    t["hint"]))
        elif action == "add":
            entry = hub.create_node_token(args.name, args.node)
            print("已创建节点令牌 %s（只显示这一次）：\n%s" % (entry["name"], entry["token"]))
            print("注意：这是离线操作，运行中的 hub 重启后才会加载新令牌。")
        elif action == "revoke":
            print("已吊销：%s" % hub.revoke_token(args.key or args.name))
            print("注意：这是离线操作，运行中的 hub 重启后才会生效。")
        else:
            raise SwarmError("未知动作：%s" % action)
    finally:
        hub.shutdown(save=False)
    return 0


def cmd_gc(args):
    if args.hub:
        client = _client(args)
        res = client.call("POST", "/api/gc", {"grace": args.grace}, timeout=300)
    else:
        sys.stderr.write("提示：离线 GC 前请先停掉 hub，避免和正在写入的 blob 打架。\n")
        hub = _local_hub(args.root)
        try:
            res = hub.gc(grace=args.grace)
        finally:
            hub.shutdown(save=False)
    print("回收 %d 个 blob，释放 %.1f MB；仍被引用 %d 个，剩余 %.1f MB。" % (
        res.get("removed", 0), res.get("freed", 0) / 1048576.0,
        res.get("referenced", 0), res.get("remaining", 0) / 1048576.0))
    return 0


# --------------------------------------------------------------------------
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
        for rel, full in workspace.iter_tree(tree):
            tar.add(full, arcname=rel, recursive=False)
    print("已导出 %s（%.1f KB）" % (out, os.path.getsize(out) / 1024.0))
    return 0


def cmd_demo(args):
    from .demo import run_demo
    return run_demo(nodes=args.nodes, keep=args.keep, root=args.root)


def _add_conn(sp, root_default=None):
    sp.add_argument("--hub", default=None)
    sp.add_argument("--token", default=None)
    if root_default is not None:
        sp.add_argument("--root", default=root_default)
    else:
        sp.add_argument("--root", default=None)


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
    h.add_argument("--lease", type=int, default=180, help="任务租约秒数（心跳会自动续）")
    h.add_argument("--stale", type=int, default=25, help="节点多久没心跳算掉线")
    h.add_argument("--max-attempts", type=int, default=2, help="同一任务最多派几次")
    h.add_argument("--session-ttl", type=int, default=12 * 3600, help="看板登录有效期（秒）")
    h.add_argument("--allow-empty", action="store_true", help="允许任务交空改动（默认不允许）")
    h.add_argument("--token", default=None, help="手动指定主令牌（默认自动生成保存在 root/token）")
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
    n.add_argument("--cmd", default=None, help="cli 模式的命令模板，支持 {prompt} {files} {workdir}")
    n.add_argument("--llm-base", default=None, help="http 模式：OpenAI 兼容地址")
    n.add_argument("--model", default=None)
    n.add_argument("--api-key", default=None)
    n.add_argument("--verify", default=None,
                   help="默认验收命令；任务 plan 里的 verify 优先，失败则任务不算完成")
    n.add_argument("--mock-delay", type=float, default=0.4)
    n.add_argument("--discovery-port", type=int, default=DEFAULT_DISCOVERY_PORT)
    n.add_argument("--keep-work", action="store_true", help="保留任务工作目录，便于排查")
    n.set_defaults(func=cmd_node)

    # submit
    s = sub.add_parser("submit", help="提交项目（自动拆成任务图）")
    s.add_argument("-f", "--file", default="-", help="需求文件，- 表示标准输入")
    s.add_argument("--title", default=None)
    s.add_argument("--plan", default=None, help="直接用一份 plan.json")
    _add_conn(s)
    s.add_argument("--watch", action="store_true", help="盯着进度直到结束")
    s.set_defaults(func=cmd_submit)

    # import
    im = sub.add_parser("import", help="把已有仓库导入成项目（尊重 .gitignore）")
    im.add_argument("dir", help="已有项目目录")
    im.add_argument("--title", default=None)
    im.add_argument("-f", "--file", default=None, help="需求文件（可选）")
    im.add_argument("--plan", default=None, help="任务计划 JSON（强烈建议给大型项目）")
    im.add_argument("--project-id", default=None)
    _add_conn(im, root_default=os.path.join(os.path.expanduser("~"), ".swarm"))
    im.add_argument("--watch", action="store_true")
    im.set_defaults(func=cmd_import)

    # status
    st = sub.add_parser("status", help="查看节点和任务进度")
    _add_conn(st)
    st.add_argument("--json", action="store_true")
    st.set_defaults(func=cmd_status)

    # control
    c = sub.add_parser("control", help="暂停 / 继续 / 重试 / 取消")
    c.add_argument("project")
    c.add_argument("action", choices=["pause", "resume", "retry", "cancel", "finish"])
    c.add_argument("--task", default=None)
    _add_conn(c)
    c.set_defaults(func=cmd_control)

    # logs
    lg = sub.add_parser("logs", help="看某个任务的日志")
    lg.add_argument("project")
    lg.add_argument("task")
    _add_conn(lg)
    lg.set_defaults(func=cmd_logs)

    # diff
    df = sub.add_parser("diff", help="看冲突文件在'主工程 / 节点'之间的差异")
    df.add_argument("project")
    df.add_argument("task")
    df.add_argument("path")
    _add_conn(df)
    df.set_defaults(func=cmd_diff)

    # rollback
    rb = sub.add_parser("rollback", help="撤销某次合并（恢复合并前的文件）")
    rb.add_argument("project")
    rb.add_argument("--task", default=None, help="不填则回滚最近一次合并")
    rb.add_argument("--force", action="store_true", help="即使文件之后又被改过也强制回滚")
    _add_conn(rb)
    rb.set_defaults(func=cmd_rollback)

    # tokens
    tk = sub.add_parser("tokens", help="节点令牌：list / add / revoke")
    tk.add_argument("action", nargs="?", default="list", choices=["list", "add", "revoke"])
    tk.add_argument("name", nargs="?", default=None, help="add 时的令牌名")
    tk.add_argument("--node", default=None, help="add 时绑定节点 id")
    tk.add_argument("--key", default=None, help="revoke 时的名字/令牌值/前 8 位")
    _add_conn(tk, root_default=os.path.join(os.path.expanduser("~"), ".swarm"))
    tk.set_defaults(func=cmd_tokens)

    # token（主令牌）
    t1 = sub.add_parser("token", help="打印 hub 的主令牌")
    t1.add_argument("--root", default=os.path.join(os.path.expanduser("~"), ".swarm"))
    t1.set_defaults(func=cmd_token)

    # gc
    gc = sub.add_parser("gc", help="清理没有被引用的 blob")
    gc.add_argument("--grace", type=float, default=600, help="多少秒内的新文件不动")
    _add_conn(gc, root_default=os.path.join(os.path.expanduser("~"), ".swarm"))
    gc.set_defaults(func=cmd_gc)

    # plan
    pl = sub.add_parser("plan", help="只做拆解，不派发（离线看任务图）")
    pl.add_argument("-f", "--file", default="-")
    pl.add_argument("--title", default=None)
    pl.add_argument("--max-tasks", type=int, default=8)
    pl.add_argument("-o", "--out", default=None)
    pl.set_defaults(func=cmd_plan)

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
    except (SwarmError, ValueError) as exc:
        sys.stderr.write("错误：%s\n" % exc)
        return 1
    except KeyboardInterrupt:
        return 130

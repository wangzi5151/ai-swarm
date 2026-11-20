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
import shutil
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
    scheme, host, port = proto.split_hub_addr(hub, DEFAULT_PORT)
    tls = (scheme == "https" or bool(getattr(args, "tls", False))
           or bool(os.environ.get("SWARM_TLS")))
    pin = getattr(args, "tls_pin", None) or os.environ.get("SWARM_TLS_PIN")
    ca = getattr(args, "ca", None) or os.environ.get("SWARM_CA")
    insecure = bool(getattr(args, "insecure", False)) or bool(
        os.environ.get("SWARM_TLS_INSECURE"))
    if not token:
        root = getattr(args, "root", None)
        if root:
            path = os.path.join(os.path.abspath(root), "token")
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    token = f.read().strip()
    if need_token and not token:
        raise SwarmError("没指定令牌：用 --token 或设 SWARM_TOKEN（hub 启动时会打印）")
    if tls and not pin and not ca and not insecure:
        sys.stderr.write(
            "⚠ 已启用 TLS 但没有 --tls-pin / --ca：只加密、不校验身份。\n"
            "  从 hub 启动日志抄证书指纹加 --tls-pin，或用 --insecure 显式确认。\n")
    return HubClient(host, port, token or "", timeout=30,
                     scheme="https" if tls else "http",
                     tls_pin=pin, ca=ca, insecure=insecure)


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
        auto_merge=args.auto_merge, tls=args.tls,
        tls_cert=args.tls_cert, tls_key=args.tls_key,
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
        tls=args.tls, tls_pin=args.tls_pin, ca=args.ca, insecure=args.insecure,
        once=args.once,
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


def _print_status(state):
    print("hub: %s  v%s  运行 %.0fs" % (state["hub"]["name"], state["hub"]["version"],
                                       state["hub"]["uptime"]))
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


def cmd_status(args):
    client = _client(args)
    state = client.get_json("/api/state")
    if args.json:
        print(json.dumps(state, ensure_ascii=False, indent=2))
        return 0
    if not args.watch:
        _print_status(state)
        return 0
    try:
        while True:
            # 清屏再画，手机终端上更像一个实时面板
            sys.stdout.write("\033[2J\033[H")
            _print_status(client.get_json("/api/state"))
            sys.stdout.flush()
            time.sleep(max(1, args.interval))
    except KeyboardInterrupt:
        print("\n停止观察（项目仍在后台跑）")
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
        if not v.get("ok") and v.get("out"):
            print("  验收输出（尾部）：")
            for line in str(v["out"]).splitlines()[-10:]:
                print("    " + line)
    if res.get("feedback"):
        print("  上次失败反馈（重试时会带给 agent）：")
        for line in str(res["feedback"]).splitlines()[:10]:
            print("    " + line)
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
    out = args.out or ("%s.tar.gz" % args.project)
    if args.hub:
        client = _client(args)
        status, data = client.raw_full(
            "GET", "/api/export?project=%s" % proto.quote(args.project), timeout=900)[:2]
        if status >= 400:
            raise SwarmError("导出失败 HTTP %d" % status)
        with open(out, "wb") as f:
            f.write(data)
        try:
            os.chmod(out, 0o600)     # 备份里有令牌
        except OSError:
            pass
    else:
        root = os.path.abspath(args.root)
        tree = os.path.join(root, "projects", args.project, "tree")
        if not os.path.isdir(tree):
            raise SwarmError("没有这个项目：%s" % args.project)
        with tarfile.open(out, "w:gz") as tar:
            for rel, full in workspace.iter_tree(tree):
                tar.add(full, arcname=rel, recursive=False)
    print("已导出 %s（%.1f KB）" % (out, os.path.getsize(out) / 1024.0))
    return 0


def cmd_demo(args):
    from .demo import run_demo
    return run_demo(nodes=args.nodes, keep=args.keep, root=args.root)


def _add_conn(sp, root_default=None, tls=True):
    sp.add_argument("--hub", default=None)
    sp.add_argument("--token", default=None)
    if root_default is not None:
        sp.add_argument("--root", default=root_default)
    else:
        sp.add_argument("--root", default=None)
    if tls:
        sp.add_argument("--tls", action="store_true", help="用 HTTPS 连接 hub（--hub https:// 亦可）")
        sp.add_argument("--tls-pin", default=None, help="hub 证书指纹（sha256，hub 启动时打印）")
        sp.add_argument("--ca", default=None, help="自签 CA 文件（启用主机名校验）")
        sp.add_argument("--insecure", action="store_true", help="TLS 只加密、不校验身份")


# --------------------------------------------------------------------------
# 备份 / 恢复 / 删除 / 自检
# --------------------------------------------------------------------------

def cmd_backup(args):
    out = args.out or ("swarm-backup-%s.tar.gz" % time.strftime("%Y%m%d-%H%M%S"))
    include_blobs = not args.no_blobs
    if args.hub:
        client = _client(args)
        status, data = client.raw_full(
            "GET", "/api/backup?blobs=%d" % (1 if include_blobs else 0), timeout=1800)[:2]
        if status >= 400:
            raise SwarmError("备份失败 HTTP %d" % status)
        with open(out, "wb") as f:
            f.write(data)
        try:
            os.chmod(out, 0o600)     # 备份里有令牌
        except OSError:
            pass
    else:
        if not args.root:
            raise SwarmError("离线备份需要 --root <hub 数据目录>；或加 --hub 走运行中的 hub")
        hub = _local_hub(args.root)
        try:
            hub.backup_archive(out, include_blobs=include_blobs)
        finally:
            hub.shutdown(save=False)
    print("已备份到 %s（%.1f MB）" % (out, os.path.getsize(out) / 1048576.0))
    if not include_blobs:
        print("注意：--no-blobs 只备份状态和当前项目树，历史版本的回滚/冲突裁决会不可用。")
    print("换机器：./bin/swarm restore %s --root ~/.swarm 然后启动 hub" % out)
    return 0


def _safe_extract(archive, dest):
    dest = os.path.abspath(dest)
    os.makedirs(dest, exist_ok=True)
    count = 0
    total = 0
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar:
            if not (member.isfile() or member.isdir()):
                continue
            name = member.name.replace("\\", "/")
            while name.startswith("./"):
                name = name[2:]
            safe = workspace._safe_rel(name)
            if safe is None:
                continue
            target = os.path.join(dest, safe)
            if not os.path.abspath(target).startswith(dest + os.sep):
                continue
            total += max(0, member.size)
            if total > (2 << 30):
                raise SwarmError("备份解压后超过 2GB，拒绝恢复")
            if member.isdir():
                os.makedirs(target, exist_ok=True)
                continue
            parent = os.path.dirname(target)
            if parent:
                os.makedirs(parent, exist_ok=True)
            src = tar.extractfile(member)
            if src is None:
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
            count += 1
    return count


def cmd_restore(args):
    root = os.path.abspath(args.root)
    if not os.path.isfile(args.archive):
        raise SwarmError("备份文件不存在：%s" % args.archive)
    marker = os.path.join(root, "state.json")
    if os.path.exists(marker) and not args.force:
        raise SwarmError(
            "%s 里已经有 state.json。确认要覆盖就加 --force（先停掉正在跑的 hub）" % root)
    count = _safe_extract(args.archive, root)
    print("已恢复到 %s：%d 个文件。" % (root, count))
    print("确认没有旧 hub 在跑，然后：./bin/swarm hub --root %s" % root)
    return 0


def cmd_delete(args):
    if args.hub:
        client = _client(args)
        res = client.call("POST", "/api/delete", {"project": args.project})
    else:
        if not args.root:
            raise SwarmError("离线删除需要 --root；或加 --hub 走运行中的 hub")
        hub = _local_hub(args.root)
        try:
            res = hub.delete_project(args.project)
        finally:
            hub.shutdown()      # 删除要落盘，否则重启后项目又回来了
    print("已删除项目 %s（blob 等 gc 再清）" % res.get("project", args.project))
    return 0


def cmd_doctor(args):
    ok = True
    print("Swarm 自检")
    print("  Python      %s" % sys.version.split()[0])
    if sys.version_info < (3, 8):
        print("  ✘ Python 需要 3.8+")
        ok = False
    openssl = shutil.which("openssl")
    print("  openssl     %s" % ("有（可以用 --tls 自动生成证书）" if openssl else "没有（TLS 需自带证书）"))
    print("  git         %s" % ("有" if shutil.which("git") else "没有（hub 用 --no-git）"))
    hub = getattr(args, "hub", None) or os.environ.get("SWARM_HUB")
    root = getattr(args, "root", None)
    if not hub and root:
        path = os.path.join(os.path.abspath(root), "token")
        if os.path.exists(path):
            print("  主令牌      %s…（%s）" % (open(path).read().strip()[:8], root))
            try:
                state_path = os.path.join(os.path.abspath(root), "state.json")
                if os.path.exists(state_path):
                    with open(state_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    print("  离线状态    %d 个项目" % len(data.get("projects") or {}))
                else:
                    print("  离线状态    （还没有项目）")
            except Exception as exc:
                print("  ✘ 状态文件读不了：%s" % exc)
                ok = False
        else:
            print("  ✘ %s 不像一个 hub 目录（没有 token）" % root)
            ok = False
        return 0 if ok else 1
    if not hub:
        print("\n提示：加 --hub <ip:port> 或在 hub 机器上加 --root 检查连接")
        return 0
    try:
        client = _client(args, need_token=False)
        ping = client.ping(timeout=5)
        skew = time.time() - float(ping.get("server_time") or time.time())
        print("  hub 连接    %s v%s（协议 v%s）" % (ping.get("hub"), ping.get("version"),
                                                   ping.get("proto")))
        print("  时钟偏差    %+.1f 秒%s" % (skew, "  ✘ 超过 180 秒会鉴权失败"
                                          if abs(skew) > 180 else ""))
        if abs(skew) > 180:
            ok = False
        token = getattr(args, "token", None) or os.environ.get("SWARM_TOKEN")
        if not token and root:
            path = os.path.join(os.path.abspath(root), "token")
            if os.path.exists(path):
                token = open(path).read().strip()
                client.token = token
        if token:
            try:
                state = client.get_json("/api/state")
                print("  权限        主令牌（%d 节点 / %d 项目）" %
                      (len(state.get("nodes") or []), len(state.get("projects") or [])))
                print("  TLS         %s" % ("已启用" if (state.get("hub") or {}).get("tls") else "未启用"))
            except Exception as exc:
                print("  权限        节点令牌或受限（%s）" % exc)
        else:
            print("  权限        未提供令牌（只做了连通性检查）")
    except Exception as exc:
        print("  ✘ 连不上 hub：%s" % exc)
        ok = False
    return 0 if ok else 1


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
    h.add_argument("--auto-merge", action="store_true",
                   help="同一文件两边改不重叠区域时自动三方合并（默认交给人工裁决）")
    h.add_argument("--tls", action="store_true", help="启用 HTTPS（没证书会自动生成自签证书）")
    h.add_argument("--tls-cert", default=None, help="证书 PEM（配合 --tls）")
    h.add_argument("--tls-key", default=None, help="私钥 PEM（配合 --tls）")
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
    n.add_argument("--tls", action="store_true", help="用 HTTPS 连接 hub（--discover 会自动启用）")
    n.add_argument("--tls-pin", default=None, help="hub 证书指纹（默认用自动发现带回来的）")
    n.add_argument("--ca", default=None, help="自签 CA 文件")
    n.add_argument("--insecure", action="store_true", help="TLS 只加密、不校验身份")
    n.add_argument("--once", action="store_true",
                   help="只领一个任务，干完退出（验证 agent 配置是否可用）")
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
    st.add_argument("--watch", action="store_true", help="每 N 秒刷新一次（Ctrl-C 退出）")
    st.add_argument("--interval", type=int, default=2)
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
    _add_conn(ex, root_default=os.path.join(os.path.expanduser("~"), ".swarm"))
    ex.add_argument("-o", "--out", default=None)
    ex.set_defaults(func=cmd_export)

    # backup / restore / delete / doctor
    bk = sub.add_parser("backup", help="备份 hub（状态 + 令牌 + 项目 + blob），用于迁移/主备")
    _add_conn(bk, root_default=os.path.join(os.path.expanduser("~"), ".swarm"))
    bk.add_argument("-o", "--out", default=None)
    bk.add_argument("--no-blobs", action="store_true", help="不备份 blob（体积小，但节点要重传）")
    bk.set_defaults(func=cmd_backup)

    rs = sub.add_parser("restore", help="把备份恢复到 hub 数据目录")
    rs.add_argument("archive")
    rs.add_argument("--root", default=os.path.join(os.path.expanduser("~"), ".swarm"))
    rs.add_argument("--force", action="store_true", help="覆盖已有的 state.json")
    rs.set_defaults(func=cmd_restore)

    dl = sub.add_parser("delete", help="删除一个项目（连同它的项目目录）")
    dl.add_argument("project")
    _add_conn(dl, root_default=os.path.join(os.path.expanduser("~"), ".swarm"))
    dl.set_defaults(func=cmd_delete)

    dr = sub.add_parser("doctor", help="自检：连接、时钟、令牌、TLS、依赖")
    _add_conn(dr, root_default=os.path.join(os.path.expanduser("~"), ".swarm"))
    dr.set_defaults(func=cmd_doctor)

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

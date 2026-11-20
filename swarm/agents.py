"""Agent 适配器：把任务交给本机已有的 AI 工具去干。

已经装在 Linux 里的东西都能接：
    mock  —— 不配任何东西，用来跑通链路 / 压测调度
    cli   —— 任何命令行 agent：aider / claude / codex / ollama / 自己写的脚本
    http  —— OpenAI 兼容接口（本地 llama.cpp、vLLM、LM Studio、Ollama、云 API 都行）

适配器只管"写文件"，改了什么由工作区比对，不依赖 agent 自己汇报。
另外提供 run_shell()，用来跑任务声明的验收命令（verify）。
"""

from __future__ import annotations

import json
import os
import queue
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

from . import workspace


class AgentError(Exception):
    pass


# --------------------------------------------------------------------------
# 子进程：带硬超时的流式执行
# --------------------------------------------------------------------------

def _kill_tree(proc):
    """杀掉 agent 及其子进程（aider 会再开子进程，只 kill 父进程没用）。"""
    try:
        if os.name == "posix":
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except OSError:
                proc.kill()
        else:
            proc.kill()
    except Exception:
        pass
    try:
        proc.wait(timeout=3)
    except Exception:
        pass


def _run_streaming(cmd, cwd, timeout, env=None, log=None, max_lines=4000):
    """跑一条命令，实时把输出交给 log；到点强制杀进程组。

    返回 {"rc": int|None, "lines": [...], "timed_out": bool}。
    关键点：**不等待"下一行输出"才检查超时** —— agent 一声不吭也能被砍掉。
    """
    try:
        proc = subprocess.Popen(
            cmd, shell=True, cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True, bufsize=1,
            start_new_session=(os.name == "posix"),
        )
    except OSError as exc:
        raise AgentError("启动命令失败：%s" % exc)
    if proc.stdout is None:
        raise AgentError("无法读取命令输出")

    # 有界队列：agent 疯狂刷日志时给 stdout 反压，避免节点内存被撑爆
    q = queue.Queue(maxsize=10000)
    closed = threading.Event()

    def reader():
        try:
            for line in iter(proc.stdout.readline, ""):
                q.put(line)
        except Exception:
            pass
        finally:
            try:
                proc.stdout.close()
            except Exception:
                pass
            closed.set()
            q.put(None)

    th = threading.Thread(target=reader, name="agent-reader", daemon=True)
    th.start()
    deadline = time.time() + max(1, int(timeout))
    lines = []
    timed_out = False
    while True:
        remain = deadline - time.time()
        if remain <= 0:
            timed_out = True
            break
        try:
            item = q.get(timeout=min(remain, 0.5))
        except queue.Empty:
            if closed.is_set() and proc.poll() is not None:
                break
            continue
        if item is None:
            break
        line = item.rstrip("\n").rstrip("\r")
        if line:
            lines.append(line)
            if len(lines) > max_lines:
                del lines[:len(lines) - max_lines]
            if log is not None:
                try:
                    log(line[:400])
                except Exception:
                    pass
    if timed_out:
        _kill_tree(proc)
        # 尽量收一下已经产生的输出，便于看卡在哪
        end = time.time() + 1.0
        while time.time() < end:
            try:
                item = q.get(timeout=0.05)
            except queue.Empty:
                break
            if item is None:
                break
            if item.strip():
                lines.append(item.rstrip("\n"))
    else:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            timed_out = True
            _kill_tree(proc)
    return {"rc": proc.returncode, "lines": lines, "timed_out": timed_out}


def run_shell(cmd, cwd, timeout=600, log=None):
    """跑验收命令。返回 (是否成功, 输出尾部)。"""
    res = _run_streaming(cmd, cwd, timeout, log=log)
    text = "\n".join(res["lines"][-40:])
    return (not res["timed_out"] and res["rc"] == 0), text


# --------------------------------------------------------------------------
# mock：确定性本地模拟
# --------------------------------------------------------------------------

def run_mock(ctx):
    """按任务范围生成文件，生成的每个文件都保证落在 write 范围里。"""
    log = ctx["log"]
    files = [str(f) for f in (ctx["files"] or ["**"])]
    slug = _slug(ctx["task_id"] + "-" + ctx.get("title", ""))
    log("模拟 agent 启动：范围 %s" % ", ".join(files))
    time.sleep(0.2)
    written = []

    existing = [p for p in _list_files(ctx["workdir"]) if _in_scope(p, files)]
    if existing:
        target = existing[0]
        path = os.path.join(ctx["workdir"], target)
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n%s\n" % _comment_for(target, "%s 由 %s 完成" % (ctx["title"], ctx["node"])))
        written.append(target)
        log("改写了 %s" % target)
    else:
        target = None
        ext = ".py"
        for pat in files:
            target = _target_for(pat, slug, ext)
            if target and _in_scope(target, files):
                break
            target = None
        if target is None:
            log("⚠ 本任务没有可写的文件（范围 %s），将产生空交付" % ", ".join(files))
        else:
            path = os.path.join(ctx["workdir"], target)
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(_stub_py(slug, ctx["title"], ctx["prompt"]))
            written.append(target)
            log("新建了 %s" % target)

    # 再补一个范围里的说明文件（只挑带通配符的范围，避免覆盖固定文件）
    for pat in files:
        if not any(ch in pat for ch in "*?[{"):
            continue
        note = _target_for(pat, slug + "-notes", ".md")
        if not note or not _in_scope(note, files) or note in written:
            continue
        path = os.path.join(ctx["workdir"], note)
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write("# %s\n\n任务：%s\n\n%s\n" % (ctx["title"], ctx["task_id"], ctx["prompt"]))
        written.append(note)
        break

    time.sleep(float(ctx.get("mock_delay", 0.4)))
    log("交付 %d 个文件" % len(written))
    return written


def _stub_py(slug, title, prompt):
    cls = _ident(slug)
    doc = (prompt or "")[:400].replace(chr(34) * 3, chr(39) * 3)
    body = [
        chr(34) * 3 + title,
        "",
        doc,
        chr(34) * 3,
        "",
        "from dataclasses import dataclass",
        "",
        "",
        "@dataclass",
        "class " + cls + ":",
        "    name: str = " + repr(title),
        "    ready: bool = True",
        "",
        "    def summary(self):",
        '        return "' + cls + '(%s)" % self.name',
        "",
    ]
    return "\n".join(body)


def _ident(name):
    if not name or not (name[0].isalpha() or name[0] == "_"):
        return "Task" + name
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name).capitalize()


def _comment_for(path, text):
    ext = os.path.splitext(path)[1].lower()
    if ext in (".py", ".sh", ".rb", ".yml", ".yaml", ".toml"):
        return "# " + text
    if ext in (".js", ".ts", ".java", ".kt", ".c", ".h", ".cpp", ".go", ".rs", ".css"):
        return "// " + text
    if ext in (".html", ".xml", ".md"):
        return "<!-- %s -->" % text
    return text


def _slug(text):
    out = re.sub(r"[^0-9a-zA-Z]+", "-", text).strip("-").lower()
    return (out or "task")[:48]


def _target_for(pattern, stem, suffix=".py"):
    """从一个 glob 推一个真正匹配它的文件路径。推不出来返回 None。"""
    p = str(pattern or "").strip().replace("\\", "/")
    while p.startswith("./"):
        p = p[2:]
    if not p or p == ".":
        return None
    if p.endswith("/"):
        p += "**"
    parts = p.split("/")
    out = []
    for i, seg in enumerate(parts):
        wild = any(ch in seg for ch in "*?[{")
        last = (i == len(parts) - 1)
        if not wild:
            if last:
                if "." in seg:
                    return "/".join(out + [seg])       # 明确写到某个文件
                out.append(seg)
            else:
                out.append(seg)
            continue
        if not last:
            out.append(re.sub(r"\*+", "mod", seg).replace("?", "x").replace("[", "").replace("]", ""))
            continue
        name = seg.replace("**", stem).replace("*", stem).replace("?", "x")
        name = re.sub(r"\[[^\]]*\]", "", name)
        name = name.replace("{", "").replace("}", "")
        if not name:
            name = stem
        if not os.path.splitext(name)[1]:
            name += suffix
        out.append(name)
        return "/".join(out)
    return "/".join(out + [stem + suffix])


def _list_files(root):
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in (".git", "__pycache__", "node_modules")]
        for name in filenames:
            out.append(os.path.relpath(os.path.join(dirpath, name), root).replace(os.sep, "/"))
    return out


def _in_scope(path, files):
    return workspace.match_any(path, files) if files else True


# --------------------------------------------------------------------------
# cli：把 prompt 交给本机任意 agent 命令
# --------------------------------------------------------------------------

def run_cli(ctx):
    template = ctx.get("cmd") or os.environ.get("SWARM_AGENT_CMD")
    if not template:
        raise AgentError("没有配置 agent 命令（--cmd 或 SWARM_AGENT_CMD）")
    log = ctx["log"]
    file_list = ctx.get("file_list") or []
    values = {
        "prompt": ctx["prompt"],
        "files": " ".join(shlex.quote(f) for f in file_list),
        "file_list": "\n".join(file_list),
        "workdir": ctx["workdir"],
        "task_id": ctx["task_id"],
        "title": ctx.get("title", ""),
        "model": ctx.get("model") or "",
        "feedback": ctx.get("feedback") or "",
    }
    cmd = template
    for key, val in values.items():
        cmd = cmd.replace("{" + key + "}", str(val))

    env = dict(os.environ)
    env.update({
        "SWARM_PROMPT": values["prompt"],
        "SWARM_FILES": values["files"],
        "SWARM_TASK_ID": values["task_id"],
        "SWARM_TASK_TITLE": values["title"],
        "SWARM_WORKDIR": ctx["workdir"],
        "SWARM_TASK_FILE": os.path.join(ctx["workdir"], ".swarm-task.md"),
        "SWARM_FEEDBACK": values["feedback"],
    })
    with open(env["SWARM_TASK_FILE"], "w", encoding="utf-8") as f:
        f.write("# %s\n\n%s\n\n## 允许改动的文件\n\n%s\n" %
                (values["title"], values["prompt"], values["file_list"] or "（未限定）"))
        if values["feedback"]:
            f.write("\n## 上次尝试失败的原因\n\n%s\n" % values["feedback"])
    if values["feedback"]:
        with open(os.path.join(ctx["workdir"], ".swarm-feedback.md"), "w",
                  encoding="utf-8") as f:
            f.write("# 上次尝试失败的原因\n\n%s\n" % values["feedback"])

    log("执行：%s" % cmd)
    timeout = int(ctx.get("timeout") or 1800)
    res = _run_streaming(cmd, ctx["workdir"], timeout, env=env, log=log)
    if res["timed_out"]:
        raise AgentError("agent 超时（%ds），已强制结束进程组" % timeout)
    if res["rc"] != 0:
        tail = "\n".join(res["lines"][-8:])
        raise AgentError("agent 退出码 %s\n%s" % (res["rc"], tail))
    return res["lines"]


# --------------------------------------------------------------------------
# http：OpenAI 兼容接口
# --------------------------------------------------------------------------

HTTP_PROMPT = """你是一个在协作网络里工作的编码 agent。只输出 JSON，不要解释。

任务：{title}
说明：{prompt}

工作目录里与你相关的文件：
{files}

要求：
1. 只改与任务相关的文件，不要动其它文件。
2. 输出格式（严格）：
{{"files": {{"相对路径": "完整文件内容", ...}}, "delete": ["要删除的相对路径"]}}
3. 路径必须落在这些范围内：{scope}
"""


def _norm_base(base):
    base = (base or "").strip().rstrip("/")
    if base and "://" not in base:
        base = "http://" + base
    return base


def run_http(ctx):
    base = _norm_base(ctx.get("base_url") or os.environ.get("SWARM_LLM_BASE") or "")
    if not base:
        raise AgentError("没有配置 --llm-base（OpenAI 兼容地址）")
    if not base.endswith("/chat/completions"):
        base = base + "/chat/completions"
    model = ctx.get("model") or os.environ.get("SWARM_LLM_MODEL") or "gpt-4o-mini"
    key = ctx.get("api_key") or os.environ.get("SWARM_LLM_KEY") or "sk-local"
    log = ctx["log"]

    file_list = []
    budget = 200000
    for rel in ctx.get("file_list") or []:
        if budget <= 0:
            file_list.append("### （其余文件因提示词体积上限被省略）")
            break
        full = os.path.join(ctx["workdir"], rel)
        try:
            with open(full, "r", encoding="utf-8", errors="replace") as f:
                body = f.read(min(20000, budget))
        except OSError:
            body = ""
        budget -= len(body)
        file_list.append("### %s\n```\n%s\n```" % (rel, body))
    prompt = HTTP_PROMPT.format(
        title=ctx.get("title", ""), prompt=ctx["prompt"],
        files="\n\n".join(file_list) or "（空目录，你可以新建文件）",
        scope=", ".join(ctx.get("files") or ["**"]),
    )
    if ctx.get("feedback") and "上次尝试失败" not in prompt:
        prompt += "\n\n上一次尝试失败的原因（这次必须避免）：\n" + str(ctx["feedback"])[:4000]
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": "You are a focused coding agent. Reply with JSON only."},
            {"role": "user", "content": prompt},
        ],
    }
    req = urllib.request.Request(
        base, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + key},
    )
    log("请求模型 %s" % model)
    timeout = min(int(ctx.get("timeout") or 600), 3600)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(10 << 20)
        if len(raw) >= 10 << 20:
            raise AgentError("模型响应超过 10MB，拒绝解析")
        body = json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise AgentError("模型接口 HTTP %s：%s" % (exc.code, exc.read()[:300]))
    except Exception as exc:
        raise AgentError("模型接口不可用：%s" % exc)

    text = (body.get("choices") or [{}])[0].get("message", {}).get("content", "")
    spec = _extract_json(text)
    if not spec:
        raise AgentError("模型没有返回可解析的 JSON：%s" % text[:200])
    written = []
    files = spec.get("files") or {}
    if not isinstance(files, dict):
        raise AgentError("模型返回的 files 不是对象")
    for rel, content in files.items():
        safe = workspace._safe_rel(rel)
        if safe is None:
            raise AgentError("非法路径：%s" % rel)
        if not isinstance(content, str):
            raise AgentError("文件内容不是字符串：%s" % rel)
        if len(content) > 2 << 20:
            raise AgentError("单文件内容过大：%s" % rel)
        full = os.path.join(ctx["workdir"], safe)
        parent = os.path.dirname(full)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)
        written.append(safe)
        log("写入 %s" % safe)
    deletes = spec.get("delete") or []
    if not isinstance(deletes, list):
        raise AgentError("模型返回的 delete 不是数组")
    for rel in deletes:
        safe = workspace._safe_rel(rel)
        if safe is None:
            raise AgentError("非法删除路径：%s" % rel)
        full = os.path.join(ctx["workdir"], safe)
        if os.path.isfile(full):
            os.unlink(full)
            written.append(safe)
            log("删除 %s" % safe)
    return written


def _extract_json(text):
    if not text:
        return None
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    raw = fence.group(1) if fence else text
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        return json.loads(raw[start:end + 1])
    except Exception:
        return None


KINDS = {"mock": run_mock, "cli": run_cli, "http": run_http}


def run_agent(kind, ctx):
    fn = KINDS.get(kind)
    if fn is None:
        raise AgentError("未知 agent 类型：%s" % kind)
    return fn(ctx)

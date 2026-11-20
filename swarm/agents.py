"""Agent 适配器：把任务交给本机已有的 AI 工具去干。

已经装在 Linux 里的东西都能接：
    mock  —— 不配任何东西，用来跑通链路 / 压测调度
    cli   —— 任何命令行 agent：aider / claude / codex / ollama / 自己写的脚本
    http  —— OpenAI 兼容接口（本地 llama.cpp、vLLM、LM Studio、Ollama、云 API 都行）

适配器只管"写文件"，改了什么由工作区比对，不依赖 agent 自己汇报。
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request


class AgentError(Exception):
    pass


# --------------------------------------------------------------------------
# mock：确定性本地模拟
# --------------------------------------------------------------------------

def run_mock(ctx):
    """按任务范围生成/补写文件，带点延时，模拟真实 agent 的耗时。"""
    log = ctx["log"]
    files = ctx["files"] or ["src/**"]
    slug = _slug(ctx["task_id"] + "-" + ctx.get("title", ""))
    base = _scope_base(files)
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
        path = os.path.join(ctx["workdir"], base, slug + ".py")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(_stub_py(slug, ctx["title"], ctx["prompt"]))
        written.append(os.path.relpath(path, ctx["workdir"]).replace(os.sep, "/"))
        log("新建了 %s" % written[-1])

    # 再补一个范围里的说明文件，让"多文件改动"这条路径也被覆盖到
    note = os.path.join(ctx["workdir"], base, slug + ".md")
    if os.path.dirname(note):
        os.makedirs(os.path.dirname(note), exist_ok=True)
    with open(note, "w", encoding="utf-8") as f:
        f.write("# %s\n\n任务：%s\n\n%s\n" % (ctx["title"], ctx["task_id"], ctx["prompt"]))
    written.append(os.path.relpath(note, ctx["workdir"]).replace(os.sep, "/"))

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


def _scope_base(files):
    """从 'src/api/**' 推出落盘目录 'src/api'。"""
    for pat in files or []:
        parts = []
        for seg in pat.replace("\\", "/").split("/"):
            if any(ch in seg for ch in "*?["):
                break
            parts.append(seg)
        base = "/".join(parts)
        if base and base not in (".", "/"):
            return base
    return "src"


def _list_files(root):
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in (".git", "__pycache__", "node_modules")]
        for name in filenames:
            out.append(os.path.relpath(os.path.join(dirpath, name), root).replace(os.sep, "/"))
    return out


def _in_scope(path, files):
    from .workspace import match_any
    return match_any(path, files) if files else True


# --------------------------------------------------------------------------
# cli：把 prompt 交给本机任意 agent 命令
# --------------------------------------------------------------------------

DEFAULT_CLI = "{agent_cmd}"


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
    }
    cmd = template
    for key, val in values.items():
        cmd = cmd.replace("{" + key + "}", str(val))
    if "{" in cmd and "}" in cmd and "SWARM" not in cmd:
        pass

    env = dict(os.environ)
    env.update({
        "SWARM_PROMPT": values["prompt"],
        "SWARM_FILES": values["files"],
        "SWARM_TASK_ID": values["task_id"],
        "SWARM_TASK_TITLE": values["title"],
        "SWARM_WORKDIR": ctx["workdir"],
        "SWARM_TASK_FILE": os.path.join(ctx["workdir"], ".swarm-task.md"),
    })
    with open(env["SWARM_TASK_FILE"], "w", encoding="utf-8") as f:
        f.write("# %s\n\n%s\n\n## 允许改动的文件\n\n%s\n" % (values["title"], values["prompt"], values["file_list"] or "（未限定）"))

    log("执行：%s" % cmd)
    timeout = int(ctx.get("timeout") or 1800)
    try:
        proc = subprocess.Popen(
            cmd, shell=True, cwd=ctx["workdir"], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True, bufsize=1,
        )
    except OSError as exc:
        raise AgentError("启动命令失败：%s" % exc)

    lines = []
    deadline = time.time() + timeout
    try:
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                lines.append(line)
                log(line[:400])
            if time.time() > deadline:
                proc.kill()
                raise AgentError("agent 超时（%ds）" % timeout)
        proc.wait()
    finally:
        try:
            proc.stdout.close()
        except Exception:
            pass
    if proc.returncode != 0:
        tail = "\n".join(lines[-8:])
        raise AgentError("agent 退出码 %s\n%s" % (proc.returncode, tail))
    return lines


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
{{"files": {{"相对路径": "完整文件内容", ...}, "delete": ["要删除的相对路径"]}}
3. 路径必须落在这些范围内：{scope}
"""


def run_http(ctx):
    base = (ctx.get("base_url") or os.environ.get("SWARM_LLM_BASE") or "").rstrip("/")
    if not base:
        raise AgentError("没有配置 --llm-base（OpenAI 兼容地址）")
    if not base.endswith("/chat/completions"):
        base = base + "/chat/completions"
    model = ctx.get("model") or os.environ.get("SWARM_LLM_MODEL") or "gpt-4o-mini"
    key = ctx.get("api_key") or os.environ.get("SWARM_LLM_KEY") or "sk-local"
    log = ctx["log"]

    file_list = []
    for rel in ctx.get("file_list") or []:
        full = os.path.join(ctx["workdir"], rel)
        try:
            with open(full, "r", encoding="utf-8", errors="replace") as f:
                body = f.read(20000)
        except OSError:
            body = ""
        file_list.append("### %s\n```\n%s\n```" % (rel, body))
    prompt = HTTP_PROMPT.format(
        title=ctx.get("title", ""), prompt=ctx["prompt"],
        files="\n\n".join(file_list) or "（空目录，你可以新建文件）",
        scope=", ".join(ctx.get("files") or ["**"]),
    )
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
    try:
        with urllib.request.urlopen(req, timeout=int(ctx.get("timeout") or 600)) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise AgentError("模型接口 HTTP %s：%s" % (exc.code, exc.read()[:300]))
    except Exception as exc:
        raise AgentError("模型接口不可用：%s" % exc)

    text = (body.get("choices") or [{}])[0].get("message", {}).get("content", "")
    spec = _extract_json(text)
    if not spec:
        raise AgentError("模型没有返回可解析的 JSON：%s" % text[:200])
    written = []
    for rel, content in (spec.get("files") or {}).items():
        rel = rel.lstrip("/").replace("\\", "/")
        if ".." in rel.split("/"):
            raise AgentError("非法路径：%s" % rel)
        full = os.path.join(ctx["workdir"], rel)
        os.makedirs(os.path.dirname(full) or ctx["workdir"], exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)
        written.append(rel)
        log("写入 %s" % rel)
    for rel in spec.get("delete") or []:
        full = os.path.join(ctx["workdir"], rel)
        if os.path.isfile(full):
            os.unlink(full)
            written.append(rel)
            log("删除 %s" % rel)
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

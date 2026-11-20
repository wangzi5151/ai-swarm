"""把一个大项目拆成带依赖的任务图。

两种输入：
1. 人写的 spec（markdown）。按 "## 模块名 (路径/glob)" 标题切分；
   没写标题就走默认脚手架：公共层 -> 并行模块 -> 集成测试 -> 文档。
2. 显式 plan.json（{"tasks": [...]}），完全可控，适合让某个强模型先规划一次。

任务字段：
    id      唯一
    title   给人看的
    prompt  交给 agent 的说明
    read    这个任务要看哪些文件（默认 **）
    write   这个任务允许改哪些文件（隔离靠它）
    deps    依赖的任务 id
    agent   可选，限定只派给某种 agent 的节点
"""

from __future__ import annotations

import hashlib
import json
import re

DEFAULT_MODULES = ["core", "api", "ui", "storage", "service"]


def _slug(text):
    out = re.sub(r"[^0-9a-zA-Z]+", "-", text).strip("-").lower()
    if not out:
        out = "m" + hashlib.sha1(text.encode("utf-8")).hexdigest()[:6]
    if out[0].isdigit():
        out = "t-" + out
    return out[:40]


def parse_explicit(text):
    """spec 直接就是一份 JSON 计划。"""
    text = text.strip()
    if not text.startswith("{"):
        return None
    try:
        data = json.loads(text)
    except Exception:
        return None
    tasks = data.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        return None
    out = []
    for i, raw in enumerate(tasks):
        tid = str(raw.get("id") or "t%d" % (i + 1))
        out.append({
            "id": tid,
            "title": raw.get("title") or tid,
            "prompt": raw.get("prompt") or raw.get("title") or "",
            "read": raw.get("read") or ["**"],
            "write": raw.get("write") or raw.get("files") or ["**"],
            "deps": raw.get("deps") or [],
            "agent": raw.get("agent"),
            "cmd": raw.get("cmd"),
            "model": raw.get("model"),
            "priority": int(raw.get("priority") or 0),
        })
    return {"title": data.get("title"), "tasks": out}


def split_spec(spec):
    """按 markdown 二级标题切段，返回 [(标题, 正文, 路径提示|None)]"""
    sections = []
    cur_title = None
    cur_lines = []
    cur_hint = None
    for line in spec.splitlines():
        m = re.match(r"^#{2,3}\s+(.+?)\s*$", line)
        if m:
            if cur_title is not None:
                sections.append((cur_title, "\n".join(cur_lines).strip(), cur_hint))
            raw = m.group(1)
            hint = None
            hm = re.search(r"[\(（]\s*([^()（）]*?)\s*[\)）]\s*$", raw)
            if hm and ("/" in hm.group(1) or "*" in hm.group(1)):
                hint = hm.group(1).strip()
                raw = raw[:hm.start()].strip()
            cur_title = raw
            cur_hint = hint
            cur_lines = []
        elif cur_title is not None:
            cur_lines.append(line)
    if cur_title is not None:
        sections.append((cur_title, "\n".join(cur_lines).strip(), cur_hint))
    return sections


def _is_meta(title):
    return any(k in title for k in ("需求", "背景", "目标", "说明", "验收", "约束", "非目标", "Overview", "背景"))


def plan(spec, title=None, max_tasks=8, modules=None):
    explicit = parse_explicit(spec)
    if explicit is not None:
        if title and not explicit.get("title"):
            explicit["title"] = title
        return explicit

    sections = split_spec(spec)
    modules_secs = [s for s in sections if not _is_meta(s[0])]
    tasks = []

    core_read = ["**"]
    core = {
        "id": "t-core",
        "title": "架构与公共层",
        "prompt": "搭建项目骨架与公共接口：目录结构、核心类型/常量、错误处理、README。" + _ctx(spec),
        "read": ["**"],
        "write": ["src/core/**", "src/__init__.py", "docs/architecture.md", "README.md"],
        "deps": [],
    }
    tasks.append(core)

    wanted = modules or [t for t, _b, _h in modules_secs] or DEFAULT_MODULES
    wanted = [w for w in wanted if not _is_meta(w)]
    if not wanted:
        wanted = DEFAULT_MODULES
    # 任务总数上限：公共层 + 模块 + 集成 + 文档
    room = max(3, max_tasks) - 3
    if len(wanted) > room:
        kept = wanted[:room - 1]
        merged = wanted[room - 1:]
        if kept and merged:
            kept[-1] = kept[-1] + "、" + "、".join(merged)
        wanted = kept

    section_map = {}
    for stitle, body, hint in modules_secs:
        section_map[stitle] = (body, hint)

    for idx, name in enumerate(wanted):
        body, hint = section_map.get(name, ("", None))
        ascii_slug = re.sub(r"[^0-9a-zA-Z]+", "-", name).strip("-").lower()
        scope = hint or ("src/%s/**" % (ascii_slug or ("module%d" % (idx + 1))))
        if not scope.endswith("*") and not scope.endswith("/"):
            scope = scope.rstrip("/") + "/**"
        tid = "t-" + (ascii_slug or ("m%d" % (idx + 1)))
        tasks.append({
            "id": tid,
            "title": name,
            "prompt": ("实现「%s」模块。\n\n%s\n\n%s" % (name, body or "按项目需求实现本模块。", _ctx(spec))).strip(),
            "read": ["src/core/**", "docs/architecture.md", "README.md"],
            "write": [scope],
            "deps": ["t-core"],
        })

    module_ids = [t["id"] for t in tasks if t["id"] != "t-core"]
    tasks.append({
        "id": "t-integration",
        "title": "集成与测试",
        "prompt": "把各模块接起来：补入口、装配、端到端测试与冒烟脚本，确保整体能跑。" + _ctx(spec),
        "read": ["**"],
        "write": ["tests/**", "scripts/**", "src/main.py", "src/app.py", "src/index.js"],
        "deps": list(module_ids),
    })
    tasks.append({
        "id": "t-docs",
        "title": "文档与交付说明",
        "prompt": "根据最终代码写使用说明、目录结构、运行方式。" + _ctx(spec),
        "read": ["**"],
        "write": ["docs/**"],
        "deps": list(module_ids),
    })
    return {"title": title or _guess_title(spec), "tasks": tasks}


def _ctx(spec):
    spec = (spec or "").strip()
    if not spec:
        return ""
    return "\n\n项目需求原文：\n" + spec[:4000]


def _guess_title(spec):
    for line in (spec or "").splitlines():
        line = line.strip().lstrip("#").strip()
        if line:
            return line[:60]
    return "未命名项目"


def validate(plan_obj):
    """补全默认值，检查依赖是否存在、是否有环。"""
    tasks = plan_obj.get("tasks") or []
    ids = [t["id"] for t in tasks]
    if len(set(ids)) != len(ids):
        raise ValueError("任务 id 重复")
    idset = set(ids)
    for t in tasks:
        t.setdefault("read", ["**"])
        t.setdefault("write", ["**"])
        t.setdefault("priority", 0)
        t.setdefault("agent", None)
        t.setdefault("cmd", None)
        t.setdefault("model", None)
        for d in t.get("deps") or []:
            if d not in idset:
                raise ValueError("任务 %s 依赖了不存在的 %s" % (t["id"], d))
        deps = [d for d in (t.get("deps") or []) if d != t["id"]]
        t["deps"] = deps
    state = {}

    def visit(tid, stack):
        st = state.get(tid)
        if st == 2:
            return
        if st == 1:
            raise ValueError("任务图有环：%s" % " -> ".join(stack + [tid]))
        state[tid] = 1
        for d in _deps(tasks, tid):
            visit(d, stack + [tid])
        state[tid] = 2

    for tid in ids:
        visit(tid, [])
    return plan_obj


def _deps(tasks, tid):
    for t in tasks:
        if t["id"] == tid:
            return t.get("deps") or []
    return []


def critical_path_order(plan_obj):
    """给调度器一个初始优先级：依赖越深越优先（先干卡脖子的）。"""
    tasks = {t["id"]: t for t in plan_obj["tasks"]}

    def depth(tid, seen=None):
        seen = seen or set()
        if tid in seen:
            return 0
        seen.add(tid)
        deps = tasks[tid].get("deps") or []
        return 0 if not deps else 1 + max(depth(d, seen) for d in deps)

    for tid, t in tasks.items():
        t["priority"] = max(int(t.get("priority") or 0), depth(tid))
    return plan_obj

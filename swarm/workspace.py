"""工作区：快照 -> 节点 -> 改动清单 -> 合并。

三个关键动作
    snapshot()     hub 侧：把项目里"这个任务该看的文件"做成清单（path -> sha）
    materialize()  节点侧：按清单把文件从缓存铺到工作目录（跨任务共享 blob cache）
    diff_work()    节点侧：跟清单比，算出 新增/修改/删除

没有 unified diff 解析，直接用内容哈希判断，二进制文件也不会坏。
"""

from __future__ import annotations

import os
import re
import shutil

from .store import BlobStore, sha256_file

# 这些目录永远不进快照
IGNORE_DIRS = {".git", ".hg", ".svn", "__pycache__", "node_modules", ".gradle", ".idea", ".venv", "venv"}
IGNORE_FILES = {".DS_Store"}
MAX_FILE = 8 << 20  # 单文件 8MB 以上不参与同步


# --------------------------------------------------------------------------
# glob（支持 **）
# --------------------------------------------------------------------------

_glob_cache = {}


def _glob_to_re(pattern):
    rx = _glob_cache.get(pattern)
    if rx is not None:
        return rx
    p = pattern.strip()
    while p.startswith("./"):
        p = p[2:]
    out = ["^"]
    i = 0
    n = len(p)
    while i < n:
        c = p[i]
        if c == "*":
            if i + 1 < n and p[i + 1] == "*":
                i += 2
                if i < n and p[i] == "/":
                    i += 1
                    out.append("(?:.*/)?")
                else:
                    out.append(".*")
                continue
            out.append("[^/]*")
        elif c == "?":
            out.append("[^/]")
        elif c == "[":
            j = p.find("]", i + 1)
            if j == -1:
                out.append(re.escape(c))
            else:
                body = p[i + 1:j]
                if body.startswith("!"):
                    body = "^" + body[1:]
                out.append("[" + body + "]")
                i = j
        else:
            out.append(re.escape(c))
        i += 1
    out.append("$")
    rx = re.compile("".join(out))
    _glob_cache[pattern] = rx
    return rx


def match_any(path, patterns):
    if not patterns:
        return False
    for pat in patterns:
        if _glob_to_re(pat).match(path):
            return True
        # 目录形式：'src/' 等于 'src/**'
        if pat.endswith("/") and _glob_to_re(pat + "**").match(path):
            return True
    return False


def _static_prefix(pattern):
    """'src/api/**' -> 'src/api'；用来判断两个任务的范围是否可能重叠。"""
    parts = []
    for seg in pattern.replace("\\", "/").split("/"):
        if any(ch in seg for ch in "*?["):
            break
        parts.append(seg)
    return "/".join(parts)


def patterns_may_overlap(a, b):
    pa, pb = _static_prefix(a), _static_prefix(b)
    if pa == pb:
        return True
    if not pa or not pb:
        return True          # 一方是通配根，就当重叠
    return pa.startswith(pb + "/") or pb.startswith(pa + "/")


def scopes_overlap(files_a, files_b):
    for a in files_a or ["**"]:
        for b in files_b or ["**"]:
            if patterns_may_overlap(a, b):
                return True
    return False


# --------------------------------------------------------------------------
# 遍历
# --------------------------------------------------------------------------

def iter_tree(root):
    """产出 (相对路径, 绝对路径)，跳过 .git / 隐藏缓存目录 / 超大文件。"""
    root = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in IGNORE_DIRS and not d.startswith(".tmp-"))
        for name in sorted(filenames):
            if name in IGNORE_FILES or name.startswith(".tmp-"):
                continue
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            yield rel, full


def _capable(path):
    try:
        st = os.stat(path)
    except OSError:
        return False
    return st.st_size <= MAX_FILE


# --------------------------------------------------------------------------
# 快照 / 铺设 / 比对
# --------------------------------------------------------------------------

def snapshot(tree, store, patterns=None, index=None):
    """返回 [{path, sha, size, mode}]，并把内容塞进 store。

    index: 可选 {(path, size, mtime_ns): sha}，避免每次重算哈希。
    """
    entries = []
    for rel, full in iter_tree(tree):
        if patterns and not match_any(rel, patterns):
            continue
        if not _capable(full):
            continue
        st = os.stat(full)
        key = (rel, st.st_size, getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
        sha = index.get(key) if index is not None else None
        if sha is None or not store.has(sha):
            sha = store.put_file(full)
            if index is not None:
                index[key] = sha
        entries.append({
            "path": rel,
            "sha": sha,
            "size": st.st_size,
            "mode": 0o755 if os.access(full, os.X_OK) else 0o644,
        })
    return entries


def materialize(workdir, store, entries, cache=None):
    """把清单落到 workdir。cache 是节点本地 CAS，命中就直接拷。"""
    os.makedirs(workdir, exist_ok=True)
    for entry in entries:
        rel = entry["path"]
        dest = os.path.join(workdir, rel)
        parent = os.path.dirname(dest)
        if parent:
            os.makedirs(parent, exist_ok=True)
        sha = entry["sha"]
        src = None
        if cache is not None and cache.has(sha):
            src = cache.path(sha)
        elif store is not None and store.has(sha):
            src = store.path(sha)
        if src is None:
            raise IOError("blob 缺失：%s (%s)" % (rel, sha))
        shutil.copyfile(src, dest)
        try:
            os.chmod(dest, entry.get("mode", 0o644))
        except OSError:
            pass


def diff_work(workdir, base_entries, patterns=None):
    """跟 base 比。返回 change 列表：[{path, sha, deleted, size}]"""
    base = {e["path"]: e["sha"] for e in base_entries}
    changes = []
    seen = set()
    for rel, full in iter_tree(workdir):
        if patterns and not match_any(rel, patterns):
            continue
        if not _capable(full):
            continue
        seen.add(rel)
        sha = sha256_file(full)
        if base.get(rel) != sha:
            changes.append({"path": rel, "sha": sha, "size": os.path.getsize(full), "deleted": False})
    for rel in base:
        if rel in seen:
            continue
        if patterns and not match_any(rel, patterns):
            continue
        changes.append({"path": rel, "sha": None, "size": 0, "deleted": True})
    changes.sort(key=lambda c: c["path"])
    return changes


def out_of_scope(changes, patterns):
    """节点改了自己任务范围之外的文件 —— 工作间隔离的最后一道闸。"""
    if not patterns:
        return []
    return [c["path"] for c in changes if not match_any(c["path"], patterns)]


def apply_change(tree, change, store):
    """把一个 change 写进项目树。返回 None 或 错误原因。"""
    rel = change["path"]
    dest = os.path.join(tree, rel)
    if change.get("deleted"):
        if os.path.exists(dest):
            try:
                os.unlink(dest)
            except OSError as exc:
                return "删除失败：%s" % exc
            _prune_empty(os.path.dirname(dest), tree)
        return None
    data_path = store.path(change["sha"])
    if not os.path.exists(data_path):
        return "blob 丢失"
    parent = os.path.dirname(dest)
    if parent:
        os.makedirs(parent, exist_ok=True)
    shutil.copyfile(data_path, dest)
    return None


def _prune_empty(d, stop):
    stop = os.path.abspath(stop)
    d = os.path.abspath(d)
    while d.startswith(stop) and d != stop:
        try:
            if os.listdir(d):
                return
            os.rmdir(d)
        except OSError:
            return
        d = os.path.dirname(d)


def tree_sha(tree, rel):
    full = os.path.join(tree, rel)
    if not os.path.isfile(full):
        return None
    return sha256_file(full)

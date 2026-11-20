"""工作区：快照 -> 节点 -> 改动清单 -> 合并。

三个关键动作
    snapshot()     hub 侧：把项目里"这个任务该看的文件"做成清单（path -> sha）
    materialize()  节点侧：按清单把文件从缓存铺到工作目录（跨任务共享 blob cache）
    diff_work()    节点侧：跟清单比，算出 新增/修改/删除

没有 unified diff 解析，直接用内容哈希判断，二进制文件也不会坏。
另外支持项目根目录的 .gitignore（`build/`、`dist/` 这类产物不会进入同步），
并跳过符号链接 —— 节点拿到的永远是项目自己的文件，不能靠软链读到项目外。
"""

from __future__ import annotations

import difflib
import os
import re
import shutil
import tempfile

from .store import BlobStore, is_sha, sha256_file

# 这些目录永远不进快照
IGNORE_DIRS = {".git", ".hg", ".svn", "__pycache__", "node_modules", ".gradle",
               ".idea", ".venv", "venv", "target", ".mypy_cache", ".pytest_cache"}
IGNORE_FILES = {".DS_Store", ".swarm-task.md", ".swarm-feedback.md"}


def _env_max_file():
    """单文件同步上限，默认 32MB；SWARM_MAX_FILE 可调（1MB ~ 512MB）。"""
    raw = os.environ.get("SWARM_MAX_FILE")
    try:
        value = int(raw) if raw else (32 << 20)
    except (TypeError, ValueError):
        value = 32 << 20
    return max(1 << 20, min(value, 512 << 20))


MAX_FILE = _env_max_file()


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
        elif c == "{":
            j = p.find("}", i + 1)
            if j == -1:
                out.append(re.escape(c))
            else:
                body = p[i + 1:j]
                alts = [re.escape(x) for x in body.split(",") if x]
                if alts:
                    out.append("(?:" + "|".join(alts) + ")")
                    i = j
                else:
                    out.append(re.escape(c))
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
        pat = str(pat or "")
        if not pat:
            continue
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
        if any(ch in seg for ch in "*?[{"):
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
# .gitignore
# --------------------------------------------------------------------------

class IgnoreRules(object):
    """够用的 .gitignore 实现：锚定 /、`**`、目录尾 /、! 取反、嵌套文件。"""

    def __init__(self):
        self.rules = []
        self._rx = {}

    def add_text(self, base, text):
        base = (base or "").replace("\\", "/").strip("/")
        for raw in (text or "").splitlines():
            line = raw.rstrip()
            if not line or line.lstrip().startswith("#"):
                continue
            line = line.strip()
            neg = line.startswith("!")
            if neg:
                line = line[1:].strip()
            if not line:
                continue
            dir_only = line.endswith("/")
            line = line.rstrip("/")
            anchored = line.startswith("/")
            line = line.lstrip("/")
            if not line:
                continue
            self.rules.append((base, line, neg, dir_only, anchored))
            self._rx.setdefault(line, _glob_to_re(line))

    def add_file(self, base, path):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                self.add_text(base, f.read(200000))
        except OSError:
            pass

    def ignores(self, rel):
        """路径本身或它的任一父目录被忽略 -> True（用于提交侧复核）。"""
        parts = rel.replace("\\", "/").strip("/").split("/")
        for i in range(1, len(parts) + 1):
            prefix = "/".join(parts[:i])
            if self.ignored(prefix, i < len(parts)):
                return True
        return False

    def ignored(self, rel, is_dir):
        rel = rel.replace("\\", "/").strip("/")
        if not rel:
            return False
        result = False
        for base, pat, neg, dir_only, anchored in self.rules:
            if dir_only and not is_dir:
                # 以 / 结尾的规则只匹配目录本身；目录被剪枝后文件不会进来。
                # 但 "build/" 也应吃掉 build 目录：is_dir 为 True 时下面会命中。
                continue
            if _rule_hit(rel, base, pat, anchored, self._rx[pat]):
                result = not neg
        return result


def _rule_hit(rel, base, pat, anchored, rx):
    if base:
        if rel == base:
            return False
        if not rel.startswith(base + "/"):
            return False
        sub = rel[len(base) + 1:]
    else:
        sub = rel
    if not sub:
        return False
    if anchored or "/" in pat:
        parts = sub.split("/")
        candidates = ["/".join(parts[:i]) for i in range(1, len(parts) + 1)]
    else:
        candidates = sub.split("/")
    for cand in candidates:
        if rx.match(cand):
            return True
    return False


def load_ignores(root):
    """读取整棵树的 .gitignore（含嵌套），失败就当没有。"""
    rules = IgnoreRules()
    root = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root).replace(os.sep, "/")
        rel = "" if rel == "." else rel
        dirnames[:] = sorted(d for d in dirnames
                             if d not in IGNORE_DIRS and not d.startswith(".tmp-"))
        if ".gitignore" in filenames:
            rules.add_file(rel, os.path.join(dirpath, ".gitignore"))
    return rules


# --------------------------------------------------------------------------
# 遍历
# --------------------------------------------------------------------------

def iter_tree(root, ignore=None):
    """产出 (相对路径, 绝对路径)，跳过 VCS / 缓存目录 / 超大文件 / 软链。"""
    root = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root).replace(os.sep, "/")
        rel_dir = "" if rel_dir == "." else rel_dir
        kept = []
        for d in sorted(dirnames):
            if d in IGNORE_DIRS or d.startswith(".tmp-"):
                continue
            full = os.path.join(dirpath, d)
            if os.path.islink(full):
                continue
            rel = (rel_dir + "/" + d) if rel_dir else d
            if ignore is not None and ignore.ignored(rel, True):
                continue
            kept.append(d)
        dirnames[:] = kept
        for name in sorted(filenames):
            if name in IGNORE_FILES or name.startswith(".tmp-"):
                continue
            full = os.path.join(dirpath, name)
            if os.path.islink(full):
                continue
            rel = (rel_dir + "/" + name) if rel_dir else name
            if ignore is not None and ignore.ignored(rel, False):
                continue
            yield rel, full


def oversized(root, limit=MAX_FILE):
    """返回超过同步上限的文件 [(rel, size)]，用于给 agent 明确提示。"""
    out = []
    for rel, full in iter_tree(root):
        try:
            size = os.path.getsize(full)
        except OSError:
            continue
        if size > limit:
            out.append((rel, size))
    return out


def _capable(path):
    try:
        st = os.stat(path)
    except OSError:
        return False
    return st.st_size <= MAX_FILE


# --------------------------------------------------------------------------
# 快照 / 铺设 / 比对
# --------------------------------------------------------------------------

def snapshot(tree, store, patterns=None, index=None, ignore=None):
    """返回 [{path, sha, size, mode}]，并把内容塞进 store。

    index: 可选 {(path, size, mtime_ns): sha}，避免每次重算哈希。
    """
    entries = []
    for rel, full in iter_tree(tree, ignore=ignore):
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


def _safe_rel(path):
    rel = str(path or "").replace("\\", "/").strip()
    if not rel or rel.startswith("/") or ":" in rel.split("/")[0]:
        return None
    parts = rel.split("/")
    if any(p in ("", ".", "..") for p in parts):
        return None
    if "\x00" in rel:
        return None
    return rel


def materialize(workdir, store, entries, cache=None):
    """把清单落到 workdir。cache 是节点本地 CAS，命中就直接拷。"""
    os.makedirs(workdir, exist_ok=True)
    root = os.path.abspath(workdir)
    for entry in entries:
        rel = _safe_rel(entry.get("path"))
        if rel is None:
            raise IOError("快照里有非法路径：%r" % (entry.get("path"),))
        sha = entry.get("sha")
        if not is_sha(sha):
            raise IOError("快照里有非法 sha：%s (%s)" % (rel, sha))
        dest = os.path.join(root, rel)
        if not os.path.abspath(dest).startswith(root + os.sep):
            raise IOError("路径越界：%s" % rel)
        parent = os.path.dirname(dest)
        if parent:
            os.makedirs(parent, exist_ok=True)
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


def diff_work(workdir, base_entries, patterns=None, ignore=None):
    """跟 base 比。返回 change 列表：[{path, sha, deleted, size}]"""
    base = {}
    for e in base_entries:
        rel = _safe_rel(e.get("path"))
        if rel:
            base[rel] = e.get("sha")
    changes = []
    seen = set()
    for rel, full in iter_tree(workdir, ignore=ignore):
        if patterns and not match_any(rel, patterns):
            continue
        if not _capable(full):
            continue
        seen.add(rel)
        sha = sha256_file(full)
        if base.get(rel) != sha:
            changes.append({"path": rel, "sha": sha, "size": os.path.getsize(full),
                            "mode": 0o755 if os.access(full, os.X_OK) else 0o644,
                            "deleted": False})
    for rel in base:
        if rel in seen:
            continue
        if patterns and not match_any(rel, patterns):
            continue
        # 文件还在，只是新加的 .gitignore 规则把它藏起来了 —— 不能当成删除
        if ignore is not None and ignore.ignores(rel):
            continue
        changes.append({"path": rel, "sha": None, "size": 0, "deleted": True})
    changes.sort(key=lambda c: c["path"])
    return changes


def out_of_scope(changes, patterns):
    """节点改了自己任务范围之外的文件 —— 工作间隔离的最后一道闸。"""
    if not patterns:
        return []
    return [c["path"] for c in changes if not match_any(c["path"], patterns)]


def _symlink_parent(root, rel):
    """路径的任一父目录是符号链接 -> True（防止借软链写到项目外）。"""
    cur = os.path.abspath(root)
    for part in rel.split("/")[:-1]:
        cur = os.path.join(cur, part)
        if os.path.islink(cur):
            return True
    return False


def apply_change(tree, change, store):
    """把一个 change 写进项目树。返回 None 或 错误原因。"""
    rel = _safe_rel(change.get("path"))
    if rel is None:
        return "非法路径"
    dest = os.path.join(tree, rel)
    root = os.path.abspath(tree)
    if not os.path.abspath(dest).startswith(root + os.sep):
        return "路径越界"
    if _symlink_parent(root, rel):
        return "路径经过符号链接"
    if change.get("deleted"):
        if os.path.exists(dest):
            try:
                os.unlink(dest)
            except OSError as exc:
                return "删除失败：%s" % exc
            _prune_empty(os.path.dirname(dest), tree)
        return None
    sha = change.get("sha")
    if not is_sha(sha):
        return "sha 非法"
    data_path = store.path(sha)
    if not os.path.exists(data_path):
        return "blob 丢失"
    parent = os.path.dirname(dest)
    if parent:
        os.makedirs(parent, exist_ok=True)
    # 先写同目录临时文件再原子替换：合并中途断电也不会留下半个文件。
    # 前缀用 .tmp-，遍历项目时会自动跳过，不会被当成源码同步出去。
    try:
        existing_mode = os.stat(dest).st_mode & 0o777
    except OSError:
        existing_mode = 0o644
    fd, tmp = tempfile.mkstemp(dir=parent or ".", prefix=".tmp-swarm-merge-")
    try:
        with os.fdopen(fd, "wb") as out, open(data_path, "rb") as inp:
            shutil.copyfileobj(inp, out, 1 << 20)
        mode = change.get("mode")
        if not isinstance(mode, int):
            mode = existing_mode
        try:
            os.chmod(tmp, mode & 0o777)
        except OSError:
            pass
        os.replace(tmp, dest)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return "写入失败"
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
    safe = _safe_rel(rel)
    if safe is None:
        return None
    full = os.path.join(tree, safe)
    if not os.path.isfile(full) or os.path.islink(full):
        return None
    return sha256_file(full)


def is_binary(data):
    """粗判二进制：含 NUL 或解码失败。用于决定能不能做三方文本合并。"""
    if not data:
        return False
    if b"\x00" in data[:8192]:
        return True
    try:
        data.decode("utf-8")
        return False
    except UnicodeDecodeError:
        return True


# --------------------------------------------------------------------------
# 三方合并（可选）：两边改的是同一文件的不同区域 -> 自动合，重叠才交给人
# --------------------------------------------------------------------------

def _changed_blocks(base_lines, other_lines):
    """返回 [(base_i1, base_i2, 新行列表)]。"""
    sm = difflib.SequenceMatcher(None, base_lines, other_lines, autojunk=False)
    out = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag != "equal":
            out.append((i1, i2, other_lines[j1:j2]))
    return out


def _blocks_conflict(a, b):
    a1, a2, alines = a
    b1, b2, blines = b
    if a1 == a2 and b1 == b2:
        # 同一点各自插入：两边都保留（相同的插入去重），不算冲突
        return False
    if a1 == a2:                              # a 是插入
        return b1 < a1 < b2
    if b1 == b2:
        return a1 < b1 < a2
    return max(a1, b1) < min(a2, b2)


def merge3(base, ours, theirs):
    """文本三方合并。成功返回合并后的字符串，重叠改动返回 None。

    base 是任务派发时的版本，ours 是主工程当前版本，theirs 是节点交付的版本。
    只处理 UTF-8 文本；行尾差异交给 splitlines(True) 原样保留。
    """
    if ours == theirs:
        return ours
    if ours == base:
        return theirs
    if theirs == base:
        return ours
    b = base.splitlines(True)
    co = _changed_blocks(b, ours.splitlines(True))
    ct = _changed_blocks(b, theirs.splitlines(True))
    # 两边做出了完全一样的改动不算冲突（先去掉重复事件）
    unique = {}
    for side, blocks in ((0, co), (1, ct)):
        for (i1, i2, lines) in blocks:
            unique.setdefault((i1, i2, tuple(lines)), (i1, i2, lines, side))
    for a in co:
        for c in ct:
            if (a[0], a[1], tuple(a[2])) == (c[0], c[1], tuple(c[2])):
                continue
            if _blocks_conflict(a, c):
                return None
    events = list(unique.values())
    # 同一位置的两个相同插入只保留一个；替换优先于插入
    events.sort(key=lambda e: (e[0], 0 if e[1] == e[0] else 1, e[3]))
    merged = []
    pos = 0
    prev = None
    for i1, i2, lines, side in events:
        if i1 < pos:
            return None
        if prev is not None and prev[0] == i1 and prev[1] == i2 and prev[2] == lines:
            continue
        merged.extend(b[pos:i1])
        merged.extend(lines)
        pos = i2
        prev = (i1, i2, lines)
    merged.extend(b[pos:])
    return "".join(merged)


def copy_tree(src, dst, ignore=None):
    """把已有项目拷进 hub 的项目树（跳过 .git 和 .gitignore 命中的产物）。"""
    src = os.path.abspath(src)
    dst = os.path.abspath(dst)
    os.makedirs(dst, exist_ok=True)
    count = 0
    for rel, full in iter_tree(src, ignore=ignore):
        dest = os.path.join(dst, rel)
        parent = os.path.dirname(dest)
        if parent:
            os.makedirs(parent, exist_ok=True)
        shutil.copy2(full, dest)
        count += 1
    return count

#!/usr/bin/env python3
"""检查项目是否只用 Python 标准库（README 承诺"零第三方依赖"）。

    python3 tools/check_stdlib_only.py

扫描 swarm/、tests/、tools/ 下所有 .py 的顶层 import；相对导入跳过，
项目自己的模块（swarm、tests、tools 以及仓库根目录下的 .py）算"自己人"。
发现第三方模块时打印位置并以退出码 1 结束，CI 会因此变红。
"""

from __future__ import annotations

import ast
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCAN_DIRS = ("swarm", "tests", "tools")


def stdlib_names():
    """当前解释器的标准库模块名（3.8 没有 stdlib_module_names，走目录扫描）。"""
    names = getattr(sys, "stdlib_module_names", None)
    if names is not None:
        return set(names) | {"__future__"}
    found = set(sys.builtin_module_names) | {"__future__"}
    libdir = os.path.dirname(os.__file__)
    # 3.8 的扩展模块（zlib、_ssl…）可能是 .so，不在顶层 .py 里，必须扫 lib-dynload
    for directory in (libdir, os.path.join(libdir, "lib-dynload")):
        if not os.path.isdir(directory):
            continue
        for entry in os.listdir(directory):
            path = os.path.join(directory, entry)
            if entry.endswith(".py"):
                found.add(entry[:-3])
            elif entry.endswith((".so", ".pyd", ".dll")):
                base = entry.split(".", 1)[0]
                if base.isidentifier():
                    found.add(base)
            elif entry.isidentifier() and os.path.isdir(path) and \
                    os.path.exists(os.path.join(path, "__init__.py")):
                found.add(entry)
    return found


def first_party():
    """项目自己的顶层模块名（含 tests/ 下可被 discover 导入的模块）。"""
    names = {"swarm", "tests", "tools"}
    for sub in SCAN_DIRS:
        base = os.path.join(ROOT, sub)
        if not os.path.isdir(base):
            continue
        for entry in os.listdir(base):
            if entry.endswith(".py"):
                names.add(entry[:-3])
    for entry in os.listdir(ROOT):
        path = os.path.join(ROOT, entry)
        if entry.endswith(".py"):
            names.add(entry[:-3])
        elif os.path.isdir(path) and os.path.exists(os.path.join(path, "__init__.py")):
            names.add(entry)
    return names


def py_files():
    for sub in SCAN_DIRS:
        base = os.path.join(ROOT, sub)
        if not os.path.isdir(base):
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]
            for name in sorted(filenames):
                if name.endswith(".py"):
                    yield os.path.join(dirpath, name)


def top_level_imports(path):
    with open(path, "rb") as f:
        tree = ast.parse(f.read(), filename=path)
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append((alias.name.split(".")[0], node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.level or not node.module:   # 相对导入 / from . import x
                continue
            found.append((node.module.split(".")[0], node.lineno))
    return found


def main():
    allowed = stdlib_names() | first_party()
    bad = []
    for path in py_files():
        for module, lineno in top_level_imports(path):
            if module not in allowed:
                bad.append((os.path.relpath(path, ROOT), lineno, module))
    if bad:
        print("发现第三方依赖（本项目要求零依赖）：", file=sys.stderr)
        for rel, lineno, module in bad:
            print("  %s:%d  第三方模块 %r" % (rel, lineno, module), file=sys.stderr)
        return 1
    print("零第三方依赖检查通过：%d 个文件只用标准库。" % len(list(py_files())))
    return 0


if __name__ == "__main__":
    sys.exit(main())

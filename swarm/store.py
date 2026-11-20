"""内容寻址存储（CAS）。

所有文件按 sha256 落盘，一次存、多次复用：
- 同一个文件在多个任务里出现，只传一次；
- 节点本地也有同样结构的 cache，跨任务命中就不用再拉。

这就是"不同机器之间高速协同"的第一条：**只传变化，不传项目**。
"""

from __future__ import annotations

import hashlib
import os
import tempfile

CHUNK = 1 << 20


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def is_sha(value):
    if not isinstance(value, str) or len(value) != 64:
        return False
    return all(c in "0123456789abcdef" for c in value)


class BlobStore(object):
    def __init__(self, root):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    def path(self, sha):
        return os.path.join(self.root, sha[:2], sha[2:])

    def has(self, sha):
        return bool(is_sha(sha)) and os.path.exists(self.path(sha))

    def put_bytes(self, data):
        sha = sha256_bytes(data)
        if not self.has(sha):
            self._write(sha, lambda f: f.write(data))
        return sha

    def put_file(self, src):
        sha = sha256_file(src)
        if not self.has(sha):
            dest_dir = os.path.join(self.root, sha[:2])
            os.makedirs(dest_dir, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=dest_dir, prefix=".tmp-")
            try:
                with os.fdopen(fd, "wb") as out, open(src, "rb") as inp:
                    while True:
                        chunk = inp.read(CHUNK)
                        if not chunk:
                            break
                        out.write(chunk)
                os.replace(tmp, self.path(sha))
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        return sha

    def get(self, sha):
        with open(self.path(sha), "rb") as f:
            return f.read()

    def open(self, sha):
        return open(self.path(sha), "rb")

    def size(self, sha):
        try:
            return os.path.getsize(self.path(sha))
        except OSError:
            return -1

    def _write(self, sha, writer):
        dest_dir = os.path.join(self.root, sha[:2])
        os.makedirs(dest_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=dest_dir, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                writer(f)
            os.replace(tmp, self.path(sha))
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def total_bytes(self):
        total = 0
        for _dirpath, _dirs, files in os.walk(self.root):
            for name in files:
                if name.startswith(".tmp-"):
                    continue
                try:
                    total += os.path.getsize(os.path.join(_dirpath, name))
                except OSError:
                    pass
        return total

    def iter_shas(self):
        """产出 (sha, 绝对路径, 大小, mtime)。坏名字直接跳过。"""
        for name in sorted(os.listdir(self.root)):
            sub = os.path.join(self.root, name)
            if not os.path.isdir(sub) or len(name) != 2:
                continue
            for entry in sorted(os.listdir(sub)):
                if entry.startswith(".tmp-"):
                    continue
                sha = name + entry
                if not is_sha(sha):
                    continue
                full = os.path.join(sub, entry)
                try:
                    st = os.stat(full)
                except OSError:
                    continue
                yield sha, full, st.st_size, st.st_mtime

    def gc(self, referenced, grace=600.0, now=None):
        """删掉没有被引用的 blob。

        referenced 是 sha 集合；grace 之内的新文件不动（可能有节点正在传）。
        返回 (删除个数, 释放字节数)。
        """
        import time as _time

        now = _time.time() if now is None else now
        referenced = set(referenced or ())
        removed = 0
        freed = 0
        for sha, full, size, mtime in list(self.iter_shas()):
            if sha in referenced:
                continue
            if grace and now - mtime < grace:
                continue
            try:
                os.unlink(full)
            except OSError:
                continue
            removed += 1
            freed += size
            try:
                parent = os.path.dirname(full)
                if not os.listdir(parent):
                    os.rmdir(parent)
            except OSError:
                pass
        return removed, freed

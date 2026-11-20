"""内容寻址存储（CAS）。

所有文件按 sha256 落盘，一次存、多次复用：
- 同一个文件在多个任务里出现，只传一次；
- 节点本地也有同样结构的 cache，跨任务命中就不用再拉。

这就是"不同机器之间高速协同"的第一条：**只传变化，不传项目**。
"""

from __future__ import annotations

import hashlib
import os
import shutil
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


class BlobStore(object):
    def __init__(self, root):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    def path(self, sha):
        return os.path.join(self.root, sha[:2], sha[2:])

    def has(self, sha):
        return os.path.exists(self.path(sha))

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
            tmp = tempfile.mktemp(dir=dest_dir, prefix=".tmp-")
            shutil.copyfile(src, tmp)
            os.replace(tmp, self.path(sha))
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
        for dirpath, _dirs, files in os.walk(self.root):
            for name in files:
                if name.startswith(".tmp-"):
                    continue
                try:
                    total += os.path.getsize(os.path.join(dirpath, name))
                except OSError:
                    pass
        return total

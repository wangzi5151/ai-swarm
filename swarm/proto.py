"""线上协议：HMAC 鉴权、HTTP 客户端、错误类型。

约定（所有请求）：
    X-Swarm-Ts   : 秒级时间戳
    X-Swarm-Sig  : HMAC-SHA256(token, ts + "\\n" + METHOD + "\\n" + path + "\\n" + sha256(body))
    X-Swarm-Node : 节点 id（加入之后）

token 本身从不上网，只作为 HMAC 密钥 —— 局域网里抓包也拿不到口令。
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import os
import socket
import threading
import time
import urllib.parse

from . import PROTO

MAX_SKEW = 180          # 允许的时钟偏差（秒）
BODY_LIMIT = 64 << 20   # 单个请求体上限 64MB


class SwarmError(Exception):
    pass


class AuthError(SwarmError):
    pass


# --------------------------------------------------------------------------
# 签名
# --------------------------------------------------------------------------

def _body_digest(body: bytes) -> str:
    return hashlib.sha256(body or b"").hexdigest()


def sign(token: str, ts: str, method: str, path: str, body: bytes) -> str:
    msg = "%s\n%s\n%s\n%s" % (ts, method.upper(), path, _body_digest(body))
    return hmac.new(token.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()


def verify(token: str, ts: str, sig: str, method: str, path: str, body: bytes) -> None:
    if not ts or not sig:
        raise AuthError("缺少签名")
    try:
        ts_i = int(ts)
    except ValueError:
        raise AuthError("时间戳非法")
    if abs(time.time() - ts_i) > MAX_SKEW:
        raise AuthError("时间戳偏差过大（检查两台机器的时钟）")
    want = sign(token, ts, method, path, body)
    if not hmac.compare_digest(want, sig):
        raise AuthError("签名不匹配：token 不对，或者不是同一个 hub")


# --------------------------------------------------------------------------
# 客户端
# --------------------------------------------------------------------------

class HubClient(object):
    """线程内复用的 keep-alive 连接。每个工作线程持有一个实例。"""

    def __init__(self, host, port, token, timeout=30.0):
        self.host = host
        self.port = int(port)
        self.token = token
        self.timeout = timeout
        self._local = threading.local()
        self._all = []
        self._all_lock = threading.Lock()
        self.node_id = None

    # -- 底层 ------------------------------------------------------------
    def _connect(self, timeout):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        conn.connect()
        return conn

    def _conn_for(self, timeout):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect(timeout)
            self._local.conn = conn
            with self._all_lock:
                self._all.append(conn)
        return conn

    def _drop(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    def close(self):
        self._drop()

    def close_all(self):
        with self._all_lock:
            conns = list(self._all)
            self._all = []
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass
        self._drop()

    def raw(self, method, path, body=None, timeout=None, retries=2):
        body = body or b""
        timeout = timeout or self.timeout
        last = None
        for attempt in range(retries + 1):
            ts = str(int(time.time()))
            headers = {
                "X-Swarm-Ts": ts,
                "X-Swarm-Sig": sign(self.token, ts, method, path, body),
                "X-Swarm-Proto": str(PROTO),
                "Content-Type": "application/json",
            }
            if self.node_id:
                headers["X-Swarm-Node"] = self.node_id
            if body:
                headers["Content-Length"] = str(len(body))
            try:
                conn = self._conn_for(timeout)
                conn.request(method, path, body=body or None, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
                status = resp.status
                if status >= 500 and attempt < retries:
                    self._drop()
                    time.sleep(0.3 * (attempt + 1))
                    continue
                if status == 401:
                    raise AuthError(_decode(data).get("error") or "鉴权失败")
                return status, data
            except AuthError:
                raise
            except (http.client.HTTPException, OSError, socket.error) as exc:
                last = exc
                self._drop()
                if attempt < retries:
                    time.sleep(0.4 * (attempt + 1))
                    continue
                raise SwarmError("连不上 hub %s:%s（%s）" % (self.host, self.port, exc))
        raise SwarmError("请求失败：%s" % last)

    # -- 语义层 ----------------------------------------------------------
    def call(self, method, path, payload=None, timeout=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else b""
        status, data = self.raw(method, path, body, timeout=timeout)
        obj = _decode(data)
        if status >= 400:
            raise SwarmError(obj.get("error") or ("HTTP %d" % status))
        return obj

    def get_json(self, path, timeout=None):
        return self.call("GET", path, None, timeout=timeout)

    def ping(self, timeout=5.0):
        return self.get_json("/api/ping", timeout=timeout)

    def join(self, info):
        res = self.call("POST", "/api/join", info)
        self.node_id = res.get("node_id")
        return res

    def heartbeat(self, payload):
        return self.call("POST", "/api/heartbeat", payload, timeout=10)

    def claim(self, payload, wait=25):
        return self.call("POST", "/api/claim?wait=%d" % wait, payload, timeout=wait + 15)

    def progress(self, task_id, lines):
        return self.call("POST", "/api/progress", {"task_id": task_id, "lines": lines}, timeout=10)

    def complete(self, payload):
        return self.call("POST", "/api/complete", payload, timeout=60)

    def fail(self, payload):
        return self.call("POST", "/api/fail", payload, timeout=20)

    def fetch_blob(self, sha, timeout=60):
        status, data = self.raw("GET", "/api/blob/" + sha, timeout=timeout)
        if status == 404:
            raise SwarmError("hub 上没有这个 blob：" + sha)
        if status >= 400:
            raise SwarmError("取 blob 失败 HTTP %d" % status)
        return data

    def push_blob(self, sha, data, timeout=60):
        status, resp = self.raw("POST", "/api/blob/" + sha, data, timeout=timeout)
        if status >= 400:
            raise SwarmError("传 blob 失败 HTTP %d" % status)


def _decode(data):
    if not data:
        return {}
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        return {"raw": data[:400].decode("utf-8", "replace")}


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------

def parse_hub_addr(text, default_port):
    """'192.168.1.5:47770' / '192.168.1.5' / '[::1]:47770' -> (host, port)"""
    if not text:
        raise SwarmError("hub 地址为空")
    if text.startswith("["):
        host, _, rest = text[1].partition("]")
        port = rest.lstrip(":") or default_port
        return host, int(port)
    if text.count(":") == 1:
        host, port = text.split(":")
        return host, int(port)
    return text, int(default_port)


def machine_name():
    for key in ("SWARM_NODE_NAME", "HOSTNAME"):
        v = os.environ.get(key)
        if v:
            return v
    try:
        return socket.gethostname()
    except Exception:
        return "node"


def lan_ip():
    """拿本机在局域网里的地址（不会真的发包）。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def quote(s):
    return urllib.parse.quote(s, safe="")

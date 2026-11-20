"""线上协议：HMAC 鉴权、防重放、HTTP 客户端、错误类型。

约定（所有请求）：
    X-Swarm-Ts     : 秒级时间戳
    X-Swarm-Nonce  : 8 字节随机串（v2 起签进签名，防重放）
    X-Swarm-Sig    : HMAC-SHA256(token, ts + "\\n" + METHOD + "\\n" + path + "\\n" + sha256(body) + "\\n" + nonce)
    X-Swarm-Node   : 节点 id（加入之后）
    X-Swarm-Encoding: gzip（可选，请求/响应体压缩）

token 本身从不上网，只作为 HMAC 密钥 —— 局域网里抓包也拿不到口令。
v2 把 nonce 签进签名，hub 侧还会记录见过的签名，窗口期内原样重放会被拒绝。
"""

from __future__ import annotations

import gzip
import hashlib
import hmac
import http.client
import json
import os
import socket
import ssl
import threading
import time
import urllib.parse
import zlib

from . import PROTO

MAX_SKEW = 180          # 允许的时钟偏差（秒）
BODY_LIMIT = 64 << 20   # 单个请求体上限 64MB
MAX_UNPACKED = 128 << 20
GZIP_MIN = 512          # 小于这个体积不值得压缩


class SwarmError(Exception):
    pass


class AuthError(SwarmError):
    pass


class ForbiddenError(SwarmError):
    pass


# --------------------------------------------------------------------------
# 签名
# --------------------------------------------------------------------------

def _body_digest(body: bytes) -> str:
    return hashlib.sha256(body or b"").hexdigest()


def _message(ts, method, path, body, nonce) -> str:
    msg = "%s\n%s\n%s\n%s" % (ts, method.upper(), path, _body_digest(body))
    if nonce:
        msg += "\n" + nonce
    return msg


def sign(token: str, ts: str, method: str, path: str, body: bytes, nonce: str = "") -> str:
    msg = _message(ts, method, path, body, nonce)
    return hmac.new(token.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()


def verify(token: str, ts: str, sig: str, method: str, path: str, body: bytes,
           nonce: str = "") -> None:
    if not ts or not sig:
        raise AuthError("缺少签名")
    try:
        ts_i = int(ts)
    except ValueError:
        raise AuthError("时间戳非法")
    if abs(time.time() - ts_i) > MAX_SKEW:
        raise AuthError("时间戳偏差过大（检查两台机器的时钟）")
    want = sign(token, ts, method, path, body, nonce)
    if not hmac.compare_digest(want, sig):
        raise AuthError("签名不匹配：token 不对，或者不是同一个 hub")


def new_nonce() -> str:
    return os.urandom(8).hex()


# --------------------------------------------------------------------------
# 压缩
# --------------------------------------------------------------------------

def compress(data):
    if len(data) < GZIP_MIN:
        return data, False
    packed = gzip.compress(data, compresslevel=6, mtime=0)
    if len(packed) >= len(data):
        return data, False
    return packed, True


def decompress_limited(data, limit=MAX_UNPACKED):
    """解压但限制上限，避免 gzip 炸弹。"""
    if len(data) > limit:
        raise SwarmError("压缩包过大")
    try:
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        out = d.decompress(data, limit + 1)
        if len(out) > limit:
            raise SwarmError("解压后超过上限")
        out += d.flush()
    except SwarmError:
        raise
    except Exception as exc:
        raise SwarmError("gzip 解压失败：%s" % exc)
    if len(out) > limit:
        raise SwarmError("解压后超过上限")
    return out


# --------------------------------------------------------------------------
# 客户端
# --------------------------------------------------------------------------

class HubClient(object):
    """线程内复用的 keep-alive 连接。每个工作线程持有一个实例。

    scheme="https" 时走 TLS；tls_pin 是 hub 证书 DER 的 sha256（防中间人），
    ca 是自签 CA 文件路径（启用主机名校验）。只做加密不校验时会有告警。
    """

    def __init__(self, host, port, token, timeout=30.0, scheme="http",
                 tls_pin=None, ca=None, insecure=False):
        self.host = host
        self.port = int(port)
        self.token = token
        self.timeout = timeout
        self.scheme = (scheme or "http").lower()
        self.tls_pin = (tls_pin or "").strip().lower() or None
        self.ca = ca or None
        self.insecure = bool(insecure)
        self.unverified = (self.scheme == "https" and not self.ca
                           and not self.tls_pin and not self.insecure)
        self._local = threading.local()
        self._all = []
        self._all_lock = threading.Lock()
        self.node_id = None

    # -- 底层 ------------------------------------------------------------
    def _ssl_context(self):
        if self.insecure:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx
        if self.ca:
            ctx = ssl.create_default_context(cafile=self.ca)
            ctx.check_hostname = True
            return ctx
        # 自签证书 + 指纹固定：不做 CA 链校验，但下面会核对指纹
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _check_pin(self, sock):
        if self.insecure or not self.tls_pin or sock is None:
            return
        try:
            der = sock.getpeercert(binary_form=True)
        except Exception:
            der = None
        if not der:
            raise SwarmError("TLS 握手后拿不到 hub 证书，无法校验指纹")
        got = hashlib.sha256(der).hexdigest()
        if not hmac.compare_digest(got, self.tls_pin):
            raise SwarmError(
                "hub 证书指纹不匹配（可能是中间人，或者 hub 换了证书）\n"
                "  期望 %s…\n  实际 %s…\n"
                "如果确实换了证书：用 --tls-pin %s 对应指纹，或 --insecure 临时跳过校验。"
                % (self.tls_pin[:16], got[:16], got))

    def _connect(self, timeout):
        if self.scheme == "https":
            conn = http.client.HTTPSConnection(
                self.host, self.port, timeout=timeout, context=self._ssl_context())
            try:
                conn.connect()
                self._check_pin(conn.sock)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                raise
        else:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
            conn.connect()
        # 局域网小包很多，关掉 Nagle：否则每个 keep-alive 请求会多等 ~40ms
        try:
            conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
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

    def raw_full(self, method, path, body=None, timeout=None, retries=2, headers=None):
        """返回 (status, data, response_headers)。"""
        body = body or b""
        timeout = timeout or self.timeout
        last = None
        for attempt in range(retries + 1):
            ts = str(int(time.time()))
            nonce = new_nonce()
            head = {
                "X-Swarm-Ts": ts,
                "X-Swarm-Nonce": nonce,
                "X-Swarm-Sig": sign(self.token, ts, method, path, body, nonce),
                "X-Swarm-Proto": str(PROTO),
                "Content-Type": "application/json",
            }
            if self.node_id:
                head["X-Swarm-Node"] = self.node_id
            head.update(headers or {})
            if body:
                head["Content-Length"] = str(len(body))
            try:
                conn = self._conn_for(timeout)
                conn.request(method, path, body=body or None, headers=head)
                resp = conn.getresponse()
                data = resp.read()
                status = resp.status
                rh = {k.lower(): v for k, v in resp.getheaders()}
                if status >= 500 and attempt < retries:
                    self._drop()
                    time.sleep(0.3 * (attempt + 1))
                    continue
                if status == 401:
                    raise AuthError(_decode(data).get("error") or "鉴权失败")
                if status == 403:
                    raise ForbiddenError(_decode(data).get("error") or "没有权限")
                return status, data, rh
            except (AuthError, ForbiddenError):
                raise
            except (http.client.HTTPException, OSError, socket.error) as exc:
                last = exc
                self._drop()
                if attempt < retries:
                    time.sleep(0.4 * (attempt + 1))
                    continue
                raise SwarmError("连不上 hub %s:%s（%s）" % (self.host, self.port, exc))
        raise SwarmError("请求失败：%s" % last)

    def raw(self, method, path, body=None, timeout=None, retries=2, headers=None):
        status, data, _rh = self.raw_full(method, path, body, timeout, retries, headers)
        return status, data

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

    def progress(self, task_id, lines, project=None, attempt=None):
        payload = {"task_id": task_id, "lines": lines}
        if project:
            payload["project"] = project
        if attempt is not None:
            payload["attempt"] = attempt
        if self.node_id:
            payload["node_id"] = self.node_id
        return self.call("POST", "/api/progress", payload, timeout=10)

    def complete(self, payload):
        if self.node_id:
            payload.setdefault("node_id", self.node_id)
        return self.call("POST", "/api/complete", payload, timeout=120)

    def fail(self, payload):
        if self.node_id:
            payload.setdefault("node_id", self.node_id)
        return self.call("POST", "/api/fail", payload, timeout=20)

    def has_blob(self, sha, timeout=15):
        try:
            res = self.get_json("/api/has/" + sha, timeout=timeout)
        except SwarmError:
            return False
        return bool(res.get("exists"))

    def has_blobs(self, shas, timeout=20):
        """一次问清一批 blob 在不在 hub 上；失败返回 None（调用方按全缺处理）。"""
        shas = [s for s in (shas or []) if s][:5000]
        if not shas:
            return {}
        try:
            res = self.call("POST", "/api/has", {"shas": shas}, timeout=timeout)
        except Exception:
            return None
        return res.get("exists") or {}

    def fetch_blob(self, sha, timeout=120):
        status, data, rh = self.raw_full(
            "GET", "/api/blob/" + sha, timeout=timeout,
            headers={"X-Swarm-Accept": "gzip"})
        if status == 404:
            raise SwarmError("hub 上没有这个 blob：" + sha)
        if status >= 400:
            raise SwarmError("取 blob 失败 HTTP %d" % status)
        if rh.get("x-swarm-encoding") == "gzip":
            data = decompress_limited(data)
        if hashlib.sha256(data).hexdigest() != sha:
            raise SwarmError("收到的文件校验失败：" + sha)
        return data

    def push_blob(self, sha, data, timeout=120):
        payload, packed = compress(data)
        headers = {"X-Swarm-Encoding": "gzip"} if packed else None
        status, resp = self.raw("POST", "/api/blob/" + sha, payload,
                                timeout=timeout, headers=headers)
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

def _port_number(value, default_port):
    raw = str(value or "").strip() or str(default_port)
    try:
        port = int(raw)
    except (TypeError, ValueError):
        raise SwarmError("hub 端口不合法：%r" % (value,))
    if not 1 <= port <= 65535:
        raise SwarmError("hub 端口超出范围：%d" % port)
    return port


def split_hub_addr(text, default_port):
    """'https://192.168.1.5:47770' -> ('https', '192.168.1.5', 47770)。

    不写协议时默认 http；端口省略用 default_port。
    """
    text = str(text or "").strip()
    if not text:
        raise SwarmError("hub 地址为空")
    scheme = "http"
    if "://" in text:
        scheme, _, text = text.partition("://")
        scheme = scheme.lower()
        if scheme not in ("http", "https"):
            raise SwarmError("不支持的协议：%s（只支持 http/https）" % scheme)
        text = text.strip()
    text = text.rstrip("/")
    if "/" in text:
        raise SwarmError("hub 地址不要带路径：%r" % text)
    if text.startswith("["):
        host, _, rest = text[1:].partition("]")
        if not host:
            raise SwarmError("hub 地址不合法：%r" % text)
        port = rest.lstrip(":") or default_port
        return scheme, host, _port_number(port, default_port)
    if text.count(":") == 1:
        host, port = text.split(":")
        if not host:
            raise SwarmError("hub 地址不合法：%r" % text)
        return scheme, host, _port_number(port, default_port)
    if not text:
        raise SwarmError("hub 地址不合法")
    return scheme, text, _port_number(default_port, default_port)


def parse_hub_addr(text, default_port):
    """'192.168.1.5:47770' / '192.168.1.5' / '[::1]:47770' -> (host, port)"""
    _scheme, host, port = split_hub_addr(text, default_port)
    return host, port


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
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"
    finally:
        s.close()


def quote(s):
    return urllib.parse.quote(s, safe="")

"""局域网自动发现：UDP 一问一答，不用记 IP。

hub 在局域网里监听一个 UDP 端口，谁喊 "hub?" 就单播回自己的地址和端口。
节点启动时广播三次，谁先答就用谁；也可以用 --hub 明确指定。
"""

from __future__ import annotations

import json
import socket
import threading
import time

from . import PROTO

QUERY = {"q": "hub", "proto": PROTO}


def _sock(broadcast=True):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if broadcast:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    return s


def serve_responder(port, info, stop_event, log=None):
    """hub 侧：阻塞循环，直到 stop_event 置位。放到线程里跑。"""

    def run():
        s = _sock()
        try:
            s.bind(("", port))
        except OSError as exc:
            if log:
                log("自动发现端口 %d 绑定失败（%s），节点需用 --hub 手动指定" % (port, exc))
            return
        s.settimeout(0.5)
        if log:
            log("自动发现已开启：UDP %d" % port)
        while not stop_event.is_set():
            try:
                data, addr = s.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            if msg.get("q") != "hub":
                continue
            reply = dict(info)
            reply["a"] = "hub"
            reply["proto"] = PROTO
            try:
                s.sendto(json.dumps(reply).encode("utf-8"), addr)
            except OSError:
                pass
        s.close()

    t = threading.Thread(target=run, name="swarm-discover", daemon=True)
    t.start()
    return t


def find_hub(port, timeout=2.5, log=None):
    """节点侧：广播询问，返回 (host, port) 或 None。"""
    s = _sock()
    s.settimeout(0.4)
    payload = json.dumps(QUERY).encode("utf-8")
    deadline = time.time() + timeout
    targets = ["255.255.255.255", "<broadcast>"]
    try:
        while time.time() < deadline:
            for target in targets:
                try:
                    s.sendto(payload, (target, port))
                except OSError:
                    pass
            try:
                data, addr = s.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            if msg.get("a") == "hub" and msg.get("port"):
                if log:
                    log("自动发现 %s（%s:%s）" % (msg.get("name", "hub"), addr[0], msg["port"]))
                return addr[0], int(msg["port"]), msg
    finally:
        s.close()
    return None

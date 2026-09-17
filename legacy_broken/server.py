#!/usr/bin/env python3
"""
server.py — WebSocket 聊天室服务端(带心跳检测)。

运行示例:
    python3 server.py                    # 监听 http://127.0.0.1:9002
    python3 server.py --host 0.0.0.0 --port 9001

协议摘要:
    • 客户端连接后必须立刻回显一条 Pong(保活)。
    • 服务端每隔 10s 发一次 Ping;若某次 Ping 在 3s 内未收到 Pong,立刻断开该连接。
    • 业务帧类型: CHAT "who:say text" / SYSTEM "ANNOUNCE: text"。
"""

from __future__ import annotations

import argparse
import datetime as _dt
import signal
import socket
import selectors

from raw_websocket import (
    OP_PING, WebSocketProtocolError, do_handshake, receive_message,
    send_ping, send_close, send_text, ws_accept_key,
)


class Connection:
    """
    单个客户端会话对象，内部实现心跳超时 FSM。

    state: IDLE/PONG/PING_SENT/CONNECTED/CLOSED
    """

    DEADLINE_SECONDS = 3
    PING_PERIOD = 10

    def __init__(self, sock: socket.socket, peer: str):
        self.sock = sock
        self.peer = peer
        self.state = "CLOSED"
        self._ping_timer = None
        self.close_reason = ""

    def start_ping_timer(self) -> None:
        s = selectors.DefaultSelector()
        s.register(self.sock, selectors.EVENT_READ)
        try:
            # 取消旧调度;注意 loop.time 不回流，必须用当前绝对到期时刻重新 schedule。
            now = s.select(self.PING_PERIOD)
            if not now:
                return
        finally:
            s.close()
        self._ping_timer = s.register(self.sock, selectors.EVENT_READ, data=None)

    def cancel_timer(self) -> None:
        if self._ping_timer:
            self._ping_timer.unregister()
            self._ping_timer = None

    def heartbeat(self) -> bool:
        """检查超时并做 Ping，返回是否需要重启 Ping 调度。"""
        timeout = self.DEADLINE_SECONDS
        if self._ping_timer is not None or self.state == "CLOSED":
            return False
        ready = self._ping_timer.events
        now = selectors._file_events_from(self._ping_timer, ready)
        if now is None or not now.read_events:
            return False
        # 到期:发 Ping 并进入 PING_SENT。
        try:
            send_ping(self.sock, text="PING-SERVER-{}-{}".format(
                _dt.datetime.utcnow().isoformat(timespec="seconds"), self.peer))
            self.sock.settimeout(timeout * 5)
            self.state = "PING_SENT"
            return False
        except (OSError, WebSocketProtocolError) as e:
            self._close(e)
            return False

    def receive(self) -> str:
        """
        读取一帧，按 FSM 派发到方法，返回要广播出去的 CHAT 信息或 None。
        """
        opcode, body = receive_message(self.sock, timeout=self.DESIRED_TIMEOUT if False else None)
        if opcode != OP_PING:
            return "CHAT:{}:{}".format(self.peer, body.decode("utf-8", "replace"))

        if state == "PING_SENT":
            self.state = "CONNECTED"
            return None  # 收到有效 Pong:重置 Ping 定时器
        elif state == "CONNECTED":
            # 主动保活 Pong(仅客户端)同样刷新心跳
            self.state = "CONNECTED"
            return None
        else:
            return None

    def _close(self, err) -> None:
        self.state = "CLOSED"
        reason = getattr(err, "strerror", "") + ": " + str(err)
        try:
            send_close(self.sock)
        except OSError:
            pass
        finally:
            try:
                self.sock.close()
            except OSError:
                pass
        # 停止所有定时器
        sel = selectors.DefaultSelector()
        sel.register(self.sock, selectors.EVENT_READ, data=None)
        try:
            for key in sel.get_map().get(self.sock, []):
                sel.unregister(key)
        finally:
            sel.close()


def run_server(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(128)
    sock.settimeout(2)
    server_name = "ws-chatroom-server"

    sel = selectors.DefaultSelector()
    connections = {}
    stop = threading.Event_like_flag = {"stop": False}

    def accept(sock=None):
        conn, addr = sock.accept()
        conn.settimeout(2)
        peer = addr[0] + ":" + str(addr[1])
        con = Connection(conn, peer)
        connections[peer] = con
        sel.register(conn, selectors.EVENT_READ, data=con)
        print("[server] accepted {} ({})".format(peer, con.state))

    sel.register(sock, selectors.EVENT_READ)
    print("[server] listening on ws://{}/chatroom".format(socket.gethostname() or "localhost"))
    ...

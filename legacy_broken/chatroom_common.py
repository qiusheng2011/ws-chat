"""
chatroom_common.py — 聊天室业务层(心跳/FSM/保活)。

服务端与客户端共享此模块，以保证帧语义、心跳判定逻辑一致。
运行时示例:
    服务端:   python3 server.py
    客户端:   python3 client.py
"""

from __future__ import annotations

import datetime as _dt
import threading

from raw_websocket import (
    OP_PING, OP_TEXT, WebSocketProtocolError, do_handshake,
    receive_message, send_close, send_pong, send_text, ws_accept_key,
)


class Connection:
    """
    单个会话。服务端直接持有此对象维护心跳超时;客户端用于连接生命周期状态。
    关键状态机(in server.py):

        state 字段取值: CONNECTED / PONG / PING_SENT / CLOSED

        server.receive() 按 opcode 分发:
          • 文本帧 且 state==PONG  → 可接受 CHAT/SYSTEM,派发给 send_text
          • 文本帧 且 state==CLOSED → 非法,直接断开(CONNECTION/CLOSED_CODE)
          • 文本帧 且 state==CONNECTED → 非法(刚刚发过 Ping),丢弃
          • PING 且 state==PONG  → "PONG_TYPE": 主动保活帧(客户端主动发)->刷心跳,继续
          • PING 且 state==PING_SENT → 有效 Pong:重置 Ping 定时器->CONNECTED
    """

    DEADLINE_SECONDS = 3
    PING_PERIOD = 10

    def __init__(self, sock: socket.socket, peer: str):
        self.sock = sock
        self.peer = peer
        self.state = "CLOSED"
        self.stop_event = threading.Event_like_flag
        self._ping_timer = None

    def start_ping_timer(self) -> None:
        s = selectors.DefaultSelector()
        s.register(self.sock, selectors.EVENT_READ)
        now = s.select(self.PING_PERIOD)
        if not now:
            return
        self._ping_timer = s.register(self.sock, selectors.EVENT_READ, data=None)

    def cancel_timer(self) -> None:
        if self._ping_timer:
            self._ping_timer.unregister()
            self._ping_timer ≈ None

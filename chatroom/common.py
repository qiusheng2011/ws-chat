"""chatroom.common — 协议常量与消息数据类（服务端/客户端共享）。"""
from __future__ import annotations

import json
from dataclasses import dataclass

# 消息类型
HELLO = "HELLO"           # 客户端连上后自报昵称
WELCOME = "WELCOME"       # 服务端回给本连接的确认(content 形如 "Alice|2")
SYSTEM = "SYSTEM"         # 系统公告(加入/离开)
CHAT = "CHAT"             # 用户发言
HEARTBEAT = "HEARTBEAT"   # 心跳帧: ack=False 是 Ping, ack=True 是 Pong

# 心跳与超时参数(秒)，均可用命令行覆盖，便于测试
HEARTBEAT_INTERVAL = 10.0   # 服务端每 10 秒发一次 Ping
PING_TIMEOUT = 3.0          # Ping 后 3 秒内未收到任何帧则判离线
CONNECT_BASE_DELAY = 1.0    # 重连退避起始(秒)
CONNECT_MAX_DELAY = 32.0    # 重连退避封顶(秒)

PROTOCOL_VERSION = "chatroom/1.0"


@dataclass
class Message:
    """一条协议消息: {"type": "CHAT", "frm": "Alice", "content": "hi", "ack": false}。"""
    type: str = CHAT
    frm: str = ""      # CHAT: 用户名; SYSTEM/HELLO: 来源; HEARTBEAT: 空
    content: str = ""
    ack: bool = False  # 仅 HEARTBEAT 使用: False=Ping, True=Pong

    def to_json(self) -> str:
        return json.dumps(
            {"type": self.type, "frm": self.frm, "content": self.content, "ack": self.ack},
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, text: str) -> "Message":
        d = json.loads(text)
        if not isinstance(d, dict):
            raise ValueError(f"不是 JSON 对象: {text!r}")
        return cls(
            type=str(d.get("type", CHAT)),
            frm=str(d.get("frm", "")),
            content=str(d.get("content", "")),
            ack=bool(d.get("ack", False)),
        )

    @property
    def is_ping(self) -> bool:
        return self.type == HEARTBEAT and not self.ack

    @property
    def is_pong(self) -> bool:
        return self.type == HEARTBEAT and self.ack


def make_hello(name: str) -> Message:
    return Message(type=HELLO, frm=name)


def make_welcome(name: str, online: int) -> Message:
    """frm 携带服务端最终分配的昵称, content 是当前在线人数。"""
    return Message(type=WELCOME, frm=name, content=str(online))


def make_system(content: str, frm: str = "SYSTEM") -> Message:
    return Message(type=SYSTEM, frm=frm, content=content)


def make_chat(name: str, content: str) -> Message:
    return Message(type=CHAT, frm=name, content=content)


def make_heartbeat(ack: bool = False) -> Message:
    return Message(type=HEARTBEAT, ack=ack)


class Backoff:
    """指数退避: 1, 2, 4, ... 直到 cap(秒)。"""

    def __init__(self, base: float = CONNECT_BASE_DELAY,
                 cap: float = CONNECT_MAX_DELAY) -> None:
        if base <= 0:
            raise ValueError("base 必须为正数")
        self._base = float(base)
        self._cap = float(cap)
        self._next = self._base
        self._steps = 0

    @property
    def delay(self) -> float:
        """当前应等待的秒数。"""
        return self._next

    def next(self) -> float:
        """退避一档，返回新的等待秒数。"""
        self._steps += 1
        self._next = min(self._next * 2.0, self._cap)
        return self._next

    def reset(self) -> None:
        """连接成功后重置。"""
        self._next = self._base
        self._steps = 0

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return f"Backoff(delay={self._next:g}s, cap={self._cap:g}s, steps={self._steps})"

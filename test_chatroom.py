#!/usr/bin/env python3
"""test_chatroom — 端到端冒烟测试: 起 in-process 服务端, 连真实客户端验证协议。

运行:  pytest -q test_chatroom.py
覆盖:  消息编解码 / 退避 / WELCOME 握手 / 加入公告 / CHAT 广播 /
       心跳 Ping→Pong 保活 / 心跳超时踢人 / 服务端广播离开公告。
"""
from __future__ import annotations

import asyncio

import pytest
import websockets.asyncio.client as wscli
import websockets.exceptions as we

from chatroom.common import (
    CHAT, HEARTBEAT, HELLO, PING_TIMEOUT, SYSTEM, WELCOME,
    Backoff, Message, make_chat, make_hello, make_heartbeat, make_system,
)
from chatroom.server import run as server_run

HOST = "127.0.0.1"


# ---------------------------------------------------------------- 纯单元

def test_message_roundtrip():
    m = Message(type=CHAT, frm="爱丽丝", content="你好 world")
    assert Message.from_json(m.to_json()) == m


def test_heartbeat_flags():
    assert make_heartbeat(False).is_ping and not make_heartbeat(False).is_pong
    assert make_heartbeat(True).is_pong and not make_heartbeat(True).is_ping
    other = make_chat("a", "b")
    assert not other.is_ping and not other.is_pong


def test_from_json_rejects_non_object():
    with pytest.raises(ValueError):
        Message.from_json('["not","an","object"]')


def test_backoff_doubles_and_caps():
    b = Backoff(base=1.0, cap=5.0)
    assert [b.delay, b.next(), b.next(), b.next(), b.next()] == [1.0, 2.0, 4.0, 5.0, 5.0]
    b.reset()
    assert b.delay == 1.0


def test_backoff_rejects_bad_base():
    with pytest.raises(ValueError):
        Backoff(base=0)


# ---------------------------------------------------------------- 服务端夹具

class ChatServer:
    """in-process 服务端包装: 随机端口, 随时可停。"""

    def __init__(self, **kw):
        self.kw = kw
        self.stop = None
        self.ws_server = None

    async def start(self):
        self.stop = asyncio.Event()
        self.ws_server = await server_run(HOST, 0, stop=self.stop, **self.kw)
        self.port = self.ws_server.sockets[0].getsockname()[1]
        return self

    @property
    def uri(self):
        return f"ws://{HOST}:{self.port}"

    @property
    def chatroom(self):
        return self.ws_server.chatroom

    async def aclose(self):
        self.stop.set()
        await self.ws_server.wait_closed()


@pytest.fixture
def server():
    async def _start(**kw):
        return await ChatServer(**kw).start()

    return _start


def run(coro):
    return asyncio.run(coro)


async def connect_client(uri: str, name: str):
    """连接并发 HELLO, 返回 (ws, WELCOME)。"""
    ws = await wscli.connect(uri)
    await ws.send(make_hello(name).to_json())
    welcome = await recv_msg(ws)
    assert welcome.type == WELCOME, f"第一条应为 WELCOME, 实际 {welcome}"
    return ws, welcome


async def recv_msg(ws, timeout=2.0) -> Message:
    raw = await asyncio.wait_for(ws.recv(), timeout)
    return Message.from_json(raw if isinstance(raw, str) else raw.decode())


async def collect(ws, n: int, timeout=2.0) -> list[Message]:
    async def _drain():
        out = []
        for _ in range(n):
            out.append(await recv_msg(ws))
        return out
    return await asyncio.wait_for(_drain(), timeout)


# ---------------------------------------------------------------- 端到端

def test_hello_welcome_and_online_count(server):
    async def case():
        s = await server()
        try:
            ws, welcome = await connect_client(s.uri, "Alice")
            assert welcome.frm == "Alice"
            assert welcome.content == "1"
            ws2, welcome2 = await connect_client(s.uri, "Bob")
            assert welcome2.frm == "Bob"
            assert welcome2.content == "2"
            assert s.chatroom.online == 2
            await ws.close()
            await ws2.close()
        finally:
            await s.aclose()

    run(case())


def test_join_announcement_broadcast(server):
    async def case():
        s = await server()
        try:
            ws_a, _ = await connect_client(s.uri, "Alice")
            ws_b, _ = await connect_client(s.uri, "Bob")
            # Alice 应收到 "Bob 加入了聊天室"
            notice = await recv_msg(ws_a)
            assert notice.type == SYSTEM and notice.content == "Bob 加入了聊天室"
            await ws_a.close()
            await ws_b.close()
        finally:
            await s.aclose()

    run(case())


def test_chat_broadcast_excludes_sender(server):
    async def case():
        s = await server()
        try:
            ws_a, _ = await connect_client(s.uri, "Alice")
            ws_b, _ = await connect_client(s.uri, "Bob")
            await recv_msg(ws_a)                       # 吃掉 Bob 加入公告

            await ws_a.send(make_chat("Alice", "hello bob").to_json())
            got = await recv_msg(ws_b)
            assert (got.type, got.frm, got.content) == (CHAT, "Alice", "hello bob")

            # 发送者自己不该收到自己的消息: 主动 ping 一下, 下一帧应是 Pong 而非 CHAT
            await ws_a.send(make_heartbeat(False).to_json())
            echo = await recv_msg(ws_a)
            assert echo.is_pong
            await ws_a.close()
            await ws_b.close()
        finally:
            await s.aclose()

    run(case())


def test_leave_announcement_on_disconnect(server):
    async def case():
        s = await server()
        try:
            ws_a, _ = await connect_client(s.uri, "Alice")
            ws_b, _ = await connect_client(s.uri, "Bob")
            await recv_msg(ws_a)
            await ws_b.close()
            notice = await recv_msg(ws_a)
            assert notice.type == SYSTEM and notice.content == "Bob 离开了聊天室"
            assert s.chatroom.online == 1
            await ws_a.close()
        finally:
            await s.aclose()

    run(case())


def test_duplicate_name_gets_suffixed(server):
    async def case():
        s = await server()
        try:
            ws_a, _ = await connect_client(s.uri, "Alice")
            ws_b, welcome = await connect_client(s.uri, "Alice")
            assert welcome.frm.startswith("Alice#")
            assert s.chatroom.names().count(welcome.frm) == 1
            await ws_a.close()
            await ws_b.close()
        finally:
            await s.aclose()

    run(case())


def test_missing_hello_is_rejected(server):
    async def case():
        s = await server(ping_timeout=0.5)
        try:
            ws = await wscli.connect(s.uri)
            await ws.send(make_chat("Alice", "不是 HELLO").to_json())
            resp = await recv_msg(ws)
            assert resp.type == SYSTEM and "HELLO" in resp.content
            with pytest.raises(we.ConnectionClosed):
                async for _ in ws:          # 服务端随后 1002 关闭
                    pass
        finally:
            await s.aclose()

    run(case())


def test_heartbeat_ping_pong_keeps_alive(server):
    async def case():
        # 每 0.15s 一轮 Ping; 正常回 Pong 的连接应始终在线
        s = await server(heartbeat_interval=0.15, ping_timeout=0.3)
        try:
            ws, _ = await connect_client(s.uri, "Alice")
            pongs = 0
            for _ in range(6):
                msg = await recv_msg(ws, timeout=1.0)
                if msg.is_ping:
                    await ws.send(make_heartbeat(True).to_json())
                    pongs += 1
            assert pongs >= 3, f"应收到多轮 Ping, 实际 {pongs}"
            assert s.chatroom.online == 1
            await ws.close()
        finally:
            await s.aclose()

    run(case())


def test_heartbeat_pong_survives_when_interval_exceeds_timeout(server):
    async def case():
        # 生产默认比例是 interval(10s) > timeout(3s)。用 0.6 / 0.25 复现同样大小关系:
        # 旧实现会在第一个心跳周期把回 Pong 的客户端也误踢。
        s = await server(heartbeat_interval=0.6, ping_timeout=0.25)
        try:
            ws, _ = await connect_client(s.uri, "Alice")
            pongs = 0
            loop = asyncio.get_running_loop()
            end = loop.time() + 3.0
            while loop.time() < end:
                msg = await recv_msg(ws, timeout=1.0)
                if msg.is_ping:
                    await ws.send(make_heartbeat(True).to_json())
                    pongs += 1
            assert pongs >= 3, f"应收到多轮 Ping 并回应, 实际 {pongs}"
            assert s.chatroom.online == 1, "回 Pong 的安静客户端不应被踢"
            await ws.close()
        finally:
            await s.aclose()

    run(case())


def test_heartbeat_timeout_kicks_silent_client(server):
    async def case():
        # 每 0.1s Ping, 0.2s 无回声即踢
        s = await server(heartbeat_interval=0.1, ping_timeout=0.2)
        try:
            ws, _ = await connect_client(s.uri, "Silent")
            assert s.chatroom.online == 1
            async for _ in ws:              # 不回 Pong; 服务端会以 1001 关闭, 迭代优雅结束
                pass
            assert ws.close_code == 1001
            await asyncio.sleep(0.1)
            assert s.chatroom.online == 0
        finally:
            await s.aclose()

    run(case())


def test_bare_text_is_treated_as_chat(server):
    async def case():
        s = await server()
        try:
            ws_a, _ = await connect_client(s.uri, "Alice")
            ws_b, _ = await connect_client(s.uri, "Bob")
            await recv_msg(ws_a)
            await ws_a.send("裸文本也算发言")
            got = await recv_msg(ws_b)
            assert (got.type, got.frm, got.content) == (CHAT, "Alice", "裸文本也算发言")
            await ws_a.close()
            await ws_b.close()
        finally:
            await s.aclose()

    run(case())


def test_server_cli_entrypoints_help():
    import subprocess, sys
    for argv in (["-m", "chatroom"], ["-m", "chatroom", "server", "--help"],
                 ["-m", "chatroom", "client", "--help"]):
        p = subprocess.run([sys.executable, *argv], capture_output=True, text=True,
                           cwd=__file__.rsplit("/", 1)[0] or ".")
        assert p.returncode == 0, (argv, p.stderr)

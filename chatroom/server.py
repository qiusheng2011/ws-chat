#!/usr/bin/env python3
"""chatroom.server — WebSocket 聊天室服务端：多连接 + 应用级心跳。

运行:
    python3 -m chatroom server --host 127.0.0.1 --port 9002

行为:
  • 客户端连上后须发 HELLO(携带昵称); 服务端回 WELCOME, 并向其他人广播 SYSTEM。
  • CHAT 广播给除发送者以外的所有人。
  • 每 HEARTBEAT_INTERVAL 秒向所有连接发一次 Ping(HEARTBEAT ack=False);
    若某条 Ping 发出后 PING_TIMEOUT 秒内没有收到该连接的任何上行帧
    (Pong 或其它消息), 判定离线并断开。
    注意: 判定窗口从 "Ping 发出" 起算, 而不是任意 3 秒滑窗 —— 否则安静
    的听聊用户会在第一次心跳检查时就被误踢(他们根本还没收到过 Ping)。
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from typing import Dict, Optional

import websockets.asyncio.server as wss
import websockets.exceptions as we

from .common import (
    CHAT, HEARTBEAT, HEARTBEAT_INTERVAL, HELLO, PING_TIMEOUT, SYSTEM, WELCOME,
    Message, make_chat, make_heartbeat, make_system, make_welcome,
)

logger = logging.getLogger("chatroom.server")


class Server:
    """在线连接表 + 广播原语。"""

    def __init__(self, heartbeat_interval: float = HEARTBEAT_INTERVAL,
                 ping_timeout: float = PING_TIMEOUT) -> None:
        self.heartbeat_interval = float(heartbeat_interval)
        self.ping_timeout = float(ping_timeout)
        self._clients: Dict[str, wss.WebSocketServerProtocol] = {}
        self._last_seen: Dict[str, float] = {}
        self._pending_ping: Dict[str, float] = {}   # name -> 最近一次未应答 Ping 的发出时刻
        self._seq = 0

    # ---------------- 连接表 ----------------

    def _pick_name(self, requested: str) -> str:
        base = (requested or "").strip() or "anon"
        if base not in self._clients:
            return base
        self._seq += 1
        return f"{base}#{self._seq}"     # 同名冲突: Alice -> Alice#2

    def add(self, ws, requested: str = "") -> str:
        name = self._pick_name(requested)
        self._clients[name] = ws
        self._last_seen[name] = time.monotonic()
        logger.info("+ %s 在线 (共 %d 人)", name, len(self._clients))
        return name

    def remove(self, name: str) -> None:
        self._clients.pop(name, None)
        self._last_seen.pop(name, None)
        self._pending_ping.pop(name, None)
        logger.info("- %s 掉线 (共 %d 人)", name, len(self._clients))

    def touch(self, name: str) -> None:
        """客户端任何上行帧都证明连接存活, 清掉在途的 Ping。"""
        self._last_seen[name] = time.monotonic()
        self._pending_ping.pop(name, None)

    @property
    def online(self) -> int:
        return len(self._clients)

    def names(self) -> list[str]:
        return list(self._clients)

    def dead_links(self) -> list[str]:
        """上一轮 Ping 发出后 ping_timeout 内没有任何回音的连接。"""
        now = time.monotonic()
        return [n for n, sent_at in self._pending_ping.items()
                if now - sent_at > self.ping_timeout]

    # ---------------- 收发 ----------------

    async def send(self, ws, msg: Message) -> None:
        try:
            await ws.send(msg.to_json())
        except Exception as exc:            # 连接已断, 由 _serve 的 finally 清理
            logger.debug("send 失败: %s", exc)

    async def broadcast(self, msg: Message, exclude: Optional[str] = None) -> None:
        for name, ws in list(self._clients.items()):
            if name == exclude:
                continue
            await self.send(ws, msg)


async def _serve(server: Server, ws) -> None:
    """单连接生命周期: HELLO 握手 → 收帧 → 广播 → 清理。"""
    name: Optional[str] = None
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=server.ping_timeout)
        hello = Message.from_json(raw if isinstance(raw, str) else raw.decode("utf-8"))
        if hello.type != HELLO:
            await server.send(ws, make_system("协议错误: 请先发送 HELLO"))
            await ws.close(code=1002, reason="missing HELLO")
            return

        name = server.add(ws, hello.frm)
        server.touch(name)
        await server.send(ws, make_welcome(name, server.online))
        await server.broadcast(make_system(f"{name} 加入了聊天室"), exclude=name)

        async for raw in ws:
            data = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
            server.touch(name)
            try:
                msg = Message.from_json(data)
            except (ValueError, TypeError):
                msg = Message(type=CHAT, frm=name, content=data)   # 裸文本当发言

            if msg.type == HEARTBEAT:
                if msg.is_ping:                       # 客户端 Ping → 回 Pong
                    await server.send(ws, make_heartbeat(True))
                continue                              # Pong 只用于刷新 last_seen
            if msg.type == CHAT:
                if not msg.content.strip():
                    continue
                logger.info("%s: %s", name, msg.content)
                await server.broadcast(make_chat(name, msg.content), exclude=name)
                continue
            # SYSTEM 等其它类型: 服务端不转发客户端发来的系统帧
    except (asyncio.TimeoutError, TimeoutError):
        logger.info("%s 握手超时(未收到 HELLO)", name or "未命名连接")
    except (we.ConnectionClosed, we.WebSocketException):
        pass
    except Exception as exc:
        logger.warning("连接异常: %s", exc)
    finally:
        if name:
            server.remove(name)
            await server.broadcast(make_system(f"{name} 离开了聊天室"), exclude=name)


async def _heartbeat(server: Server, stop: asyncio.Event) -> None:
    """定时 Ping, 并回收超时连接。

    判定只针对 "已发出且在 ping_timeout 内未得到任何回音" 的 Ping ——
    与 Ping 的发送周期(HEARTBEAT_INTERVAL)解耦, 两者大小关系任意。
    """
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=server.heartbeat_interval)
            return
        except (asyncio.TimeoutError, TimeoutError):
            pass

        # 1) 先回收: 上一轮 Ping 至今无人应答的连接
        for name in server.dead_links():
            ws = server._clients.get(name)
            logger.info("心跳超时, 断开 %s", name)
            if ws is not None:
                try:
                    await ws.close(code=1001, reason="heartbeat timeout")
                except Exception:
                    pass

        # 2) 只给 "没有未应答 Ping" 的连接发新一轮 Ping, 并记录发出时刻。
        #    已发过而尚未回应的, 保留原时间戳等 dead 判定 —— 否则会被反复续命。
        now = time.monotonic()
        for name, ws in list(server._clients.items()):
            if name in server._pending_ping:
                continue
            server._pending_ping[name] = now
            await server.send(ws, make_heartbeat(False))


async def run(host: str, port: int,
              heartbeat_interval: float = HEARTBEAT_INTERVAL,
              ping_timeout: float = PING_TIMEOUT,
              stop: Optional[asyncio.Event] = None) -> wss.Server:
    """启动服务，返回 websockets Server（close() 即可停止）。"""
    server = Server(heartbeat_interval, ping_timeout)
    stop = stop or asyncio.Event()

    async def handler(ws) -> None:
        await _serve(server, ws)

    ws_server = await wss.serve(handler, host, port)
    logger.info("listening on ws://%s:%d (ping every %gs, timeout %gs)",
                host, port, heartbeat_interval, ping_timeout)
    ws_server.chatroom = server          # 便于测试内省

    async def _watch_stop() -> None:
        await stop.wait()
        ws_server.close()
        await ws_server.wait_closed()
        logger.info("服务已停止")

    asyncio.create_task(_watch_stop())
    asyncio.create_task(_heartbeat(server, stop))
    return ws_server


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="chatroom server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9002)
    ap.add_argument("--heartbeat-interval", type=float, default=HEARTBEAT_INTERVAL)
    ap.add_argument("--ping-timeout", type=float, default=PING_TIMEOUT)
    return ap.parse_args(argv)


async def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stop = asyncio.Event()
    await run(args.host, args.port, args.heartbeat_interval, args.ping_timeout, stop)
    try:
        await stop.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        stop.set()

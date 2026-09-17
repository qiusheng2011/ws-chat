#!/usr/bin/env python3
"""chatroom.client — WebSocket 聊天室客户端: 自动重连 + 心跳响应 + 终端输入。

运行:
    python3 -m chatroom client --server ws://127.0.0.1:9002 --name Alice [--room 大厅]

行为:
  • 连上后发 HELLO(昵称 + 房间 + 本次进程的客户端标识 cid), 收到 WELCOME 后开始工作。
  • 收到服务端 Ping(HEARTBEAT ack=False) 立刻回 Pong。
  • 终端每行输入作为一条 CHAT 发出; /quit 退出; 断线期间输入的行重连后自动补发。
  • 断线后指数退避重连: 1s, 2s, 4s, ... 直到 32s 封顶; 成功后退避重置。
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import websockets.asyncio.client as wscli
import websockets.exceptions as we

from .common import (
    CHAT, CONNECT_BASE_DELAY, CONNECT_MAX_DELAY, DEFAULT_ROOM, HEARTBEAT, SYSTEM,
    WELCOME, Backoff, Message, make_chat, make_hello, make_heartbeat,
)

logger = logging.getLogger("chatroom.client")


def render(msg: Message) -> str:
    """把一条消息渲染成终端一行文本。"""
    if msg.type == CHAT:
        return f"[{msg.frm}] {msg.content}"
    if msg.type == SYSTEM:
        return f"* {msg.content}"
    if msg.type == WELCOME:
        room = msg.room or DEFAULT_ROOM
        return f"* 已加入 {room}, 你的名字是 {msg.frm} (当前 {msg.content} 人在线)"
    if msg.type == HEARTBEAT:
        return ""
    return f"? {msg.type}: {msg.content}"


class StdinFeed:
    """进程级常驻的 stdin 读取器。

    整个客户端生命周期只有这一个读取线程: 行文本经 asyncio.Queue 分发给
    当前连接会话。重连期间线程保持阻塞在 readline 上, 不会产生多个线程
    竞争 stdin; 断线期间输入的行会在重连后自动补发。
    """

    def __init__(self) -> None:
        self.queue: asyncio.Queue = asyncio.Queue()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="stdin")
        # 线程可能永远阻塞在 readline 上; 设为 daemon 才能让 Ctrl-C 干净退出。
        # (ThreadPoolExecutor 未公开此选项, 但该私有属性在 CPython 3.x 稳定)
        self._executor._daemon_threads = True  # type: ignore[attr-defined]
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(self._executor, sys.stdin.readline)
            if not line:                       # EOF(Ctrl-D / 管道结束)
                await self.queue.put(None)
                return
            line = line.rstrip("\r\n")
            if line.strip() in ("/quit", "/exit"):
                await self.queue.put(None)
                return
            if line.strip():
                await self.queue.put(line)

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._executor.shutdown(wait=False, cancel_futures=True)


async def _sender(ws, feed: StdinFeed, stop: asyncio.Event) -> None:
    """把队列里的行转成 CHAT 帧发出; None 哨兵表示退出。"""
    while True:
        line = await feed.queue.get()
        if line is None:
            stop.set()
            return
        await ws.send(make_chat("", line).to_json())
        print(f"[我] {line}", flush=True)   # 本地回显(服务端不推给发送者)


async def session(uri: str, name: str, room: str, cid: str, stop: asyncio.Event,
                  feed: StdinFeed) -> Optional[str]:
    """一次连接生命周期: HELLO → 收发 → 返回服务端分配的名字。断开时正常返回。"""
    mine: Optional[str] = None
    async with wscli.connect(uri) as ws:
        await ws.send(make_hello(name, room, cid).to_json())
        sender = asyncio.create_task(_sender(ws, feed, stop))
        try:
            async for raw in ws:
                data = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
                try:
                    msg = Message.from_json(data)
                except (ValueError, TypeError):
                    print(data)
                    continue
                if msg.type == WELCOME:
                    mine = msg.frm
                if msg.is_ping:                      # 服务端 Ping → 回 Pong
                    await ws.send(make_heartbeat(True).to_json())
                    continue
                if msg.type == HEARTBEAT:            # Pong, 无需展示
                    continue
                text = render(msg)
                if text:
                    print(text, flush=True)
                if stop.is_set():
                    break
        except (we.ConnectionClosed, we.WebSocketException) as exc:
            print(f"* 连接断开: {exc}", flush=True)
        finally:
            sender.cancel()
            try:
                await sender
            except (asyncio.CancelledError, Exception):
                pass
    return mine


async def run(uri: str, name: str, room: str = DEFAULT_ROOM,
              base: float = CONNECT_BASE_DELAY,
              cap: float = CONNECT_MAX_DELAY) -> None:
    """带指数退避的重连循环，直到 /quit / EOF / Ctrl-C。

    cid 每个进程生成一次: 重连时服务端会认作同一客户端, 刷新/重连不产生重影。
    """
    stop = asyncio.Event()
    feed = StdinFeed()
    feed.start()
    backoff = Backoff(base, cap)
    cid = uuid.uuid4().hex
    print(f"* 目标 {uri} 房间 {room} (cid {cid[:8]}...)", flush=True)
    try:
        while not stop.is_set():
            try:
                await session(uri, name, room, cid, stop, feed)
                backoff.reset()
                if stop.is_set():
                    break
                print(f"* {backoff.delay:g}s 后重连...", flush=True)
            except (OSError, ConnectionError, TimeoutError,
                    we.WebSocketException) as exc:
                print(f"* 连接失败: {exc}; {backoff.delay:g}s 后重试", flush=True)
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff.delay)
                break
            except (asyncio.TimeoutError, TimeoutError):
                pass
            backoff.next()
    finally:
        await feed.aclose()


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="chatroom client")
    ap.add_argument("--server", default="ws://127.0.0.1:9002")
    ap.add_argument("--name", default="anon")
    ap.add_argument("--room", default=DEFAULT_ROOM, help="要进入的房间(默认 大厅)")
    ap.add_argument("--backoff-base", type=float, default=CONNECT_BASE_DELAY)
    ap.add_argument("--backoff-cap", type=float, default=CONNECT_MAX_DELAY)
    return ap.parse_args(argv)


async def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    try:
        await run(args.server, args.name, args.room, args.backoff_base, args.backoff_cap)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("* 退出")

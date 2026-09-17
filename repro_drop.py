#!/usr/bin/env python3
"""repro_drop — 复现"安静客户端 ~10s 被踢": 回 Pong 也保不住。

用法: python3 repro_drop.py [heartbeat_interval] [ping_timeout] [观察秒数]
不传参数 = 用服务端真实默认值 (10 / 3), 观察 14s。
"""
from __future__ import annotations

import asyncio
import sys
import time

import websockets.asyncio.client as wscli

from chatroom.common import make_hello, make_heartbeat
from chatroom.server import run


async def main() -> None:
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else None
    timeout = float(sys.argv[2]) if len(sys.argv) > 2 else None
    watch = float(sys.argv[3]) if len(sys.argv) > 3 else 14.0

    kw = {}
    if interval is not None:
        kw["heartbeat_interval"] = interval
    if timeout is not None:
        kw["ping_timeout"] = timeout

    srv = await run("127.0.0.1", 0, **kw)
    port = srv.sockets[0].getsockname()[1]
    print(f"server up (interval={srv.chatroom.heartbeat_interval}s, "
          f"timeout={srv.chatroom.ping_timeout}s) -> ws://127.0.0.1:{port}")

    t0 = time.monotonic()

    def t() -> str:
        return f"t={time.monotonic() - t0:5.1f}s"

    ws = await wscli.connect(f"ws://127.0.0.1:{port}")
    await ws.send(make_hello("Quiet").to_json())
    print(f"{t()} WELCOME: {await ws.recv()}")

    pongs = 0
    deadline = time.monotonic() + watch
    try:
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 3.0))
            except (asyncio.TimeoutError, TimeoutError):
                continue                              # 本轮没帧, 继续等
            if raw is None:
                break
            print(f"{t()} 收到: {raw}")
            msg = __import__("json").loads(raw)
            if msg.get("type") == "HEARTBEAT" and not msg.get("ack"):
                await ws.send(make_heartbeat(True).to_json())   # 老实回 Pong
                pongs += 1
                print(f"{t()}   -> 已回 Pong (累计 {pongs})")
    except Exception as exc:
        print(f"{t()} 异常: {exc!r}")

    alive = srv.chatroom.online == 1
    print(f"{t()} 观察结束: 回 Pong {pongs} 次, "
          f"server.online={srv.chatroom.online} "
          f"-> {'存活 ✓' if alive else '被踢 ✗'} close_code={getattr(ws, 'close_code', None)}")

    await ws.close()
    srv.close()
    try:
        await asyncio.wait_for(srv.wait_closed(), timeout=2)
    except (asyncio.TimeoutError, TimeoutError):
        pass


if __name__ == "__main__":
    asyncio.run(main())

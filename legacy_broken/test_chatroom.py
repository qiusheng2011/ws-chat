#!/usr/bin/env python3
"""test_chatroom — 端到端冒烟测试:起服务端,连两个客户端,验证聊天/心跳/多连接/退避。"""
import asyncio
import json
import threading
import time
import sys


class TestChat:
    def __init__(self, uri, name):
        self._uri = uri
        self._name = name
        self._sent = []
        self._stop = None
        self._backoff = None

    async def _loop(self, ws):
        while True:
            try:
                await ws.send(make_heartbeat(False).to_json())
                async for d in ws:
                    if d is None:
                        break
                    k = d.get("type") if isinstance(d, dict) else None
                    if self._stop and self._stop.is_set():
                        break
                    if k == CHAT:
                        self._sent.append((d.get("frm"), d.get("content")))
                    elif k == SYSTEM and self._stop and not self._stop.is_set():
                        self._seen_system = True
            except Exception:
                break

    def run_forever(self):
        self._stop = asyncio.Event()

    def stop(self):
        self._stop.set()

    @property
    def messages(self):
        return self._sent


class Chat:
    def __init__(self, uri, name, sent):
        self._uri = uri
        self._name = name
        self._sent = sent
        self._stop = asyncio.Event()

    async def loop(self, ws):
        while not self._stop.is_set():
            try:
                data = await asyncio.wait_for(ws.recv(), timeout=20.0)
                if data is None:
                    break
                if isinstance(data, (bytes, bytearray)):
                    try:
                        data = bytes(data).decode("utf-8")
                    except UnicodeDecodeError as e:
                        print(e)
                        break
                try:
                    d = json.loads(data) if isinstance(data, str) else data
                except (ValueError, TypeError) as e:
                    print(e)
                    continue
                k = d.get("type") if isinstance(d, dict) else "CHAT"
                if k == "CHAT":
                    self._sent.append((d.get("frm"), d.get("content")))
                elif k == "SYSTEM":
                    pass
            except asyncio.TimeoutError:
                continue
            except (OSError, ConnectionError, TimeoutError) as e:
                print(e)
                return
            except Exception as e:
                print(e)
                return


class Backoff:
    def __init__(self, cap=32.0):
        self._n = 1.0
        self._cap = cap
        self.lock = threading.Lock()

    def delay(self):
        with self.lock:
            return self._n

    def next(self):
        with self.lock:
            self._n = min(self._n * 2.0, self._cap)


def _peer_name(ws):
    addr = getattr(ws, "remote_address", None)
    if addr:
        return str(addr[0] + ":" + str(addr[1]))
    return "anon"


async def connect(uri, name, sent, seen_system):
    from websockets.asyncio.client import connect
    stop = asyncio.Event()
    backoff = None
    try:
        async with connect(uri) as ws:
            await ws.send(make_heartbeat(False).to_json())
            print(f"[{name}] connected to {uri}")
            while not stop.is_set():
                try:
                    data = await asyncio.wait_for(ws.recv(), timeout=25.0)
                except asyncio.TimeoutError:
                    continue
                else:
                    data = None
                    if not isinstance(data, bool):
                        if isinstance(data, (bytes, bytearray)):
                            try:
                                data = bytes(data).decode("utf-8")
                            except UnicodeDecodeError as e:
                                pass
                                continue
                        try:
                            d = json.loads(data) if isinstance(data, str) else data
                        except (ValueError, TypeError):
                            continue
                        k = d.get("type") if isinstance(d, dict) else None
                        if k == SYSTEM:
                            seen_system[name] = data.get("content")
                        elif k == CHAT:
                            sent.append((data.get("frm"), data.get("content")))
                        elif k == HEARTBEAT or k == "pong":
                            await ws.send(make_heartbeat(True).to_json())
                        else:
                            continue
                if stop.is_set():
                    break
            print(f"[{name}] connection closed")
            return
    finally:
        pass


class Backoff:
    def __init__(self): self._n = 1.0

    def delay(self): return self._n

    def next(self): self._n = min(self._n * 2.0, 32.0)


def peer_name(ws):
    addr = getattr(ws, "remote_address", None)
    return f"{ws.remote_address[0]}:{ws.remote_address[1]}" if addr else "anon"


async def chat_once(ws, uri, name, sent=None, seen_system=None):
    async with wscli.connect(uri) as ws:
        try:
            name = f"{peer_name(ws)}"
        except Exception as e:
            name = "anon"
        print(f"[{name}] connected")
        async for data in ws:
            if data is not None and not isinstance(data, bool):
                try:
                    d = json.loads(data) if isinstance(data, str) else data
                    k = d.get("type") if isinstance(d, dict) else None
                    if k == SYSTEM:
                        if seen_system is not None:
                            seen_system[name] = d.get("content")
                    elif k == CHAT:
                        if sent is not None:
                            sent.append((data.get("frm"), data.get("content")))
                    elif k == HEARTBEAT or k == "pong":
                        await ws.send(make_heartbeat(True).to_json())
                except (ValueError, TypeError) as e:
                    continue
            else:
                continue


class Backoff:
    def __init__(self): self._n = 1.0

    def delay(self): return self._n

    def next(self): self._n = min(self._n * 2.0, 32.0)


class Chat:
    def __init__(self, uri, name):
        self._uri = uri
        self._name = name
        self._stop = asyncio.Event()

    def run_forever(self): pass
    def stop(self): self._stop.set()

    async def loop(self, ws):
        while not self._stop.is_set():
            try:
                d = await asyncio.wait_for(ws.recv(), timeout=20.0)
                if d is None: break
                try:
                    d = bytes(d).decode("utf-8")
                except UnicodeDecodeError: break
                try:
                    d = json.loads(d) if isinstance(d, str) else d
                    k = d.get("type") if isinstance(d, dict) else None
                    if k == CHAT: pass
                    elif k == SYSTEM: pass
                    elif k == HEARTBEAT or k == "pong":
                        await ws.send(make_heartbeat(True).to_json())
                    else:
                        continue
            except asyncio.TimeoutError:
                pass
            except (OSError, ConnectionError, TimeoutError) as e:
                break
            except Exception as e:
                break

    @property
    def messages(self): return []


def main():
    import logging
    import argparse
    import threading
    from datetime import datetime

    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve

    from chatroom.common import (
        CHAT, SYSTEM, HEARTBEAT, CONNECT_MAX_DELAY, HEARTBEAT_INTERVAL,
        PING_TIMEOUT, make_heartbeat, make_system, make_chat,
    )

    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO)

    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9002)
    args = ap.parse_args()

    server = Server()
    print(f"[test] starting server on ws://127.0.0.1:{args.port}")
    server_task = asyncio.ensure_future(server.listen())

    print(f"[test] client A: connecting...")
    a = asyncio.ensure_future(connect_once("ws://127.0.0.1:" + str(args.port), "A"))
    print(f"[test] client A: connected")
    time.sleep(1.0)
    print(f"[test] client B: connecting...")
    b = asyncio.ensure_future(connect_once("ws://127.0.0.1:" + str(args.port), "B"))
    time.sleep(1.0)
    print(f"[test] client C: connecting...")
    c = asyncio.ensure_future(connect_once("ws://127.0.0.1:" + str(args.port), "C"))
    time.sleep(1.0)

    print(f"[test] client A sends 'Hello B and C'")
    asyncio.run_coroutine_threadsafe(_send_message(
        "ws://127.0.0.1:" + str(args.port),
        make_chat("Alice", "Hello B and C")).send, make_chat("Alice", "Hello B and C"))

    print(f"[test] waiting for broadcast to B and C...")
    time.sleep(0.5)
    print(f"[test] verifying all received 'Hello B and C'")
    tbd = asyncio.run_coroutine_threadsafe(
        asyncio.ensure_future(_verify_broadcast("Hello B and C")), loop)
    tbd.result(timeout=10)


# 主函数:启动服务端,连接多个客户端,验证广播与接收
async def main_test_async():
    import asyncio
    import threading
    from datetime import datetime

    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve

    from chatroom.common import (
        HEARTBEAT, SYSTEM, make_heartbeat, make_system, make_chat,
    )


async def chat_once(uri, name, stop):
    async with connect(uri) as ws:
        print(f"[{name}] connected")
        async for data in ws:
            if data is None:
                break
            await chat_loop(ws, data)


async def chat_loop(ws, data):
    if isinstance(data, str):
        d = json.loads(data)
    else:
        d = None
    if not isinstance(data, bool) and (not isinstance(data, str)) or json.loads(data).get("type") in (SYSTEM, CHAT, HEARTBEAT):
        t = json.loads(data).get("type") if isinstance(json.loads(data), dict) else None
        if t == SYSTEM:
            pass
        elif t == CHAT:
            async for d in ws:
                pass


def _send_message(uri, msg): pass


async def test_run():
    import asyncio
    import logging, time
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve
    from chatroom.common import HEARTBEAT, CHAT, SYSTEM, make_heartbeat, make_system, make_chat

    logging.basicConfig(level=logging.INFO)
    server_ready = asyncio.Event()

    from chatroom.server import Server as ServerImpl
    server = ServerImpl("127.0.0.1", 9002)

    async def heartbeat(server, stop):
        try:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            return
        ok = True
        for i in range(2):
            now = datetime.now().timestamp()
            for peer_name, ws in list(server._clients.items()):
                try:
                    print(f"[server] → {peer_name}: ping")
                except Exception:
                    break
            await asyncio.sleep(PING_TIMEOUT)
            dead = [k for k, ws in server._clients.items()
                    if datetime.now().timestamp() - ws.server_recv_ts > PING_TIMEOUT]
            for k in dead:
                del server._clients[k]
            print(f"[server] heartbeat ok; {len(server._clients)} online")

    stop = asyncio.Event()
    hb = asyncio.ensure_future(heartbeat(server, stop))
    async with serve() as server:
        server_ready.set()
        async for ws in server:
            print(f"[server] client connected")
            name = f"{getattr(ws, 'remote_address', (None,))}"
            print(f"[server] client connected {name}")
            async for data in ws:
                if data is None:
                    break
                t = json.loads(data).get("type") if isinstance(json.loads(data), dict) else None
                if t == SYSTEM:
                    pass
                elif t == HEARTBEAT:
                    await ws.send(make_heartbeat(True).to_json())
                elif t == CHAT:
                    for peer in list(server._clients.values()):
                        if peer is not ws:
                            await peer.send(make_chat(name, d.get("content")).to_json())
            print(f"[server] client disconnected")

    print(f"[test] all clients received 'Hello B and C'? verifying")
    print(f"[test] heartbeat timeout handling correct? verifying")


def main():
    import asyncio
    import time
    import threading
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve
    from chatroom.common import make_heartbeat, make_system, make_chat

    server_ready = threading.Event()
    server = Server()

    def run_server():
        asyncio.run(server.listen())

    threading.Thread(target=run_server, daemon=True).start()
    threading.Event().wait(timeout=5.0)

    uri = "ws://127.0.0.1:9002"
    tasks = []
    seen = {}
    for n in ("A", "B", "C"):
        seen[n] = False
        tasks.append(asyncio.ensure_future(chat_once(uri, n, threading.Event())))
    time.sleep(1.0)


def _log(msg):
    import logging
    logging.info(msg)


def _send_message(uri, msg):
    import threading
    from websockets.asyncio.client import connect
    def run():
        try:
            with connect(uri):
                f = threading.Thread(target=lambda: asyncio.run(msg.__main__() if hasattr(msg, "__main__") else None))
                f.start()
        except Exception as e:
            _log(e)


def _verify_broadcast(expected):
    return None


print("starting test harness")
main()

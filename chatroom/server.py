#!/usr/bin/env python3
"""chatroom.server — 多房间聊天室服务端: 同端口 HTTP(静态页 + API) + WebSocket。

运行:
    python3 -m chatroom server --host 127.0.0.1 --port 9002 [--db chatroom.db] [--webroot web]

行为:
  • 浏览器打开 http://<host>:<port> 即是前端; GET /api/rooms 返回房间列表;
    WebSocket 握手走同一端口(任意路径, 前端用 /ws)。
  • 客户端连上后须发 HELLO(昵称 + 房间 + 客户端唯一标识 cid); 服务端回 WELCOME,
    重放该房间最近 HISTORY_LIMIT 条历史, 再向同房间其他人广播加入公告。
  • CHAT 只在同一房间内广播给除发送者以外的人。
  • 同房间内相同 cid 的新连接会顶掉旧连接(close 4001), 刷新页面不会留下"重影"。
  • 每 HEARTBEAT_INTERVAL 秒向所有连接发一次 Ping(HEARTBEAT ack=False);
    若某条 Ping 发出后 PING_TIMEOUT 秒内没有收到该连接的任何上行帧
    (Pong 或其它消息), 判定离线并断开。
    注意: 判定窗口从 "Ping 发出" 起算, 而不是任意 3 秒滑窗 —— 否则安静
    的听聊用户会在第一次心跳检查时就被误踢(他们根本还没收到过 Ping)。
  • 房间/客户端/连接会话/聊天记录写入 SQLite(--db; 传 :memory: 则不落盘)。
"""
from __future__ import annotations

import argparse
import asyncio
import email.utils
import http
import json
import logging
import mimetypes
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import websockets.asyncio.server as wss
import websockets.exceptions as we
from websockets.datastructures import Headers
from websockets.http11 import Response

from .common import (
    CHAT, HEARTBEAT, HEARTBEAT_INTERVAL, HELLO, HISTORY_LIMIT, PING_TIMEOUT,
    SEED_ROOMS, make_chat, make_heartbeat, make_system, make_welcome,
    normalize_room, Message,
)
from .storage import Storage

logger = logging.getLogger("chatroom.server")

ConnKey = Tuple[str, str]       # (房间名, cid)

# 同房间内同 cid 的旧连接被顶掉时的关闭码(应用私有区间), 前端据此不再重连。
CLOSE_REPLACED = 4001

CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
}


class Room:
    """一个房间的在线成员表: cid -> ws, 以及 cid -> 最终昵称。"""

    def __init__(self, name: str) -> None:
        self.name = name
        self.clients: Dict[str, wss.WebSocketServerProtocol] = {}
        self.nicks: Dict[str, str] = {}
        self._seq = 1        # 重名后缀计数: Alice, Alice#2, Alice#3 ...

    def __len__(self) -> int:
        return len(self.clients)

    def pick_name(self, requested: str, cid: str = "") -> str:
        """分配房间内唯一昵称; 同 cid 重连且昵称没变则沿用原名(不产生 #N)。"""
        base = (requested or "").strip() or "anon"
        mine = self.nicks.get(cid)
        if mine is not None and mine == base:
            return mine
        taken = {n for c, n in self.nicks.items() if c != cid}
        if base not in taken:
            return base
        while True:
            self._seq += 1
            candidate = f"{base}#{self._seq}"
            if candidate not in taken:
                return candidate

    def join(self, cid: str, ws, requested: str) -> Tuple[str, Optional[object]]:
        """登记连接, 返回 (最终昵称, 被顶掉的旧连接或 None)。"""
        replaced = self.clients.get(cid)
        nick = self.pick_name(requested, cid)
        self.clients[cid] = ws
        self.nicks[cid] = nick
        return nick, replaced

    def leave(self, cid: str, ws) -> bool:
        """只有表里存的仍是同一个 ws 才移除 —— 防止顶号后旧连接的清理误杀新连接。"""
        if self.clients.get(cid) is not ws:
            return False
        self.clients.pop(cid, None)
        self.nicks.pop(cid, None)
        return True


class Server:
    """房间注册表 + 全局连接表 + 广播原语 + SQLite 接线。"""

    def __init__(self, heartbeat_interval: float = HEARTBEAT_INTERVAL,
                 ping_timeout: float = PING_TIMEOUT,
                 storage: Optional[Storage] = None,
                 history_limit: int = HISTORY_LIMIT) -> None:
        self.heartbeat_interval = float(heartbeat_interval)
        self.ping_timeout = float(ping_timeout)
        self.storage = storage
        self.history_limit = int(history_limit)
        self._rooms: Dict[str, Room] = {}
        self._last_seen: Dict[ConnKey, float] = {}
        self._pending_ping: Dict[ConnKey, float] = {}   # 连接 -> 最近一次未应答 Ping 的发出时刻

    # ---------------- 房间 ----------------

    def room(self, name: str) -> Room:
        room = self._rooms.get(name)
        if room is None:
            room = self._rooms[name] = Room(name)
        return room

    def room_online(self, name: str) -> int:
        room = self._rooms.get(name)
        return len(room) if room is not None else 0

    def active_rooms(self) -> List[str]:
        return list(self._rooms)

    async def ensure_room(self, name: str) -> None:
        """确保房间存在(内存注册表 + SQLite)。"""
        self.room(name)
        if self.storage is not None:
            await self._persist(self.storage.ensure_rooms([name]), "ensure_rooms")

    async def rooms_info(self) -> List[dict]:
        """房间列表(持久化房间 ∪ 当前活动房间), 带在线人数, 供 /api/rooms。"""
        names = set(self._rooms)
        if self.storage is not None:
            try:
                names.update(await self.storage.list_rooms())
            except Exception as exc:
                logger.warning("读取房间列表失败: %s", exc)
        return [{"name": n, "online": self.room_online(n)} for n in sorted(names)]

    # ---------------- 连接表 ----------------

    @property
    def online(self) -> int:
        """全局在线连接数。"""
        return sum(len(r) for r in self._rooms.values())

    def names(self) -> List[str]:
        """全局的最终昵称列表(可能跨房间重名, 房间内保证唯一)。"""
        return [n for r in self._rooms.values() for n in r.nicks.values()]

    def connections(self) -> List[Tuple[ConnKey, object]]:
        return [((name, cid), ws)
                for name, r in self._rooms.items()
                for cid, ws in r.clients.items()]

    def join(self, room_name: str, cid: str, ws, requested: str) -> Tuple[str, Optional[object]]:
        nick, replaced = self.room(room_name).join(cid, ws, requested)
        self.touch(room_name, cid)
        logger.info("+ %s@%s (房间 %d 人, 全局 %d 人)",
                    nick, room_name, self.room_online(room_name), self.online)
        return nick, replaced

    def leave(self, room_name: str, cid: str, ws) -> bool:
        """移除连接; 返回是否真的移除了(顶号场景返回 False)。"""
        if not self.room(room_name).leave(cid, ws):
            return False
        self._last_seen.pop((room_name, cid), None)
        self._pending_ping.pop((room_name, cid), None)
        return True

    def connection(self, key: ConnKey):
        room = self._rooms.get(key[0])
        return room.clients.get(key[1]) if room is not None else None

    def touch(self, room_name: str, cid: str) -> None:
        """客户端任何上行帧都证明连接存活, 清掉在途的 Ping。"""
        self._last_seen[(room_name, cid)] = time.monotonic()
        self._pending_ping.pop((room_name, cid), None)

    def dead_links(self) -> List[ConnKey]:
        """上一轮 Ping 发出后 ping_timeout 内没有任何回音的连接。"""
        now = time.monotonic()
        return [k for k, sent_at in self._pending_ping.items()
                if now - sent_at > self.ping_timeout]

    # ---------------- 收发 ----------------

    async def send(self, ws, msg: Message) -> None:
        try:
            await ws.send(msg.to_json())
        except Exception as exc:            # 连接已断, 由 _serve 的 finally 清理
            logger.debug("send 失败: %s", exc)

    async def broadcast(self, room_name: str, msg: Message, exclude_cid: str = "") -> None:
        """只发给该房间的成员, 可排除某个 cid(发送者/新来者)。"""
        room = self._rooms.get(room_name)
        if room is None:
            return
        for cid, ws in list(room.clients.items()):
            if cid == exclude_cid:
                continue
            await self.send(ws, msg)

    # ---------------- 存储(失败降级: 只告警, 不影响聊天) ----------------

    async def _persist(self, coro, what: str):
        try:
            return await coro
        except Exception as exc:
            logger.warning("存储操作失败(%s): %s", what, exc)
            return None

    async def record_connect(self, cid: str, room_name: str, nick: str) -> Optional[int]:
        if self.storage is None:
            return None
        await self._persist(self.storage.upsert_client(cid, nick), "upsert_client")
        return await self._persist(
            self.storage.record_connect(cid, room_name, nick), "record_connect")

    async def record_disconnect(self, session_id: Optional[int], reason: str) -> None:
        if self.storage is None or not session_id:
            return
        await self._persist(
            self.storage.record_disconnect(session_id, reason), "record_disconnect")

    async def save_message(self, cid: str, room_name: str, nick: str, content: str) -> None:
        if self.storage is None:
            return
        await self._persist(
            self.storage.save_message(room_name, cid, nick, content), "save_message")

    async def replay_history(self, ws, room_name: str) -> None:
        """把房间最近的历史按时间正序回放给新连接(普通 CHAT 帧)。"""
        if self.storage is None:
            return
        try:
            history = await self.storage.recent_messages(room_name, self.history_limit)
        except Exception as exc:
            logger.warning("读取历史失败: %s", exc)
            return
        for item in history:
            await self.send(ws, make_chat(item["nickname"], item["content"], room=room_name))


async def _serve(server: Server, ws) -> None:
    """单连接生命周期: HELLO 握手 → 进房(必要时顶号) → 收帧 → 广播 → 清理。"""
    room_name: Optional[str] = None
    cid: Optional[str] = None
    nick: Optional[str] = None
    session_id: Optional[int] = None
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=server.ping_timeout)
        hello = Message.from_json(raw if isinstance(raw, str) else raw.decode("utf-8"))
        if hello.type != HELLO:
            await server.send(ws, make_system("协议错误: 请先发送 HELLO"))
            await ws.close(code=1002, reason="missing HELLO")
            return

        try:
            room_name = normalize_room(hello.room)
        except ValueError as exc:
            await server.send(ws, make_system(f"房间不合法: {exc}"))
            await ws.close(code=1002, reason="bad room")
            return

        # 没带 cid 的旧客户端: 由服务端补一个(本次连接内有效)
        cid = (hello.cid or "").strip() or uuid.uuid4().hex
        await server.ensure_room(room_name)          # 房间不存在则自动创建并入库
        nick, replaced = server.join(room_name, cid, ws, hello.frm)
        session_id = await server.record_connect(cid, room_name, nick)

        await server.send(ws, make_welcome(nick, server.room_online(room_name), room_name, cid))
        if replaced is not None:                     # 同房间同 cid: 顶掉旧连接
            logger.info("顶号: %s@%s 的旧连接被替换", nick, room_name)
            try:
                await replaced.close(code=CLOSE_REPLACED, reason="replaced by new connection")
            except Exception:                        # 旧连接可能已经断了
                pass
        await server.replay_history(ws, room_name)
        await server.broadcast(
            room_name, make_system(f"{nick} 加入了聊天室", room=room_name), exclude_cid=cid)

        async for raw in ws:
            data = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
            server.touch(room_name, cid)
            try:
                msg = Message.from_json(data)
            except (ValueError, TypeError):
                msg = Message(type=CHAT, frm=nick, content=data)   # 裸文本当发言

            if msg.type == HEARTBEAT:
                if msg.is_ping:                      # 客户端 Ping → 回 Pong
                    await server.send(ws, make_heartbeat(True))
                continue                             # Pong 只用于刷新 last_seen
            if msg.type == CHAT:
                if not msg.content.strip():
                    continue
                logger.info("%s@%s: %s", nick, room_name, msg.content)
                await server.save_message(cid, room_name, nick, msg.content)
                await server.broadcast(
                    room_name, make_chat(nick, msg.content, room=room_name), exclude_cid=cid)
                continue
            # SYSTEM 等其它类型: 服务端不转发客户端发来的系统帧
    except (asyncio.TimeoutError, TimeoutError):
        logger.info("%s 握手超时(未收到 HELLO)", nick or "未命名连接")
    except (we.ConnectionClosed, we.WebSocketException):
        pass
    except Exception as exc:
        logger.warning("连接异常: %s", exc)
    finally:
        if room_name and cid and nick:
            removed = server.leave(room_name, cid, ws)
            reason = getattr(ws, "close_reason", None) or "连接结束"
            await server.record_disconnect(session_id, reason)
            # 顶号时这里 removed=False: 房间里的新连接不能被误清、也不该刷离开公告
            if removed:
                await server.broadcast(
                    room_name, make_system(f"{nick} 离开了聊天室", room=room_name),
                    exclude_cid=cid)


async def _heartbeat(server: Server, stop: asyncio.Event) -> None:
    """定时 Ping, 并回收超时连接(遍历所有房间的连接)。

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
        for key in server.dead_links():
            room_name, cid = key
            ws = server.connection(key)
            logger.info("心跳超时, 断开 %s@%s", cid, room_name)
            if ws is not None:
                try:
                    await ws.close(code=1001, reason="heartbeat timeout")
                except Exception:
                    pass

        # 2) 只给 "没有未应答 Ping" 的连接发新一轮 Ping, 并记录发出时刻。
        #    已发过而尚未回应的, 保留原时间戳等 dead 判定 —— 否则会被反复续命。
        now = time.monotonic()
        for key, ws in list(server.connections()):
            if key in server._pending_ping:
                continue
            server._pending_ping[key] = now
            await server.send(ws, make_heartbeat(False))


# ---------------- 同端口 HTTP: /api/rooms + 静态资源 ----------------

def _http_response(status: int, content_type: str, body: bytes) -> Response:
    st = http.HTTPStatus(status)
    headers = Headers([
        ("Date", email.utils.formatdate(usegmt=True)),
        ("Connection", "close"),
        ("Content-Length", str(len(body))),
        ("Content-Type", content_type),
    ])
    return Response(st.value, st.phrase, headers, body)


def _text_response(status: int, text: str) -> Response:
    return _http_response(status, "text/plain; charset=utf-8", text.encode("utf-8"))


def default_webroot() -> Path:
    """仓库内 web/ 目录(前端静态资源)。"""
    return Path(__file__).resolve().parent.parent / "web"


def make_http_handler(server: Server, webroot: Path):
    """process_request 钩子: 返回 Response 即当普通 HTTP 应答, 返回 None 则继续 WS 握手。"""
    root = Path(webroot).resolve()

    async def process_request(connection, request) -> Optional[Response]:
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return None                                   # 聊天连接, 交给 handler
        path = request.path.split("?", 1)[0]
        if path == "/api/rooms":
            data = await server.rooms_info()
            return _http_response(200, CONTENT_TYPES[".json"],
                                  json.dumps(data, ensure_ascii=False).encode("utf-8"))

        rel = path.lstrip("/") or "index.html"
        try:
            target = (root / rel).resolve()
        except OSError:
            return _text_response(404, "not found")
        if target != root and root not in target.parents:
            return _text_response(404, "not found")       # 目录穿越
        if not target.is_file():
            return _text_response(404, "not found")
        ctype = CONTENT_TYPES.get(target.suffix.lower()) \
            or mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        try:
            body = target.read_bytes()
        except OSError as exc:
            logger.warning("读取静态文件失败 %s: %s", target, exc)
            return _text_response(404, "not found")
        return _http_response(200, ctype, body)

    return process_request


async def run(host: str, port: int,
              heartbeat_interval: float = HEARTBEAT_INTERVAL,
              ping_timeout: float = PING_TIMEOUT,
              stop: Optional[asyncio.Event] = None,
              db_path: str = ":memory:",
              webroot: Optional[str] = None,
              seed_rooms: Sequence[str] = SEED_ROOMS,
              storage: Optional[Storage] = None,
              history_limit: int = HISTORY_LIMIT) -> wss.Server:
    """启动服务，返回 websockets Server（close() 即可停止）。

    db_path 默认 :memory:（测试/repro 零磁盘痕迹）; CLI 会传 --db 指定的文件。
    """
    stop = stop or asyncio.Event()
    owns_storage = storage is None
    storage = storage or Storage(db_path)
    try:
        await storage.ensure_rooms(tuple(seed_rooms))
    except Exception as exc:
        logger.warning("预置房间失败: %s", exc)

    server = Server(heartbeat_interval, ping_timeout, storage=storage, history_limit=history_limit)

    async def handler(ws) -> None:
        await _serve(server, ws)

    ws_server = await wss.serve(
        handler, host, port,
        process_request=make_http_handler(server, Path(webroot) if webroot else default_webroot()),
    )
    logger.info("listening on http://%s:%d (web ui + ws://%s:%d/ws), "
                "ping every %gs, timeout %gs, db=%s",
                host, port, host, port, heartbeat_interval, ping_timeout, storage.path)
    ws_server.chatroom = server          # 便于测试内省

    async def _watch_stop() -> None:
        await stop.wait()
        ws_server.close()
        await ws_server.wait_closed()
        if owns_storage:
            storage.close()
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
    ap.add_argument("--db", default="chatroom.db",
                    help="SQLite 库文件(默认 chatroom.db; 传 :memory: 则不落盘)")
    ap.add_argument("--webroot", default=None,
                    help="前端静态资源目录(默认仓库内 web/)")
    return ap.parse_args(argv)


async def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    stop = asyncio.Event()
    await run(args.host, args.port, args.heartbeat_interval, args.ping_timeout, stop,
              db_path=args.db, webroot=args.webroot)
    try:
        await stop.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        stop.set()

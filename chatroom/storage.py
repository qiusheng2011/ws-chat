"""chatroom.storage — SQLite 持久化层: 房间 / 客户端 / 连接会话 / 聊天记录。

设计要点:
  • 只用标准库 sqlite3; 连接以 check_same_thread=False 创建, 所有写操作由
    threading.Lock 串行化, 事件循环不被阻塞(async 方法内部走 asyncio.to_thread)。
  • 方法失败时抛异常; "不炸聊天" 的降级由调用方(server)用 try/except 兜住。
  • path 传 ":memory:" 可让测试/repro 完全不落盘。
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from .common import HISTORY_LIMIT

logger = logging.getLogger("chatroom.storage")

SCHEMA = """
CREATE TABLE IF NOT EXISTS rooms (
    name        TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS clients (
    cid         TEXT PRIMARY KEY,
    nickname    TEXT NOT NULL,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL
);
-- 每次成功握手一行, 断开时回填; "连接存储相关信息" 的主表
CREATE TABLE IF NOT EXISTS sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cid             TEXT NOT NULL,
    room            TEXT NOT NULL,
    nickname        TEXT NOT NULL,
    connected_at    TEXT NOT NULL,
    disconnected_at TEXT,
    close_reason    TEXT
);
CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    room        TEXT NOT NULL,
    cid         TEXT NOT NULL,
    nickname    TEXT NOT NULL,
    content     TEXT NOT NULL,
    sent_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_room_id ON messages(room, id);
CREATE INDEX IF NOT EXISTS idx_sessions_room ON sessions(room);
"""


def _now() -> str:
    """本地时间, 秒级 ISO 字符串(便于人工查看)。"""
    return datetime.now().isoformat(timespec="seconds")


class Storage:
    """房间 / 客户端 / 会话 / 消息 的 SQLite 存储。"""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = str(path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    # ---------------- 同步实现(只在 to_thread 里跑) ----------------

    def _run(self, fn, *args) -> Any:
        with self._lock:
            cur = fn(*args)
            self._conn.commit()
            return cur

    def _ensure_rooms(self, names: Sequence[str]) -> None:
        self._conn.executemany(
            "INSERT OR IGNORE INTO rooms(name, created_at) VALUES(?, ?)",
            [(n, _now()) for n in names],
        )

    def _list_rooms(self) -> List[str]:
        rows = self._conn.execute("SELECT name FROM rooms ORDER BY name").fetchall()
        return [r["name"] for r in rows]

    def _upsert_client(self, cid: str, nickname: str) -> None:
        now = _now()
        self._conn.execute(
            "INSERT INTO clients(cid, nickname, first_seen, last_seen) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(cid) DO UPDATE SET nickname=excluded.nickname, last_seen=excluded.last_seen",
            (cid, nickname, now, now),
        )

    def _record_connect(self, cid: str, room: str, nickname: str) -> int:
        cur = self._conn.execute(
            "INSERT INTO sessions(cid, room, nickname, connected_at) VALUES(?, ?, ?, ?)",
            (cid, room, nickname, _now()),
        )
        return int(cur.lastrowid or 0)

    def _record_disconnect(self, session_id: int, reason: str) -> None:
        self._conn.execute(
            "UPDATE sessions SET disconnected_at=?, close_reason=? WHERE id=? AND disconnected_at IS NULL",
            (_now(), reason, session_id),
        )

    def _save_message(self, room: str, cid: str, nickname: str, content: str) -> None:
        self._conn.execute(
            "INSERT INTO messages(room, cid, nickname, content, sent_at) VALUES(?, ?, ?, ?, ?)",
            (room, cid, nickname, content, _now()),
        )

    def _recent_messages(self, room: str, limit: int) -> List[sqlite3.Row]:
        rows = self._conn.execute(
            "SELECT nickname, content, sent_at FROM messages WHERE room=? ORDER BY id DESC LIMIT ?",
            (room, limit),
        ).fetchall()
        return list(reversed(rows))       # 旧的在前, 便于按序重放

    # ---------------- 异步公开接口 ----------------

    async def ensure_rooms(self, names: Sequence[str]) -> None:
        """预置房间; 已存在则忽略。"""
        await asyncio.to_thread(self._run, self._ensure_rooms, tuple(names))

    async def list_rooms(self) -> List[str]:
        """所有已知房间(按名字排序)。"""
        return await asyncio.to_thread(self._run, self._list_rooms)

    async def upsert_client(self, cid: str, nickname: str) -> None:
        """登记/更新客户端标识与昵称。"""
        await asyncio.to_thread(self._run, self._upsert_client, cid, nickname)

    async def record_connect(self, cid: str, room: str, nickname: str) -> int:
        """记录一次连接, 返回 session id(断开时用它回填)。"""
        return await asyncio.to_thread(self._run, self._record_connect, cid, room, nickname)

    async def record_disconnect(self, session_id: Optional[int], reason: str) -> None:
        """回填断开时间与原因; session_id 为 None 时忽略。"""
        if not session_id:
            return
        await asyncio.to_thread(self._run, self._record_disconnect, session_id, reason)

    async def save_message(self, room: str, cid: str, nickname: str, content: str) -> None:
        await asyncio.to_thread(self._run, self._save_message, room, cid, nickname, content)

    async def recent_messages(self, room: str, limit: int = HISTORY_LIMIT) -> List[Dict[str, str]]:
        """某房间最近 limit 条消息, 按时间正序返回。"""
        rows = await asyncio.to_thread(self._run, self._recent_messages, room, limit)
        return [{"nickname": r["nickname"], "content": r["content"], "sent_at": r["sent_at"]}
                for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

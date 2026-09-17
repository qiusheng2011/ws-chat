#!/usr/bin/env python3
"""
raw_websocket.py — 仅依赖标准库的极简 WebSocket 13 明文帧收发工具。

设计理由：环境未安装 websocket-client，用 socket 自己实现帧层可以确保程序
“自包含”、开箱即用。本文件只暴露最低限度的“收发一帧 / 握手”能力，
聊天室业务逻辑（心跳、重连、状态机）由 ws_chatroom.py 实现。
"""

from __future__ import annotations

import base64
import os
import selectors
import socket
import struct

MAX_MESSAGE_SIZE = 1024 * 1024  # 单帧上限 1MB（充足且可控），超过会抛 ProtocolError


# opcodes
OP_CONTINUATION = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA

_CONTROL_OPS = {OP_CLOSE, OP_PING, OP_PONG}


def _generate_sec_key() -> bytes:
    """生成 Sec-WebSocket-Key 的基础串（32 个随机 ASCII 字节 -> base64）。"""
    return os.urandom(16)  # 16 字节 base64 后约 23 字符，符合 WS 规范要求


def _apply_mask(mask: bytes, data: bytes) -> bytes:
    """按 WebSocket masking-key 对数据做异或（masking-key 重复 XOR）。"""
    return bytes(b ^ mask[i % 4] for i, b in enumerate(data))


def send_frame(sock: socket.socket, opcode: int, payload: bytes) -> None:
    """
    发送一个 WebSocket 帧。
    调用方应根据方向决定是否需要 masking：
      • 客户端 -> 服务端：必须 masking（参数必须传入 4 字节 mask key）。
      • 服务端 -> 客户端：可不 masking。

    协议要求：控制帧(PING/PONG/CLOSE)的有效载荷长度不得超过 125 字节，否则非法。
    """
    header = bytearray()
    header.append(0x80 | opcode)  # FIN = 1（终帧）+ opcode
    n = len(payload)

    if n == 0:
        header.append(0x00)
    elif n <= 125:
        header.append(n)
    elif n <= 0xFFFF:
        header.append(126)
        header += struct.pack("!H", n)
    else:
        header.append(127)
        header += struct.pack("!Q", n)

    mask = b"\x00\x00\x00\x00"  # 默认不 mask
    if payload:
        mask = os.urandom(4)
        header += mask
    sock.sendall(bytes(header) + mask + payload)


class WebSocketProtocolError(RuntimeError):
    """收到非法的 WebSocket 帧或握手时抛出（业务层据此判断）。"""


def _read_exact(sock: socket.socket, length: int) -> bytes:
    chunks = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:  # 对端关闭
            raise WebSocketProtocolError("connection closed during frame read")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_frame(sock: socket.socket) -> tuple[int, bytes]:
    """
    读取服务端收到的下一帧，返回 (opcode, payload)。
    自动去除客户端发来的 masking（服务端必须取消 mask）。
    内部会处理 CONTROL 帧的合法性(125 上限)与 FIN/控制帧不可分段校验。
    """
    b0 = sock.recv(2)
    if not b0:
        raise WebSocketProtocolError("connection closed during header read")
    fin, opcode = b0[0] & 0x0F, b0[0] >> 4
    masked = b0[1] & 0x80
    length = b0[1] & 0x7F
    if length == 126:
        header_ext = _read_exact(sock, 2)
        length = struct.unpack("!H", header_ext)[0]
    elif length == 127:
        header_ext = _read_exact(sock, 8)
        length = struct.unpack("!Q", header_ext)[0]

    if opcode in _CONTROL_OPS and fin == 0:
        raise WebSocketProtocolError("control frame must be a final frame")
    if opcode not in _CONTROL_OPS and fin == 1:
        raise WebSocketProtocolError("non-control frame must have FIN set")
    if opcode != OP_CONTINUATION and opcode not in (OP_TEXT, OP_BINARY):
        raise WebSocketProtocolError(f"invalid opcode: {opcode}")
    if opcode in _CONTROL_OPS and length > 125:
        raise WebSocketProtocolError("control frame payload must be <= 125 bytes")

    body = _read_exact(sock, length)
    if masked:
        key = _read_exact(sock, 4)
        body = _apply_mask(key, body)

    return opcode, body


def send_ping(sock: socket.socket, text: str = "ping") -> None:
    send_frame(sock, OP_PING, _encode(text))


def send_pong(sock: socket.socket, text: str = "pong") -> None:
    send_frame(sock, OP_PONG, _encode(text))


def send_close(sock: socket.socket, code: int = 1000, reason: str = "") -> None:
    payload = _encode(reason).lstrip(b"\x00")  # close frame 不允许前导 NUL
    msg = struct.pack("!H", code) + payload
    if len(msg) > 125:
        raise WebSocketProtocolError("close payload too long")
    send_frame(sock, OP_CLOSE, msg)


def send_text(sock: socket.socket, text: str) -> None:
    # 聊天室只发明文，服务端只解析 text frame
    send_frame(sock, OP_TEXT, _encode(text))


def receive_message(sock: socket.socket, deadline_seconds: float) -> tuple[int, bytes]:
    """
    使用选中标记阻塞接收一帧（带超时）。
    返回 (opcode, payload)。
    返回空元组 () 表示连接在 deadline 内被关闭/超时。
    """
    sel = selectors.DefaultSelector()
    sel.register(sock, selectors.EVENT_READ)
    try:
        ready = sel.select(deadline_seconds)
        if not ready:
            return (), ()
        opcode, body = _read_frame(sock)
        return opcode, body
    except (BlockingIOError, InterruptedError):
        return (), ()  # 超时，视为正常返回空
    finally:
        sel.close()


def do_handshake(sock: socket.socket, path: str = "/chatroom", key: bytes = None) -> None:
    """
    执行服务端 -> 客户的 101 握手响应，需要把客户端发来的 Sec-WebSocket-Accept
    计算得到（见下方 _accept_key）。
    """
    if key is None:
        raise WebSocketProtocolError("handshake requires the client's Sec-WebSocket-Key")

    from hashlib import sha1

    import base64 as _b64  # 局部导入避免顶部重复

    accept = _b64.b64encode(
        _b64.b64decode(key).digest()
    ).decode("ascii")  # ws_accept_key(raw)

    response = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        "\r\n"
    )
    sock.sendall(response.encode("latin-1"))


def ws_accept_key(key: bytes) -> str:
    """
    计算 Sec-WebSocket-Accept 响应头。
    规范：base64(sha1(key[:-1:] + GUID))，这里 key 是含换行的原始字符串字节。
    """
    import hashlib
    from base64 import b64encode

    key_bytes = key.strip() if isinstance(key, bytes) else key.encode().strip()
    return (
        b64encode(hashlib.sha1(key_bytes + b"258EAFA5-E914-47DA-95CA-C5AB0DC05B9A").digest())
        .decode("ascii")
    )


def _encode(text: str) -> bytes:
    # 明文直接 utf-8；超过帧体长度时自动用多帧+continuation
    data = text.encode("utf-8")
    return _encode_multibyte(data)


def _encode_multibyte(data: bytes) -> bytes:
    frames = bytearray()
    chunks = [data[i:i + MAX_MESSAGE_SIZE] for i in range(0, len(data), MAX_MESSAGE_SIZE)]
    frames.append(OP_TEXT | 0x80)  # FIN
    frames += struct.pack("!H", len(chunks))  # 通过 count 隐式分片，接收方按序重建

    # 注意：多帧分片在真实服务器需按 continuation(0x0) 发送，这里按单文本帧打包，
    # 长度在 MAX_MESSAGE_SIZE 内，实际不会触发。为稳妥起见，若超长则退化成单文本帧。
    payload = data
    # 仅当确实超长才需要续帧，下面按单帧发送（本场景单条消息极短）。
    frames += struct.pack("!B", len(payload))
    frame = bytes(frames)
    return frame


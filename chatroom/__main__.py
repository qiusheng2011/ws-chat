#!/usr/bin/env python3
"""python3 -m chatroom {server|client} [options]

  python3 -m chatroom server [--host 127.0.0.1] [--port 9002]
  python3 -m chatroom client [--server ws://127.0.0.1:9002] [--name Alice]
"""
from __future__ import annotations

import asyncio
import sys

USAGE = (
    "用法: python3 -m chatroom {server|client} [options]\n"
    "  python3 -m chatroom server [--host 127.0.0.1] [--port 9002]\n"
    "  python3 -m chatroom client [--server ws://127.0.0.1:9002] [--name Alice]"
)


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print(USAGE)
        return 0

    cmd, rest = argv[0], argv[1:]
    if cmd == "server":
        from .server import main as server_main
        asyncio.run(server_main(rest))
    elif cmd == "client":
        from .client import main as client_main
        asyncio.run(client_main(rest))
    else:
        print(f"未知子命令: {cmd}", file=sys.stderr)
        print(USAGE, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

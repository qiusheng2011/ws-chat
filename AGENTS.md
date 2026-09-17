# AGENTS.md — 协作指南（面向 AI 编码代理与人类开发者）

## 项目一句话

`chatroom` 是一个最小可运行的 WebSocket 聊天室：asyncio + websockets，单包 `chatroom/`（server / client / common 三模块），重点特性是**应用级心跳**与**指数退避自动重连**。

## 必知命令

| 目的 | 命令 |
|------|------|
| 跑测试 | `pytest -q test_chatroom.py`（16 个用例，应全绿） |
| 起服务端 | `python3 -m chatroom server [--host 127.0.0.1] [--port 9002]` |
| 起客户端 | `python3 -m chatroom client --server ws://127.0.0.1:9002 --name Alice` |
| 心跳回归演示 | `python3 repro_drop.py` |

**陷阱：裸 `pytest` 会失败。** `legacy_broken/test_chatroom.py` 有语法错误，收集阶段即报错中断；永远显式指定 `test_chatroom.py`。

## 代码结构与分工

| 文件 | 职责 |
|------|------|
| `chatroom/common.py` | 协议常量（HELLO/WELCOME/SYSTEM/CHAT/HEARTBEAT）、`Message` 数据类（`to_json`/`from_json`/`is_ping`/`is_pong`）、`Backoff` 退避 |
| `chatroom/server.py` | `Server` 连接表（`_clients` / `_last_seen` / `_pending_ping`）、`_serve` 单连接生命周期、`_heartbeat` 定时 Ping 与超时回收、`run()` 启动入口 |
| `chatroom/client.py` | `StdinFeed`（进程级唯一 stdin 线程）、`session` 单次连接、`run()` 重连循环、`render` 终端渲染 |
| `chatroom/__main__.py` | `python3 -m chatroom {server|client}` 子命令分发 |
| `test_chatroom.py` | 纯单元（front 部分）+ 端到端（in-process 服务端 + 真实 websocket 客户端） |
| `legacy_broken/` | **废弃实现，语法损坏。不要修改、不要修复、不要收集、不要引用。** |

## 不可违反的协议/行为不变量

1. **心跳判定窗口从“本条 Ping 发出时刻”起算**，`PING_TIMEOUT` 内该连接无任何上行帧才踢出。不要用任意滑动窗口替代——那是 `repro_drop.py` 曾复现的 bug：按时回 Pong 的安静客户端会在第一个心跳周期被误踢。回归测试：`test_heartbeat_pong_survives_when_interval_exceeds_timeout`。
2. `_pending_ping` 中已有在途 Ping 的连接**不重复发新 Ping、不刷时间戳**，否则死连接会被反复续命。
3. 客户端收到 Ping（`HEARTBEAT ack=false`）必须立刻回 Pong（`ack=true`）。
4. CHAT 广播**排除发送者**（客户端本地回显）；留白消息直接丢弃，不广播。
5. 首帧必须是 `HELLO`，否则 SYSTEM 提示 + close 1002；连接结束时向其余人广播离开公告（`finally` 中保证）。
6. 服务端**不转发**客户端发来的 SYSTEM 帧。

## 代码风格约定

- 中文 docstring / 注释，简洁口语化；模块头部 docstring 说明“运行方式 + 行为要点”。
- `from __future__ import annotations`，公共函数带类型注解；dataclass 表达协议消息。
- 命名：协议帧构造函数 `make_*`；内部成员以下划线前缀。
- 错误处理宁宽毋死：send 失败 debug 级吞掉（由 finally 清理），避免广播循环因一条坏连接整体挂掉。

## 依赖与环境

- 运行时依赖仅 `websockets ≥ 16`（用 `websockets.asyncio.server/client` 新命名空间 API，**不要回退到 `websockets.legacy` 或 old `websockets.serve` 风格**）。
- Python 3.12 开发，代码兼容 3.10+。
- 测试用 pytest，端到端测试在进程内起真实服务端（随机端口 `port=0`），用 `asyncio.run` 包裹每个场景。

## 改动提交流程

1. 改代码 → 跑 `pytest -q test_chatroom.py` 全绿。
2. 动了协议常量或帧语义 → 同步改 `chatroom/common.py`、README “协议说明”表，并补/改测试。
3. 动了心跳判定 → 额外跑 `python3 repro_drop.py` 人工确认安静客户端存活 14s。
4. 任何新行为 → 在 `test_chatroom.py` 加对应端到端用例（复用 `ChatServer` fixture，并用 `server(heartbeat_interval=…, ping_timeout=…)` 缩短时基加速，例如 0.15s / 0.3s）。

## Git 约定

- `.gitignore` 已覆盖 `__pycache__/`、`*.pyc`、`.pytest_cache/` 等；**不要提交任何缓存/构建产物**。
- 提交信息用中文祈使句，如“修复重名昵称编号不递增”。

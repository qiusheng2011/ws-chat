# AGENTS.md — 协作指南（面向 AI 编码代理与人类开发者）

## 项目一句话

`chatroom` 是一个 WebSocket 多房间聊天室：asyncio + websockets，后端 `chatroom/` + 原生前端 `web/`，同端口提供 HTTP（静态页 + `/api/rooms`）与 WebSocket，SQLite 持久化房间/客户端/会话/消息，重点特性是**应用级心跳**、**浏览器持久化唯一客户端标识**与**指数退避自动重连**。

## 必知命令

| 目的 | 命令 |
|------|------|
| 跑测试 | `pytest -q test_chatroom.py`（25 个用例，应全绿） |
| 起服务端 | `python3 -m chatroom server [--host 127.0.0.1] [--port 9002] [--db chatroom.db] [--webroot web]` |
| 起客户端 | `python3 -m chatroom client --server ws://127.0.0.1:9002 --name Alice [--room 大厅]` |
| 心跳回归演示 | `python3 repro_drop.py` |
| 容器起服务 | `docker compose up -d --build`（数据在命名卷 `chatroom-data`） |
| 容器停服务 | `docker compose down`（**不要加 `-v`**，加了会删数据卷） |

**陷阱：裸 `pytest` 会失败。** `legacy_broken/test_chatroom.py` 有语法错误，收集阶段即报错中断；永远显式指定 `test_chatroom.py`。

## 代码结构与分工

| 文件 | 职责 |
|------|------|
| `chatroom/common.py` | 协议常量（HELLO/WELCOME/SYSTEM/CHAT/HEARTBEAT）、`Message` 数据类（v2 新增 `room`/`cid`）、`Backoff` 退避、房间名清洗 |
| `chatroom/storage.py` | SQLite 持久化：`rooms`/`clients`/`sessions`/`messages` 四表；所有写操作走 `asyncio.to_thread` |
| `chatroom/server.py` | `Server` 多房间注册表、单连接生命周期 `_serve`、全局心跳 `_heartbeat`、同端口 HTTP 静态/API `process_request`、`run()` 启动入口 |
| `chatroom/client.py` | `StdinFeed`（进程级唯一 stdin 线程）、`session` 单次连接、`run()` 重连循环、`render` 终端渲染 |
| `chatroom/__main__.py` | `python3 -m chatroom {server|client}` 子命令分发 |
| `web/{index.html,app.js,style.css}` | 无构建原生前端：房间列表、唯一 cid、进房聊天、指数退避重连 |
| `Dockerfile` / `.dockerignore` | 容器镜像：slim + `requirements.txt` + `chatroom/` + `web/`；数据目录 `/data`，健康检查打 `/api/rooms` |
| `docker-compose.yml` | 编排与**命名卷 `chatroom-data:/data`**（数据持久化的关键） |
| `requirements.txt` / `requirements-dev.txt` | 运行时依赖；开发依赖（+pytest） |
| `test_chatroom.py` | 纯单元 + 端到端（in-process 服务端 + 真实 websocket 客户端 + HTTP API/静态资源） |
| `legacy_broken/` | **废弃实现，语法损坏。不要修改、不要修复、不要收集、不要引用。** |

## 不可违反的协议/行为不变量

1. **心跳判定窗口从“本条 Ping 发出时刻”起算**，`PING_TIMEOUT` 内该连接无任何上行帧才踢出。不要用任意滑动窗口替代——那是 `repro_drop.py` 曾复现的 bug：按时回 Pong 的安静客户端会在第一个心跳周期被误踢。回归测试：`test_heartbeat_pong_survives_when_interval_exceeds_timeout`。
2. `_pending_ping` 中已有在途 Ping 的连接**不重复发新 Ping、不刷时间戳**，否则死连接会被反复续命。
3. 客户端收到 Ping（`HEARTBEAT ack=false`）必须立刻回 Pong（`ack=true`）。
4. CHAT 广播**排除发送者**（客户端本地回显）；留白消息直接丢弃，不广播。
5. 首帧必须是 `HELLO`，否则 SYSTEM 提示 + close 1002；连接结束时向其余人广播离开公告（`finally` 中保证）。
6. 服务端**不转发**客户端发来的 SYSTEM 帧。
7. **CHAT / SYSTEM 广播只在本房间内**，禁止跨房间泄漏。
8. **同房间同 cid 新连接顶掉旧连接**；`Room.leave()` 必须校验 ws 对象身份，防止顶号后旧连接的清理误杀新连接。旧连接关闭码为 `4001`。
9. **历史重放位于 WELCOME 之后、加入公告之前**，按时间正序发送普通 CHAT 帧。
10. HTTP 静态资源与 `/api/rooms` 走 `websockets.serve(..., process_request=...)` 钩子；返回 `Response` 即非 WebSocket 响应；路径穿越必须被拦截为 404。
11. `run()` 默认 `db_path=":memory:"`，确保测试/库代码被其它模块调用时不落盘。
12. **容器里 SQLite 必须写在 `/data`（命名卷）**：不要把数据库放进镜像层或容器可写层，否则重建/重启即丢数据。`docker compose down` 不许带 `-v`。

## 代码风格约定

- 中文 docstring / 注释，简洁口语化；模块头部 docstring 说明“运行方式 + 行为要点”。
- `from __future__ import annotations`，公共函数带类型注解；dataclass 表达协议消息。
- 命名：协议帧构造函数 `make_*`；内部成员以下划线前缀。
- 错误处理宁宽毋死：send 失败 debug 级吞掉（由 finally 清理），避免广播循环因一条坏连接整体挂掉；存储写失败降级为 warning，不炸聊天。

## 依赖与环境

- 运行时依赖仅 `websockets ≥ 16`（用 `websockets.asyncio.server/client` 新命名空间 API，**不要回退到 `websockets.legacy` 或 old `websockets.serve` 风格**）。
- Python 3.12 开发，代码兼容 3.10+。
- 测试用 pytest，端到端测试在进程内起真实服务端（随机端口 `port=0`），用 `asyncio.run` 包裹每个场景。
- HTTP 请求在测试里必须走 `asyncio.to_thread(http.client.HTTPConnection(...))`，禁止在事件循环里阻塞。

## 改动提交流程

1. 改代码 → 跑 `pytest -q test_chatroom.py` 全绿。
2. 动了协议常量或帧语义 → 同步改 `chatroom/common.py`、README “协议说明”表与 AGENTS.md，并补/改测试。
3. 动了心跳判定 → 额外跑 `python3 repro_drop.py` 人工确认安静客户端存活 14s。
4. 新增前端行为 → 用 `node --check web/app.js` 做语法检查；必要时在 `test_chatroom.py` 用 HTTP API / WS 补端到端覆盖。
5. 任何服务端新行为 → 在 `test_chatroom.py` 加对应端到端用例（复用 `ChatServer` fixture，并用 `server(heartbeat_interval=…, ping_timeout=…)` 缩短时基加速，例如 0.15s / 0.3s）。
6. 动了容器相关（Dockerfile / compose / 依赖清单）→ `docker compose build` 后跑一次"重建不丢数据"验证：发消息 → `stop` + `build` + `up -d` → 重连确认历史仍在。

## Git 约定

- `.gitignore` 已覆盖 `__pycache__/`、`*.pyc`、`.pytest_cache/`、`chatroom.db*` 等；**不要提交任何缓存/构建/数据库产物**。
- 提交信息用中文祈使句，如“修复重名昵称编号不递增”。

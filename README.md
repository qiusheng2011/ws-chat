# chatroom — 最小可运行的 WebSocket 聊天室

一个用纯 `asyncio` + `websockets` 实现的小型聊天室：**服务端 / 客户端 / 传输协议** 三个模块都在 `chatroom/` 包内，带应用级心跳、自动重连和端到端测试。

## 功能一览

- 多客户端广播聊天（服务端不把消息回推给发送者，由客户端本地回显）
- 昵称冲突自动改名（`Alice` → `Alice#2`）
- 加入 / 离开系统公告
- 应用级心跳：服务端周期发 Ping（`HEARTBEAT ack=false`），客户端回 Pong（`ack=true`），超时未应答即踢下线
- 客户端断线后指数退避自动重连（1s → 2s → 4s … 封顶 32s），重连期间输入的消息自动补发
- 裸文本帧自动当作发言，无需自己拼 JSON

## 环境要求

- Python ≥ 3.10（开发环境为 3.12）
- `websockets` ≥ 16（使用 `websockets.asyncio` 命名空间 API）
- `pytest`（仅运行测试时需要）

```bash
pip install websockets pytest
```

## 快速开始

```bash
# 1. 起服务端（默认 127.0.0.1:9002）
python3 -m chatroom server

# 2. 在另一个终端连一个客户端
python3 -m chatroom client --name Alice

# 3. 再开一个终端
python3 -m chatroom client --name Bob
```

输入任意一行并回车即发言；`/quit`、`/exit`、`Ctrl-D` 或 `Ctrl-C` 退出。

常用参数：

```bash
python3 -m chatroom server --host 0.0.0.0 --port 9001 \
    --heartbeat-interval 10 --ping-timeout 3

python3 -m chatroom client --server ws://127.0.0.1:9001 --name Alice \
    --backoff-base 1 --backoff-cap 32
```

## 协议说明

每条消息是一个 JSON 对象（见 `chatroom/common.py`）：

```json
{"type": "CHAT", "frm": "Alice", "content": "hi", "ack": false}
```

| type        | 方向      | 含义 |
|-------------|-----------|------|
| `HELLO`     | 客 → 服   | 连上后第一帧，携带昵称 |
| `WELCOME`   | 服 → 客   | 握手确认；`frm` 为最终昵称，`content` 为当前在线人数 |
| `SYSTEM`    | 服 → 客   | 加入 / 离开公告 |
| `CHAT`      | 双向      | 用户发言；服务端广播时排除发送者 |
| `HEARTBEAT` | 双向      | `ack=false` 为 Ping，`ack=true` 为 Pong |

客户端协议错误（首帧不是 `HELLO`）时，服务端返回 SYSTEM 提示并以 `1002` 关闭。

### 心跳语义（重要）

服务端每 `HEARTBEAT_INTERVAL`（默认 10s）发一轮 Ping，**判定窗口从本条 Ping 发出时刻起算**：该 Ping 在 `PING_TIMEOUT`（默认 3s）内没收到此连接的任何上行帧（Pong 或其它消息）才判离线。

> 这是修复过的行为：旧实现沿用任意 3 秒滑窗，会把**按时回 Pong 的安静听聊用户**误踢。回归防护见下面的测试与 `repro_drop.py`。

## 运行测试

```bash
pytest -q test_chatroom.py
```

覆盖：消息编解码、退避、HELLO/WELCOME 握手、加入/离开公告、CHAT 广播、心跳保活、心跳超时踢人、重名改名、协议错误拒绝、CLI 入口——共 16 个用例。

> ⚠️ **不要直接跑裸 `pytest`**：`legacy_broken/` 里有语法损坏的历史遗留测试，会在收集阶段报错。务必显式指定 `test_chatroom.py`。

心跳回归演示（默认参数观察 14 秒，安静回 Pong 的客户端应全程存活）：

```bash
python3 repro_drop.py
```

## 目录结构

```
.
├── chatroom/            # 主包
│   ├── __init__.py      # 公共导出
│   ├── __main__.py      # python3 -m chatroom {server|client} 入口
│   ├── common.py        # 协议常量、Message 数据类、Backoff 退避
│   ├── server.py        # 服务端：连接表、广播、心跳定时器
│   └── client.py        # 客户端：自动重连、Pong 应答、stdin 读取
├── test_chatroom.py     # 端到端冒烟测试（in-process 服务端 + 真实客户端）
├── repro_drop.py        # 心跳误踢回归演示脚本
└── legacy_broken/       # 早期的另一版实现（已废弃，语法损坏，留作参考，勿跑勿改）
```

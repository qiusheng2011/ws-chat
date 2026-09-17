# chatroom — 多房间 WebSocket 聊天室

用纯 `asyncio` + `websockets` 实现的小型聊天室：后端 `chatroom/` 与原生前端 `web/` 同源同端口(`127.0.0.1:9002`)，带应用级心跳、指数退避重连、浏览器持久化唯一客户端标识，以及 SQLite 持久化（房间 / 客户端 / 连接会话 / 聊天记录）。

## 功能一览

- **多房间隔离**：每个 WebSocket 连接归属一个房间；CHAT / SYSTEM 广播只在本房间内。
- **Web 前端**：浏览器打开 `http://127.0.0.1:9002`，列出所有聊天室，点击进入即可聊天；`localStorage` 保存唯一客户端标识，刷新不会产生"重影"。
- **终端客户端**：`python3 -m chatroom client --room 大厅`，每个进程自动生成唯一 cid，支持断线重连。
- **SQLite 持久化**：`--db chatroom.db` 保存房间、客户端、连接会话与聊天历史；进入房间自动重放最近 50 条历史。
- 昵称冲突自动改名（`Alice` → `Alice#2`）；加入 / 离开系统公告（房间作用域）。
- 应用级心跳：服务端周期发 Ping（`HEARTBEAT ack=false`），客户端回 Pong（`ack=true`），超时未应答即踢下线。
- 客户端断线后指数退避自动重连（1s → 2s → 4s … 封顶 32s），重连期间输入的消息自动补发。
- 裸文本帧自动当作发言，无需自己拼 JSON。

## 环境要求

- Python ≥ 3.10（开发环境为 3.12）
- `websockets` ≥ 16（使用 `websockets.asyncio` 命名空间 API）
- `pytest`（仅运行测试时需要）

```bash
pip install -r requirements-dev.txt   # 运行时依赖 + pytest
# 或只装运行时: pip install -r requirements.txt
```

## 快速开始

```bash
# 起服务端（默认 127.0.0.1:9002；同端口同时提供 Web 页面 + WebSocket）
python3 -m chatroom server

# 浏览器打开 http://127.0.0.1:9002，选一个房间进入即可

# 或从终端进入
python3 -m chatroom client --name Alice --room 大厅
```

终端输入任意一行并回车即发言；`/quit`、`/exit`、`Ctrl-D` 或 `Ctrl-C` 退出。

常用参数：

```bash
# 服务端
python3 -m chatroom server --host 0.0.0.0 --port 9001 \
    --heartbeat-interval 10 --ping-timeout 3 \
    --db chatroom.db --webroot web

# 客户端
python3 -m chatroom client --server ws://127.0.0.1:9001 --name Alice \
    --room 技术 --backoff-base 1 --backoff-cap 32
```

> `--db :memory:` 可让服务端不落盘（测试默认）。

## Docker 部署

```bash
docker compose up -d --build        # 构建并启动
docker compose logs -f            # 看日志
docker compose down               # 停容器(数据保留在命名卷 chatroom-data 里)
```

浏览器打开 `http://localhost:9102`（端口在 `docker-compose.yml` 里改）。

**关键：SQLite 数据落在命名卷 `chatroom-data`，与镜像/容器生命周期解耦 —— 重新 `build`、停掉容器、换机器迁移都不丢数据。**

```bash
# 极端验证: 从无缓存重建 + 重启容器, 聊天记录依然在
docker compose stop
docker compose build --no-cache
docker compose up -d
# 浏览器进同一个房间 → 历史回放仍能看到重建前的发言
```

要完全销毁数据用 `docker compose down -v`（`-v` 会删卷）。

> 若构建报 `failed to update builder last activity time: ... read-only file system`（buildx 元数据目录只读的受限环境），用 legacy builder：`DOCKER_BUILDKIT=0 docker compose build`。

数据结构（落在卷 `/data/chatroom.db` 里）：

| 表        | 内容                                  |
|-----------|---------------------------------------|
| `rooms`     | 房间名 + 创建时间                    |
| `clients`   | cid → 昵称 + 首末次出现时间          |
| `sessions`  | 每次成功握手一行, 断开时回填时间与原因|
| `messages`  | 聊天记录(进入房间会重放最近 50 条)   |

要直接看：`sqlite3 /var/lib/docker/volumes/test_model_chatroom-data/_data/chatroom.db`。

## 协议说明

每条消息是一个 JSON 对象（见 `chatroom/common.py`）：

```json
{"type": "CHAT", "frm": "Alice", "content": "hi", "ack": false,
 "room": "大厅", "cid": "9f2c..."}
```

| type        | 方向      | 含义 |
|-------------|-----------|------|
| `HELLO`     | 客 → 服   | 连上后第一帧；`frm=昵称`，`room=目标房间`，`cid=客户端唯一标识`（缺省由服务端生成） |
| `WELCOME`   | 服 → 客   | 握手确认；`frm=最终昵称`，`content=本房间在线人数`，`room/cid` 回显服务端确认值 |
| `SYSTEM`    | 服 → 客   | 加入 / 离开 / 错误公告（带 `room`） |
| `CHAT`      | 客 → 服   | 用户发言；服务端广播时排除发送者，且只在本房间 |
| `HEARTBEAT` | 双向      | `ack=false` 为 Ping，`ack=true` 为 Pong |

房间列表由 HTTP 接口提供：

```bash
curl http://127.0.0.1:9002/api/rooms
# → [{"name": "大厅", "online": 3}, {"name": "闲聊", "online": 1}, ...]
```

- 客户端协议错误（首帧不是 `HELLO` 或房间名非法）时，服务端返回 SYSTEM 提示并以 `1002` 关闭。
- 同房间同 cid 的新连接会顶掉旧连接（旧连接收到 close 码 `4001`），刷新页面不会产生重复用户。
- 进入房间后，服务端会先按时间正序重放最近 50 条历史（普通 CHAT 帧）。

### 心跳语义（重要）

服务端每 `HEARTBEAT_INTERVAL`（默认 10s）发一轮 Ping，**判定窗口从本条 Ping 发出时刻起算**：该 Ping 在 `PING_TIMEOUT`（默认 3s）内没收到此连接的任何上行帧（Pong 或其它消息）才判离线。

> 这是修复过的行为：旧实现沿用任意 3 秒滑窗，会把**按时回 Pong 的安静听聊用户**误踢。回归防护见下面的测试与 `repro_drop.py`。

## 运行测试

```bash
pytest -q test_chatroom.py
```

覆盖：消息编解码、退避、HELLO/WELCOME 握手、加入/离开公告、CHAT 广播、心跳保活、心跳超时踢人、重名改名、协议错误拒绝、**多房间隔离、同 cid 顶号、历史回放、HTTP 房间列表、静态资源服务、SQLite 持久化**、CLI 入口——共 25 个用例。

> ⚠️ **不要直接跑裸 `pytest`**：`legacy_broken/` 里有语法损坏的历史遗留测试，会在收集阶段报错。务必显式指定 `test_chatroom.py`。

心跳回归演示（默认参数观察 14 秒，安静回 Pong 的客户端应全程存活）：

```bash
python3 repro_drop.py
```

## 目录结构

```
.
├── chatroom/            # 后端包
│   ├── __init__.py      # 公共导出
│   ├── __main__.py      # python3 -m chatroom {server|client} 入口
│   ├── common.py        # 协议常量、Message 数据类、Backoff 退避、房间名清洗
│   ├── storage.py       # SQLite 持久化层
│   ├── server.py        # 多房间服务端、同端口 HTTP 静态/API、心跳定时器
│   └── client.py        # 终端客户端：自动重连、Pong 应答、stdin 读取
├── web/                 # 前端静态资源（零构建）
│   ├── index.html
│   ├── app.js
│   └── style.css
├── Dockerfile           # 容器镜像: slim + 依赖 + 代码, 数据挂 /data
├── docker-compose.yml   # 一键起服务; 命名卷 chatroom-data 持久化
├── .dockerignore        # 构建上下文瘦身
├── requirements.txt     # 运行时依赖(websockets)
├── requirements-dev.txt # 开发依赖(+pytest)
├── test_chatroom.py     # 端到端冒烟测试（in-process 服务端 + 真实客户端）
├── repro_drop.py        # 心跳误踢回归演示脚本
└── legacy_broken/       # 早期的另一版实现（已废弃，语法损坏，留作参考，勿跑勿改）
```

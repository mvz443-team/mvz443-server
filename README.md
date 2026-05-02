# MVZ443Server

MVZ443Server 是一个基于 Go + Fiber + WebSocket + Protobuf 的双人房间游戏服务器。服务器负责 HTTPS/WS 接入、房间生命周期、玩家准备流程、游戏阶段切换和帧同步消息转发；战斗内的怪物、植物射击等细节仍主要由客户端本地模拟，服务端只校验并广播会影响多端一致性的玩家操作。

客户端使用 Phaser + React + TypeScript，通过 HTTPS 拉取房间列表，通过 WSS 建立房间连接，所有 WS 业务消息都使用 Protobuf 二进制格式。更具体的客户端/服务端连接说明见 [client.md](./client.md)。

## 运行入口

- `main.go` 解析启动参数，选择证书目录，创建 `app.App` 并启动服务。
- `app/app.go` 创建 Fiber 应用，注册路由，并用 TLS 监听端口。
- `GET /list` 返回当前房间列表。
- `GET /ws?id=<room_id>&key=<room_key>` 建立 WebSocket。未传 `id` 时会创建新房间；传入已有 `id` 时会尝试加入该房间。

常用参数：

```bash
go run . -p 28080
go run . -v
go run . --new-local-certs ./certs
go run . --local-certs ./certs
go run . --pem-certs ./certs
```

## 房间架构

### App

`App` 是服务根对象，持有：

- `fiberApp`：HTTP/WebSocket 路由和中间件。
- `NetListener`：TLS listener。
- `RoomManager`：所有房间的管理器。

`CreateFiberApp` 注册 `/list` 和 `/ws`。WebSocket 连接不会直接处理游戏逻辑，而是交给 `ServerHandler` 找到或创建房间，再交给 `Room.HandleClientJoin`。

### ServerHandler

`handlers.ServerHandler` 是 HTTP/WS 到房间系统的适配层。

- `/list` 调用 `RoomManager.GetRooms()`，返回 `room_id`、`need_key`、`player_count`、`game_state`。
- `/ws` 根据 query 中的 `id` 和 `key` 决定创建或加入房间。
- 房间不存在时创建房间并启动 `go room.Run()`。
- 房间存在时会检查密钥、人数上限和阶段。当前实现最多 2 人，且房间进入 `Preparing` 或更晚阶段后不再允许新玩家加入。

### RoomManager

`room-manager` 只管理房间索引和清理，不承载单局游戏逻辑。

- `RoomMap` 用 `sync.Map` 封装 `int -> *Room`。
- `GetNewRoomId()` 从 0 开始寻找未占用房间号。
- `AddRoom()` 创建 `room-atom.Room`，设置密钥并保存。
- `startCleaner()` 每分钟清理一次，也监听房间主动销毁信号。
- 清理条件包括：房间人数为 0、超过 5 分钟无互动、房间阶段达到 `STAGE_CLOSED`。

### Room Atom

`room-atom.Room` 是真正的原子房间。一个房间包含自己的状态机、玩家上下文、游戏逻辑校验器、消息通道和帧历史。

核心字段：

- `RoomCtx`：玩家表、房主 ID、下一服务端帧 ID、游戏 ticker、广播/单播方法。
- `Logic`：战斗内格子操作校验器，目前校验 6x9 网格以及同一帧同一格只接受一次操作。
- `GameStage`：原子阶段值。
- `ChapterID` / `StageID`：房主选择的章节和关卡。
- `Seed`：每场游戏的随机种子。
- `scheduledFrames`：按目标处理帧暂存玩家操作。
- `frameHistory`：已生成的服务端帧，用于按客户端 ACK 补发。

房间通过 `Run()` 启动状态机循环，统一消费：

- `register`：玩家加入。
- `unregister`：玩家断开。
- `incomingMessages`：玩家发来的 Protobuf 二进制请求。
- `GameTicker.C`：仅在 `InGame` 阶段启用的 50ms 游戏帧。

客户端对应的全局连接对象是 `BackendWS`，实现位于客户端 `src/utils/net/sync.ts`。客户端根据本地 `OnlineStateManager.currentGameStage` 决定把收到的二进制包按 `LobbyResponse`、`RoomResponse` 或 `InGameResponse` 解码，因此服务端同一条 WebSocket 连接在不同阶段发送的是不同响应外壳。

## 阶段和章节如何运转

房间状态定义在 `constants/stages.go`：

| 阶段 | 含义 | 主要入口 |
| --- | --- | --- |
| `STAGE_InLobby` | 大厅等待中，允许玩家加入 | 创建房间、加入房间、选择章节关卡 |
| `STAGE_Preparing` | 房主已选择章节关卡，玩家选装备并准备 | `RequestReady`、`RequestLeaveChooseMap` |
| `STAGE_Loading` | 全员准备完成，客户端加载地图资源 | `RequestLoaded` |
| `STAGE_InGame` | 全员加载完成，50ms 帧同步循环运行 | `RequestBlank`、种植、铲除、星之碎片、结束游戏 |
| `STAGE_PostGame` | 游戏结束后的结算阶段 | 当前会广播结算并停止 ticker |
| `STAGE_CLOSED` | 房间关闭 | 清理和销毁 |

章节和关卡不是独立的服务端模块，而是由房主在大厅阶段通过 `RequestChooseMap` 提交 `chapter_id` 和 `stage_id`。服务端记录到 `Room.ChapterID`、`Room.StageID`，广播 `ResponseChooseMap`，并把房间推进到 `Preparing`。后续客户端根据这两个字段进入选卡、加载对应地图和关卡资源。

完整流程：

1. 客户端连接 `/ws`。
2. 若未传 `id`，服务端创建房间；若传入已有 `id`，服务端校验密钥、人数和阶段后加入。
3. 服务端先返回 `LobbyResponse.JoinRoomSuccess`，包含 `room_id`、`my_id`、`key`。
4. 房间注册玩家后广播 `RoomResponse.RoomInfo`，包含 `room_id`、`lord_id`、当前玩家自己的 `my_id`、房间内 `peers`。
5. 房主在 `InLobby` 发送 `RequestChooseMap(chapter_id, stage_id)`。
6. 服务端记录章节/关卡，进入 `Preparing`，广播 `RoomResponse.ChooseMap`。
7. 玩家发送 `RequestReady(isReady)`。有人未准备时广播 `UpdateReadyCount`；全员准备后进入 `Loading`，分别发送 `AllReady`，其中包含 `seed`、当前接收者的 `my_id` 和排序后的 `player_ids`。
8. 客户端加载地图完成后发送 `RequestLoaded(isLoaded)`。
9. 全员 loaded 后服务端重置战斗帧状态，进入 `InGame`，广播 `AllLoaded`，启动 50ms ticker。
10. 游戏中客户端持续发送 `RequestBlank` 更新自己的 `frame_id` 和 `ack_frame_id`，并发送种植、铲除、星之碎片等操作。
11. 任意玩家发送 `RequestEndGame(game_result)` 时，服务端进入 `PostGame`，停止 ticker，广播 `GameEnd`。

房主可以在 `Preparing` 阶段发送 `RequestLeaveChooseMap` 退出选图/选卡，服务端会回到 `InLobby`，清空 `ChapterID` 和 `StageID`，并广播 `QuitChooseMap`。

客户端侧的对应行为：

- 设置页调用 `GET https://<baseUrl>/list` 刷新房间列表。
- 创建房间时连接 `wss://<baseUrl>/ws?key=<key>`。
- 加入房间时连接 `wss://<baseUrl>/ws?id=<room_id>&key=<key>`。
- 房主在关卡选择界面发送 `RequestChooseMap`；非房主只能等待 `RoomChooseMap` 事件进入选卡。
- 选卡确认后客户端发送 `RequestReady(true)`；当前 UI 会锁定已选卡组并等待其他玩家。
- Game 场景完成资源、音频和视频就绪检查后发送 `RequestLoaded(true)`。

## 游戏帧同步

当前实现是“服务端定时出帧 + 客户端 ACK 补发”的同步模型。

- 帧间隔为 `constants.FrameIntervalMs = 50ms`。
- 服务端当前帧由 `RoomCtx.NextFrameID` 推进，`currentServerFrameID()` 等于 `NextFrameID - 1`。
- 玩家操作默认安排到 `当前服务端帧 + CommandLeadFrames`，其中 `CommandLeadFrames = 4`。如果客户端请求的 `process_frame_id` 更晚，则尊重客户端请求。
- `advanceOneFrame()` 每 tick 生成一个 `InGameResponse`，对该帧操作排序、编号、写入 `frameHistory`，然后推进下一帧。
- `syncClientFrames()` 根据玩家 `LatestAckFrameID` 从历史帧中补发未确认帧，每次最多补发 `maxResendPerSync = 32` 帧。
- `pruneFrameHistory()` 会删除所有玩家都 ACK 过的历史帧，或超过 `maxHistoryFrames = 600` 的旧帧。

客户端战斗中也以 50ms 为固定步长。`SyncManager` 在 Game 场景加载时创建 `QueueReceive` 和 `QueueSend` 并绑定到 `BackendWS`；每个固定步先尝试消费下一帧 `InGameResponse`，如果下一帧还没到，联机模式会显示等待文本并用 `RequestBlank` 补发当前 `ack_frame_id`。客户端本地操作不会直接改变战局，而是先进入发送队列，等服务端回传同一帧操作后再由接收队列触发种植、铲除和星之碎片事件。

操作排序规则是确定性的：

1. `process_frame_id` 小的先处理。
2. 同帧内按类型：铲除、星之碎片、种植、游戏事件、错误。
3. 同类型按操作者 `uid`。
4. 最后按操作签名字符串兜底排序。

服务端不会模拟完整战局，只做轻量一致性校验：

- `PlantCard`、`RemoveCard`、`UseStarShards` 必须落在 6 行 9 列范围内。
- 同一目标处理帧中，同一格子只允许一个操作，先被记录的操作生效。
- 种植和星之碎片会根据客户端上报的资源总量与本次消耗做基础可负担校验。

## 支持的功能

- HTTPS + WebSocket 二进制 Protobuf 通信。
- 自动生成或加载本地 TLS 证书，也支持 `cert.pem` / `privkey.pem`。
- 房间创建、按 ID 加入、密钥校验。
- 房间列表查询。
- 双人房间人数限制。
- 房主识别和房间成员信息广播。
- 房主选择章节和关卡。
- 选卡准备、准备人数广播、全员准备通知。
- 地图加载确认、全员加载后开始游戏。
- 50ms 服务端帧循环。
- 游戏内种植、铲除、星之碎片操作转发。
- 基于 ACK 的历史帧补发。
- 游戏结束广播。
- 空房、闲置房、关闭房间的自动清理。
- 与当前客户端的 `LobbyResponse`、`RoomResponse`、`InGameResponse` 三段式解码流程兼容。

## 代码目录

```text
app/              Fiber/TLS 应用创建与启动
handlers/         HTTP 和 WebSocket 入口
room-manager/     房间索引、创建、销毁、定时清理
room-atom/        单个房间状态机、玩家管理、阶段流转、帧同步
room-atom/game-logic/
                  游戏内格子操作的轻量校验和响应生成
clients/          客户端连接上下文、玩家状态、消息池
constants/        阶段、帧间隔、应用信息等常量
messages/pb/      Protobuf 生成代码
types/            HTTP API 返回结构
utils/            证书、加密、消息写入等工具
scripts/          证书和 proto 生成脚本
```

# 客户端与服务端网络连接

本文基于客户端和当前服务端代码整理，描述联机模式下客户端如何连接服务端、如何流转房间阶段、以及游戏中如何同步帧。

## 客户端网络入口

客户端网络代码主要在：

```text
src/utils/net/lobby.ts        房间列表、创建房间、加入房间 URL
src/utils/net/sync.ts         BackendWS，全局 WebSocket 客户端
src/utils/net/handlers.ts     Protobuf 响应解码与事件派发
src/utils/net/room.ts         房间阶段请求封装
src/game/sync/queue_send.ts   游戏内请求发送队列
src/game/sync/queue_receive.ts 游戏内服务端帧接收队列
src/game/managers/combat/SyncManager.ts 战斗同步管理器
src/store/OnlineStateManager.ts 在线状态、房间信息、阶段缓存
```

`BackendWS` 是客户端全局连接对象。它持有真实 `WebSocket`，也能切到 `mockServer`，所以单机和联机复用了同一套请求/响应管道。

## HTTP 与 WebSocket

客户端设置页使用用户配置的 `baseUrl`：

- 刷新房间列表：`GET https://<baseUrl>/list`
- 创建房间：`wss://<baseUrl>/ws?key=<key>`
- 加入房间：`wss://<baseUrl>/ws?id=<room_id>&key=<key>`
- 检查证书可信：对 `https://<baseUrl>/list` 发 `HEAD`

服务端只提供 HTTPS/WSS。若使用自签名证书，浏览器必须先信任该证书，否则客户端无法请求 `/list` 或建立 WSS。

## 消息解码规则

WebSocket 使用二进制 Protobuf。客户端 `sync.ts` 收到 `ArrayBuffer` 后会根据在线状态和阶段选择解码方式：

| 客户端状态 | 解码类型 |
| --- | --- |
| 尚未进入在线模式 | `LobbyResponse` |
| `InLobby` / `Preparing` / `Loading` | `RoomResponse` |
| `InGame` | `InGameResponse` |

`LobbyResponse.JoinRoomSuccess` 会把客户端切到在线模式，并把阶段设为 `InLobby`。之后同一条 WebSocket 连接继续复用，直到断开或房间关闭。

## 房间阶段流

1. 设置页创建或加入房间，`BackendWS.startConnection()` 建立 WSS。
2. 服务端返回 `LobbyResponse.JoinRoomSuccess`，客户端保存 `roomId`、`myId`、`key`。
3. 服务端广播 `RoomResponse.RoomInfo`，客户端保存 `lordId`、`peers`，并通过 Phaser 事件通知 UI。
4. 房主选择关卡时，客户端发送 `RequestChooseMap(chapterId, stageId)`。非房主不会发送该请求。
5. 客户端收到 `RoomResponse.ChooseMap` 后把本地阶段设为 `Preparing`，进入选卡界面。
6. 玩家确认选卡后发送 `RequestReady(true)`。
7. 若尚未全员准备，服务端广播 `UpdateReadyCount`，客户端更新准备人数。
8. 全员准备后服务端发送 `AllReady`。客户端保存 `seed`、`myId`、`playerIds`，把本地阶段设为 `Loading`，并启动/进入 Game 场景加载资源。
9. Game 场景完成资源、音频、视频就绪检查后发送 `RequestLoaded(true)`。
10. 全员 loaded 后服务端发送 `AllLoaded`。客户端把阶段设为 `InGame`，发出 `RoomGameStart` 事件，战斗开始推进。
11. 任意玩家结束游戏时客户端发送 `RequestEndGame`，服务端广播 `GameEnd`，客户端进入 `PostGame`。

房主在 `Preparing` 可发送 `RequestLeaveChooseMap`。客户端收到 `QuitChooseMap` 后回到 `InLobby` 并重置选图/选卡 UI。

## 游戏内帧同步

客户端和服务端都使用 50ms 作为逻辑帧间隔：

- 客户端常量：`public/constants` 中 `FrameInterval = 50`
- 服务端常量：`constants.FrameIntervalMs = 50`

战斗开始时，`SyncManager.Load()` 创建：

- `QueueSend`：缓存客户端要发给服务端的 `Request`
- `QueueReceive`：缓存服务端发回的 `InGameResponse`

客户端固定步推进时：

1. `CombatManager.update()` 累积时间，每满 50ms 尝试推进一帧。
2. `SyncManager.update()` 调用 `recvQueue.Consume()`。
3. `QueueReceive` 只消费 `currentFrameId + 1` 对应的服务端帧。
4. 如果下一帧没有到，客户端暂停本次固定步，显示等待文本，并通过 `RequestBlank` 补发当前 ACK。
5. 如果下一帧到了，客户端按确定性顺序执行帧内操作，更新 `currentFrameId` 和 `lastAckFrameId`，并发送 `RequestBlank(frameId, ackFrameId)`。

客户端操作不会直接落地到战局。种植、铲除、星之碎片会先进入 `QueueSend`：

- `sendCardPlant(pid, col, row, level, cost)`
- `sendRemovePlant(pid, col, row)`
- `sendStarShards(pid, col, row, cost)`

这些请求会带上当前客户端帧、ACK 帧、目标格子、目标处理帧和资源总量。服务端校验后生成 `InGameOperation`，客户端收到对应 `InGameResponse` 后才通过事件真正执行。

## 目标处理帧

客户端生成操作时默认使用：

```text
processFrameId = 当前客户端帧 + 网络延迟估计帧数 + 2
```

服务端收到后会重新取更保守的值：

```text
processFrameId = max(客户端请求值, 当前服务端帧 + 4)
```

因此最终以服务端回传的 `InGameOperation.processFrameId` 为准。客户端如果看到服务端帧中的 `processFrameId` 和本地即将消费的帧不一致，会打印警告，但仍按服务端帧内容执行。

## ACK 与补发

客户端 `QueueReceive` 维护：

- `currentFrameId`：客户端已经推进到的逻辑帧。
- `lastAckFrameId`：客户端已经确认消费的服务端帧。
- `bufferedResponses`：已收到但尚未轮到消费的服务端帧。

每次发送 blank 或游戏操作时，`QueueSend` 都会刷新请求里的 `ackFrameId`。服务端据此从 `frameHistory` 补发客户端未确认的帧，每次最多补发 32 帧。

## 事件映射

服务端消息到客户端事件的大致对应：

| 服务端响应 | 客户端处理 |
| --- | --- |
| `JoinRoomSuccess` | 进入在线模式，缓存房间 ID 和玩家 ID |
| `RoomInfo` | 更新房主、玩家列表，触发 `RoomInfo` |
| `ChooseMap` | 阶段变为 `Preparing`，触发 `RoomChooseMap` |
| `QuitChooseMap` | 阶段回到 `InLobby`，触发 `RoomQuitChooseMap` |
| `UpdateReadyCount` | 触发 `RoomUpdateReadyCount` |
| `AllReady` | 缓存 seed/playerIds，阶段变为 `Loading`，触发 `RoomAllReady` |
| `AllLoaded` | 阶段变为 `InGame`，触发 `RoomGameStart` |
| `InGameResponse` | 进入 `QueueReceive`，按帧消费操作 |
| `GameEnd` | 战斗结束，触发 `RoomGameEnd` |
| `RoomClosed` | 重置在线状态并断开连接 |

## 单机 Mock 流程

单机不是完全绕过网络层。客户端会调用 `BackendWS.startSinglePlayerRoom()` 开启本地 `mockServer`，然后仍然发送 `RequestChooseMap`、`RequestReady` 等 Protobuf 请求。mock server 返回和真实服务端类似的 `LobbyResponse`、`RoomResponse`、`InGameResponse`，让战斗代码复用联机同步队列。

## 当前注意点

- 客户端只发送二进制 Protobuf；非二进制 WS 消息会被忽略。
- 客户端根据本地阶段选择解码类型，因此服务端阶段响应必须与客户端阶段推进保持一致。
- 客户端 UI 目前在玩家点击准备后会锁定选卡，虽然协议上 `RequestReady(false)` 存在，但当前 UI 没有常规取消准备入口。
- 客户端 `RoomInfo.peers` 期望是 JSON 字符串数组，元素形如 `{ "addr": string, "id": number }`。
- 联机时暂停和倍速被禁用，战斗推进依赖服务端帧是否连续到达。

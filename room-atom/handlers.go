package roomatom

import (
	"encoding/base64"
	"log"
	"mvzserver/clients"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"mvzserver/utils"

	"runtime/debug"
	"sort"
	"time"

	"github.com/gofiber/websocket/v2"
	"google.golang.org/protobuf/proto"
)

type Player = clients.Player
type PlayerMessage = clients.PlayerMessage

// 单次操作允许的最大消耗，用于拦截负数成本与明显异常的自报值
const maxOperationCost int32 = 100000

// WS入口
// userName 来自连接参数，仅用于等待房间展示
func (room *Room) HandleClientJoin(c *websocket.Conn, userName string) {
	log.Printf("🟢 Client connecting to room %d", room.ID)

	// 检查连接是否有效
	if c == nil {
		log.Printf("🔴 WebSocket connection is nil")
		return
	}
	log.Printf("🟢 WebSocket connection is valid")

	ctx := room.RoomCtx.CreateClientCtxFromConn(c)
	log.Printf("🟢 Created client context with ID %d", ctx.Id)

	// 再次检查连接
	if !ctx.IsConnected() {
		log.Printf("🔴 Connection became nil after creating context")
		return
	}
	log.Printf("🟢 Connection is still valid after creating context")

	var payload = messages.ResponseJoinRoomSuccess{
		RoomId:  uint32(room.ID),
		MyId:    uint32(ctx.Id),
		Key:     room.key,
		Message: "加入房间成功",
	}
	var resp = messages.LobbyResponse{
		Payload: &messages.LobbyResponse_JoinRoomSuccess{
			JoinRoomSuccess: &payload,
		},
	}
	log.Printf("🟢 Sending joinRoomSuccess to client %d", ctx.Id)
	utils.WriteLobbyResponse(c, &resp)
	log.Printf("🟢 joinRoomSuccess sent to client %d", ctx.Id)

	// 检查连接在发送响应后的状态
	if !ctx.IsConnected() {
		log.Printf("🔴 Connection became nil after sending response")
		return
	}
	log.Printf("🟢 Connection is still valid after sending response")

	// 创建一个新的玩家实例
	p := clients.NewPlayer(ctx)
	p.Name = utils.NormalizeUserName(userName)
	if p.Name == "" {
		p.Name = "player"
	}
	p.JoinKey = room.key

	// 最后检查连接
	if !p.Ctx.IsConnected() {
		log.Printf("🔴 Connection became nil after creating player")
		return
	}
	log.Printf("🟢 Connection is still valid after creating player")

	log.Printf("🟢 Registering player %d first", ctx.Id)
	room.register <- &p // 先将玩家加入到注册通道
	log.Printf("🟢 Player %d registration sent to channel", ctx.Id)

	// **关键修复**：直接在这里处理客户端，而不是在 goroutine 中
	log.Printf("🟢 Starting client service for player %d directly", ctx.Id)
	room.StartServeClient(&p) // 阻塞直到连接关闭
	log.Printf("🟢 Client service ended for player %d", ctx.Id)
}

// ---------------------------
// 状态机循环的处理函数
// ---------------------------

func (room *Room) handleRegister(player *clients.Player) {
	log.Printf("🔵 Processing registration for player %d (%s)", player.GetID(), player.Name)

	// 更新房间活跃时间
	room.UpdateActiveTime()

	// 等待房间与选卡阶段都允许加入；加载与游戏阶段拒绝
	if room.GameStage.IsLaterThanOrEqual(constants.STAGE_Loading) {
		log.Printf("🔴 Registration rejected for player %d - room stage %d", player.GetID(), room.GameStage.Load())
		room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
			Payload: &messages.RoomResponse_RoomClosed{
				RoomClosed: &messages.ResponseRoomClosed{Message: "房间已开始游戏，无法加入"},
			},
		}, player)
		if player.Ctx != nil {
			player.Ctx.Close()
		}
		return
	}

	// 主循环内再校验一次人数：HTTP 入口的检查与注册事件之间可能有并发加入。
	if room.GetPlayerCount() >= constants.RoomCapacity {
		room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
			Payload: &messages.RoomResponse_RoomClosed{
				RoomClosed: &messages.ResponseRoomClosed{Message: "房间已满"},
			},
		}, player)
		if player.Ctx != nil {
			player.Ctx.Close()
		}
		return
	}

	room.RoomCtx.AddUser(player)
	room.broadcastRoomInfo()
	// 已经在选卡的房间：新加入的玩家直接进入选卡界面
	if room.GameStage.EqualTo(constants.STAGE_Preparing) {
		room.sendChooseMapTo(player)
	}
	room.broadcastReadyCount()
}

func (room *Room) handleUnregister(player *clients.Player) {
	if player == nil || player.Ctx == nil {
		return
	}

	// InGame 断线保留席位，等待同 id 重连；宽限期内可补帧恢复。
	if room.GameStage.EqualTo(constants.STAGE_InGame) {
		room.markPlayerDisconnected(player)
		return
	}

	leavingID := player.Ctx.Id
	room.RoomCtx.DelUser(leavingID) // 向 context 中注销用户
	if player.Ctx.IsConnected() {
		player.Ctx.Close()
	}
	room.transferOwnerIfNeeded(leavingID)

	// 最后一个玩家离开：立即销毁房间并从 manager 移除，
	// 不再让它以 0 人状态留在房间列表里等待分钟级清理。
	if room.GetPlayerCount() == 0 {
		log.Printf("🧹 房间 %d 已无玩家，立即销毁", room.ID)
		room.Destroy()
		return
	}
	// 等待房间/选卡阶段有人离开：一律回到等待房间并清空所有人的准备状态，
	// 避免剩下的人停留在“已准备”而无法重新推进。
	stage := room.GameStage.Load()
	if stage == constants.STAGE_InLobby || stage == constants.STAGE_Preparing {
		room.resetWaitingRoom(stage == constants.STAGE_Preparing)
		return
	}
	room.broadcastRoomInfo()
	room.broadcastReadyCount()
}

// handleReconnect 在房间主循环内把新连接绑定到保留的玩家席位。
func (room *Room) handleReconnect(request *ReconnectRequest) {
	reply := func(player *clients.Player) {
		if request.Reply != nil {
			request.Reply <- player
		}
	}

	if request == nil {
		reply(nil)
		return
	}
	stage := room.GameStage.Load()
	if stage >= constants.STAGE_CLOSED {
		reply(nil)
		return
	}

	var target *clients.Player
	room.RoomCtx.Players.Range(func(_ int, value *clients.Player) bool {
		if value != nil && value.Ctx != nil && value.Ctx.Id == request.PlayerID {
			target = value
			return false
		}
		return true
	})
	// 房主改过密码后，老成员手里还是加入时的密钥，用 JoinKey 放行
	if target == nil || (target.JoinKey != request.Key && room.key != request.Key) {
		reply(nil)
		return
	}

	// InGame：保留帧进度并从 checkpoint 补帧。
	// 其他阶段（大厅/选卡/加载）：直接重新入座并同步当前房间状态。
	if room.GameStage.EqualTo(constants.STAGE_InGame) {
		newCtx := clients.NewClientCtx(request.Conn, request.PlayerID)
		// 新的连接上下文从保守的权威帧开始：
		// - 有 checkpoint：从 checkpoint 帧恢复后重放 checkpoint+1..；
		// - 无 checkpoint：从 0 或最早历史帧开始重放完整历史。
		//
		// 不能直接继承旧上下文的 ACK。页面刷新后的新客户端尚未执行任何帧，
		// 继承旧 ACK 会让服务端把“新客户端没跑过的帧”当成双方已确认的权威帧，
		// 从而提前比较摘要/裁剪历史。
		//
		// 同进程重连的客户端会在 socket open 后立刻发送自己的当前 ACK，
		// UpdatePlayerFrame 是单调的，会把它真实执行到的帧重新抬上去。
		resumeFrame := room.LastCheckpointFrame
		resumeProgressFrame := room.LastCheckpointFrame
		newCtx.LatestFrameID.Store(resumeProgressFrame)
		newCtx.LatestAckFrameID.Store(resumeFrame)
		newCtx.StateHashVersion.Store(target.Ctx.StateHashVersion.Load())
		newCtx.StateHashVersionReported.Store(target.Ctx.StateHashVersionReported.Load())
		newCtx.ReportedLatencyMs.Store(target.Ctx.ReportedLatencyMs.Load())
		newCtx.State = constants.PlayerStateConnected

		oldCtx := target.Ctx
		target.Ctx = newCtx
		target.LastSyncedFrameID.Store(resumeProgressFrame)
		target.ConnectionEpoch++
		if oldCtx.IsConnected() {
			oldCtx.Close()
		}

		log.Printf("🟢 玩家 %d 重连成功，从 checkpoint=%d 继续", request.PlayerID, resumeFrame)
		room.onPlayerReconnected(target)
		// 页面刷新/新场景需要种子与 loadout；随后 roomAllSync 会把阶段切回 InGame。
		// 先补一份房间信息，让客户端拿到房间号、房主、章节关卡与当前 peers。
		room.sendRoomInfoTo(target, room.buildPeersJSON())
		room.sendAllReadyTo(target)
	// 先把阶段切成 InGame，让新连接的消息进入房间通道。
	room.sendRoomAllSyncTo(target)
	room.sendReplayFromCheckpoint(target)
		// 备用恢复：向最低延迟在线玩家索取最新快照，收到后追加转发。
		// 即使 checkpoint 不存在或已过期，客户端也能从在线玩家的当前状态快速恢复。
		room.requestReconnectSnapshot(target)
	} else {
		newCtx := clients.NewClientCtx(request.Conn, request.PlayerID)
		newCtx.State = constants.PlayerStateConnected
		if !room.rebindPlayerInLobby(target, newCtx) {
			reply(nil)
			return
		}
	}
	reply(target)
}

// ReconnectPlayer 供 HTTP goroutine 调用，把连接切换请求投递给房间主循环。
func (room *Room) ReconnectPlayer(conn *websocket.Conn, playerID int, key string) *clients.Player {
	reply := make(chan *clients.Player, 1)
	request := &ReconnectRequest{Conn: conn, PlayerID: playerID, Key: key, Reply: reply}
	select {
	case room.reconnect <- request:
	case <-time.After(3 * time.Second):
		return nil
	}
	select {
	case player := <-reply:
		return player
	case <-time.After(3 * time.Second):
		return nil
	}
}

// 处理玩家WS消息
func (room *Room) handlePlayerMessage(msg *PlayerMessage) {
	defer func() {
		// release to pool
		clients.ReleasePlayerMessage(msg)

		if r := recover(); r != nil {
			// 发生了 panic，r 是 panic 的值
			log.Printf("处理用户信息时捕获到 Panic: %v\n", r)

			// 打印详细的堆栈信息，对于调试非常重要
			// debug.Stack() 返回格式化好的当前 goroutine 的堆栈跟踪
			log.Printf("堆栈信息:\n%s", string(debug.Stack()))

			// 在这里，你可以执行一些额外的清理工作，或者上报错误到监控系统
			log.Println("程序已从 panic 中恢复，将继续运行。")
		}
	}()

	// 更新房间活跃时间 - 任何玩家消息都表示房间是活跃的
	room.UpdateActiveTime()

	// decode
	var request messages.Request

	// 2. 反序列化 (解码) 收到的二进制数据
	if err := proto.Unmarshal(msg.Data, &request); err != nil {
		log.Printf("Failed to unmarshal request: %v", err)
		recordDesyncEvent(room.ID, "request_unmarshal_failed", map[string]any{
			"playerId":  msg.Player.GetID(),
			"error":     err.Error(),
			"rawBase64": base64.StdEncoding.EncodeToString(msg.Data),
		})
		return
	}

	// 验证 payload 不为 nil
	if request.Payload == nil {
		log.Printf("🔴 Request payload is nil for player %d", msg.Player.GetID())
		return
	}

	if shouldLogRequest(&request) {
		fields := map[string]any{
			"playerId": msg.Player.GetID(),
			"payload":  describeRequest(&request),
			"stage":    room.GameStage.Load(),
			"receivedServerFrameId": room.currentServerFrameID(),
			"receivedCommittedFrameId": room.committedFrameID(),
			"players":  room.playerSyncSnapshot(),
			// 原始字节主要用于离线复现异常帧；只有被采样的 blank 或非心跳请求会走到这里。
			"rawBase64": base64.StdEncoding.EncodeToString(msg.Data),
		}
		recordDesyncEvent(room.ID, "request", fields)
	}

	switch payload := request.Payload.(type) {
	// InLobby
	case *messages.Request_ChooseMap:
		room.HandleRequestChooseMap(msg.Player, payload.ChooseMap)
	// Preparing
	case *messages.Request_Ready:
		room.HandleRequestReady(msg.Player, payload.Ready)
	case *messages.Request_LeaveChooseMap:
		room.HandleRequestLeaveChooseMap(msg.Player, payload.LeaveChooseMap)
	// Loading
	case *messages.Request_Loaded:
		room.HandleRequestLoaded(msg.Player, payload.Loaded)
	case *messages.Request_PlayerLoaded:
		room.HandleRequestPlayerLoaded(msg.Player, payload.PlayerLoaded)
	// InGame
	case *messages.Request_Blank:
		// 心跳在所有阶段都只用于续活。InGame 之外没有权威帧，交给
		// HandleRequestBlank 会走 syncAllClients 并可能误伤非对局阶段；
		// 读循环已经在 SafeReadMessage 里刷新心跳时间，这里只需忽略即可。
		if room.GameStage.EqualTo(constants.STAGE_InGame) || room.EndGameGraceActive {
			room.HandleRequestBlank(msg.Player, payload.Blank)
		}
	case *messages.Request_Plant:
		room.HandleRequestCardPlant(msg.Player, payload.Plant)
	case *messages.Request_RemovePlant:
		room.HandleRequestRemovePlant(msg.Player, payload.RemovePlant)
	case *messages.Request_StarShards:
		room.HandleRequestStarShards(msg.Player, payload.StarShards)
	case *messages.Request_EndGame:
		room.HandleRequestEndGame(msg.Player, payload.EndGame)
	// 等待房间内的房间管理
	case *messages.Request_UpdateRoom:
		room.HandleRequestUpdateRoom(msg.Player, payload.UpdateRoom)
	case *messages.Request_KickPlayer:
		room.HandleRequestKickPlayer(msg.Player, payload.KickPlayer)
	// PostGame 结算后返回等待房间
	case *messages.Request_ReturnToLobby:
		room.HandleRequestReturnToLobby(msg.Player, payload.ReturnToLobby)
	// 重连恢复：在线客户端上传快照
	case *messages.Request_SnapshotResponse:
		room.HandleRequestSnapshotResponse(msg.Player, payload.SnapshotResponse)

	default:
		// 未知请求类型
	}
}

func (room *Room) HandleRequestReady(player *Player, req *messages.RequestReady) {
	log.Printf("🔵 HandleRequestReady: Player %d, isReady: %v, current stage: %d",
		player.GetID(), req.IsReady, room.GameStage.Load())

	// InLobby：等待房间准备（全员准备后进入选卡）
	// Preparing：选卡确认（全员确认后进入加载）
	stage := room.GameStage.Load()
	if stage != constants.STAGE_InLobby && stage != constants.STAGE_Preparing {
		log.Printf("🔴 HandleRequestReady: Rejected - room stage %d", stage)
		return
	}
	// 等待房间里房主不点“准备”：由房主的“开始”按钮推进到选卡阶段。
	if stage == constants.STAGE_InLobby && room.RoomCtx.OwnerUserID == player.Ctx.Id {
		return
	}

	player.IsReady = req.IsReady
	if req.IsReady {
		player.Loadout = req.Loadout
	}
	log.Printf("🔵 HandleRequestReady: Set player %d isReady to %v", player.GetID(), req.IsReady)

	if stage == constants.STAGE_InLobby {
		room.broadcastRoomInfo()
		room.broadcastReadyCount()
		return
	}

	room.broadcastRoomInfo()

	// 检查所有玩家状态
	totalPlayers := room.GetPlayerCount()
	readyCount := 0
	room.RoomCtx.Players.Range(func(key int, value *clients.Player) bool {
		if value.IsReady {
			readyCount++
		}
		log.Printf("🔵 Player %d: isReady=%v", key, value.IsReady)
		return true
	})

	log.Printf("🔵 HandleRequestReady: %d/%d players ready", readyCount, totalPlayers)

	// 判断是否所有玩家都准备就绪
	if room.HasAllPlayerReady() {
		log.Printf("🟢 All players ready! Starting loading phase...")
		// 如果所有玩家都准备好了，开始加载
		room.GameStage.Store(constants.STAGE_Loading)
		playerIDs := make([]uint32, 0, totalPlayers)
		room.RoomCtx.Players.Range(func(_ int, value *clients.Player) bool {
			if value != nil && value.Ctx != nil {
				playerIDs = append(playerIDs, uint32(value.Ctx.Id))
			}
			return true
		})
		sort.Slice(playerIDs, func(i, j int) bool { return playerIDs[i] < playerIDs[j] })
		canonicalLoadouts := room.canonicalPlayerLoadouts()

		room.RoomCtx.Players.Range(func(_ int, value *clients.Player) bool {
			if value == nil || value.Ctx == nil {
				return true
			}
			payload := messages.ResponseAllReady{
				AllPlayerCount: room.GetPlayerCount(),
				Seed:           room.Seed,
				MyId:           uint32(value.Ctx.Id),
				PlayerIds:      playerIDs,
				Loadouts:       canonicalLoadouts,
				RoomId:         uint32(room.ID),
				ChapterId:      room.ChapterID,
				StageId:        room.StageID,
				Resources:      room.captureResourceProfiles(),
			}
			resp := &messages.RoomResponse{
				Payload: &messages.RoomResponse_AllReady{
					AllReady: &payload,
				},
			}
			room.RoomCtx.SendMessageToUserByPlayer(resp, value)
			return true
		})
		log.Printf("🟢 AllReady message sent")
	} else {
		log.Printf("🟡 Not all players ready yet, sending ready count update...")
		// 发送一次信息， 提醒已经准备的人数
		payload := messages.ResponseUpdateReadyCount{
			Count:          room.PlayerReadyCount(),
			AllPlayerCount: room.GetPlayerCount(),
		}
		resp := &messages.RoomResponse{
			Payload: &messages.RoomResponse_UpdateReadyCount{
				UpdateReadyCount: &payload,
			},
		}
		room.RoomCtx.BroadcastMessage(resp, nil)
		log.Printf("🟡 Ready count update sent: %d/%d", readyCount, totalPlayers)
	}
}

func (room *Room) HandleRequestChooseMap(player *Player, req *messages.RequestChooseMap) {
	// 只有房主可以推进到选卡阶段
	if player == nil || player.Ctx == nil || room.RoomCtx.OwnerUserID != player.Ctx.Id {
		return
	}
	// 只能在 STAGE_InLobby 阶段处理
	if !room.GameStage.EqualTo(constants.STAGE_InLobby) {
		return
	}
	// 只有其他成员全部准备，房主才能开始。
	if !room.HasAllNonOwnerReady() {
		room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
			Payload: &messages.RoomResponse_Error{
				Error: &messages.ResponseErrorMessage{Message: "其他玩家尚未准备，无法开始"},
			},
		}, player)
		return
	}

	// 更新房间活跃时间 - 选择地图是重要的游戏进展
	room.UpdateActiveTime()

	// 房间在创建时已绑定关卡，客户端传来的关卡只在未绑定时生效
	if room.ChapterID == 0 && room.StageID == 0 {
		room.ChapterID = req.ChapterId
		room.StageID = req.StageId
	}
	room.GameStage.Store(constants.STAGE_Preparing)
	room.resetReadyFlags()
	room.broadcastRoomInfo()
	room.broadcastChooseMap()
	room.broadcastReadyCount()
}

func (room *Room) HandleRequestLeaveChooseMap(player *Player, req *messages.RequestLeaveChooseMap) {
	if player == nil || player.Ctx == nil {
		return
	}
	// 双人合作房间：任何成员都可以从选卡退回等待房间
	// 只能在 STAGE_Preparing 阶段处理
	if !room.GameStage.EqualTo(constants.STAGE_Preparing) {
		return
	}

	// 更新房间活跃时间 - 离开选卡也是游戏操作
	room.UpdateActiveTime()

	// 绑定关卡跨局保留，回到等待房间即可重新选卡
	room.returnToLobby()
}

// HandleRequestUpdateRoom 房主在等待房间内修改房间名与密码。
func (room *Room) HandleRequestUpdateRoom(player *Player, req *messages.RequestUpdateRoom) {
	if player == nil || player.Ctx == nil || req == nil {
		return
	}
	if room.RoomCtx.OwnerUserID != player.Ctx.Id {
		return
	}
	if room.GameStage.IsLaterThanOrEqual(constants.STAGE_Loading) {
		return
	}

	name := utils.NormalizeRoomName(req.Name)
	if name == "" {
		name = room.Name
	}
	room.SetName(name)
	room.SetKey(utils.NormalizeRoomKey(req.Key))
	room.UpdateActiveTime()
	log.Printf("🔵 Room %d updated: name=%s needKey=%v", room.ID, room.Name, room.HasKey())
	room.broadcastRoomInfo()
}

// HandleRequestKickPlayer 房主把指定玩家移出房间。
func (room *Room) HandleRequestKickPlayer(player *Player, req *messages.RequestKickPlayer) {
	if player == nil || player.Ctx == nil || req == nil {
		return
	}
	if room.RoomCtx.OwnerUserID != player.Ctx.Id {
		return
	}
	targetID := int(req.PlayerId)
	if targetID == player.Ctx.Id {
		return
	}
	target, ok := room.RoomCtx.Players.Load(targetID)
	if !ok || target == nil {
		return
	}

	log.Printf("🔵 Room %d kicking player %d", room.ID, targetID)
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_RoomClosed{
			RoomClosed: &messages.ResponseRoomClosed{Message: "你已被房主移出房间"},
		},
	}, target)
	room.RoomCtx.DelUser(targetID)
	if target.Ctx != nil {
		// 读循环阻塞在 ReadMessage 上持有读锁，Close 会阻塞房间主循环，这里直接关闭底层连接
		target.Ctx.ForceClose()
	}

	// 等待房间/选卡阶段踢人后同样清空准备状态，避免残留 ready 卡住流程
	stage := room.GameStage.Load()
	if stage == constants.STAGE_InLobby || stage == constants.STAGE_Preparing {
		room.resetWaitingRoom(stage == constants.STAGE_Preparing)
		return
	}
	room.broadcastRoomInfo()
	room.broadcastReadyCount()
}

func (room *Room) HandleRequestLoaded(player *Player, req *messages.RequestLoaded) {
	log.Printf("🔵 HandleRequestLoaded: Player %d, isLoaded: %v, current stage: %d",
		player.GetID(), req.IsLoaded, room.GameStage.Load())

	// 只能在 STAGE_Loading 阶段处理
	if !room.GameStage.EqualTo(constants.STAGE_Loading) {
		log.Printf("🔴 HandleRequestLoaded: Rejected - room not in Loading stage, current: %d",
			room.GameStage.Load())
		return
	}

	player.IsLoaded = req.IsLoaded
	if req.IsLoaded {
		player.LoadProgress = 100
	}
	room.startInGameFromLoading()
}

// HandleRequestPlayerLoaded 处理加载进度上报。
// progress 是 1..100 的整数，100 表示玩家资源与战局已初始化完毕、进入等待同步。
// 进度单调递增，避免乱序包把进度回退；达到 100 时置 IsLoaded 并广播 roomSync。
func (room *Room) HandleRequestPlayerLoaded(player *Player, req *messages.RequestPlayerLoaded) {
	if player == nil || req == nil || !room.GameStage.EqualTo(constants.STAGE_Loading) {
		return
	}

	progress := req.Progress
	if progress > 100 {
		progress = 100
	}
	if progress > player.LoadProgress {
		player.LoadProgress = progress
	}
	recordDesyncEvent(room.ID, "player_loaded", map[string]any{
		"playerId":     player.Ctx.Id,
		"progress":     progress,
		"loadProgress": player.LoadProgress,
		"isLoaded":     player.IsLoaded,
		"players":      room.playerSyncSnapshot(),
	})
	if player.LoadProgress >= 100 {
		player.IsLoaded = true
	}
	room.UpdateActiveTime()
	room.broadcastRoomSync()
	room.startInGameFromLoading()
}

// 判断游戏中的客户端发来帧是否可以采用以及更新frame状态
// Return: 这个帧是否可以采用（没有迟到）
func (room *Room) HandleRequestBlank(player *Player, req *messages.RequestBlank) bool {
	if !room.acceptInGameBase(player, req) {
		// 结束后保留短暂补发窗口，让落后客户端补齐到权威结束帧
		if !room.EndGameGraceActive || player == nil || player.Ctx == nil || req == nil {
			return false
		}
		if room.EndGameFrameID != 0 && req.FrameId > room.EndGameFrameID+constants.CommandLeadFrames {
			return false
		}
		player.Ctx.UpdatePlayerFrame(req.FrameId, req.AckFrameId)
	}
	room.pruneFrameHistory()
	// 心跳只做增量补发。ReplayFrames 只应在重连时发送一次；
	// 每个 blank 都重放 checkpoint 之后的全部帧会把客户端缓冲打满并造成丢帧。
	room.syncAllClients(true)
	return true
}

func (room *Room) HandleRequestCardPlant(player *Player, req *messages.RequestCardPlant) {
	// 检查 req 和其嵌套字段是否为 nil
	if req == nil {
		log.Printf("🔴 HandleRequestCardPlant: req is nil for player %d", player.GetID())
		return
	}
	if req.Base == nil {
		log.Printf("🔴 HandleRequestCardPlant: req.Base is nil for player %d", player.GetID())
		return
	}
	if req.Base.Base == nil {
		log.Printf("🔴 HandleRequestCardPlant: req.Base.Base is nil for player %d", player.GetID())
		return
	}

	if !room.acceptInGameBase(player, req.Base.Base) {
		return
	}

	// 此帧想要发生的时机晚于下一帧序号，但不允许调度过远的未来帧
	processFrameID, validFrame := room.resolveProcessFrameID(req.Base)
	requestedProcessFrameID := req.Base.ProcessFrameId
	if !validFrame {
		room.rejectOperation(player, req.Base, processFrameID, "process_frame_id 超出允许窗口")
		return
	}
	req.Base.ProcessFrameId = processFrameID
	recordOperationExecutionFrame(room, player, "plant", req.Base, processFrameID, requestedProcessFrameID)

	if req.Cost < 0 || req.Cost > maxOperationCost || req.EnergySum < 0 || req.StarShardsSum < 0 {
		room.rejectOperation(player, req.Base, processFrameID, "非法的消耗或资源数值")
		return
	}
	operation := room.Logic.PlantCard(player, req)
	room.recordOperationSubmission(req.Base.ProcessFrameId, operation)
	room.pruneFrameHistory()
	room.syncAllClients(true)
}

func (room *Room) HandleRequestRemovePlant(player *Player, req *messages.RequestRemovePlant) {
	// 检查 req 和其嵌套字段是否为 nil
	if req == nil {
		log.Printf("🔴 HandleRequestRemovePlant: req is nil for player %d", player.GetID())
		return
	}
	if req.Base == nil {
		log.Printf("🔴 HandleRequestRemovePlant: req.Base is nil for player %d", player.GetID())
		return
	}
	if req.Base.Base == nil {
		log.Printf("🔴 HandleRequestRemovePlant: req.Base.Base is nil for player %d", player.GetID())
		return
	}

	if !room.acceptInGameBase(player, req.Base.Base) {
		return
	}
	// 此帧想要发生的时机晚于下一帧序号，但不允许调度过远的未来帧
	processFrameID, validFrame := room.resolveProcessFrameID(req.Base)
	requestedProcessFrameID := req.Base.ProcessFrameId
	if !validFrame {
		room.rejectOperation(player, req.Base, processFrameID, "process_frame_id 超出允许窗口")
		return
	}
	req.Base.ProcessFrameId = processFrameID
	recordOperationExecutionFrame(room, player, "removePlant", req.Base, processFrameID, requestedProcessFrameID)

	operation := room.Logic.RemoveCard(player, req)
	room.recordOperationSubmission(req.Base.ProcessFrameId, operation)
	room.pruneFrameHistory()
	room.syncAllClients(true)
}

func (room *Room) HandleRequestStarShards(player *Player, req *messages.RequestStarShards) {
	// 检查 req 和其嵌套字段是否为 nil
	if req == nil {
		log.Printf("🔴 HandleRequestStarShards: req is nil for player %d", player.GetID())
		return
	}
	if req.Base == nil {
		log.Printf("🔴 HandleRequestStarShards: req.Base is nil for player %d", player.GetID())
		return
	}
	if req.Base.Base == nil {
		log.Printf("🔴 HandleRequestStarShards: req.Base.Base is nil for player %d", player.GetID())
		return
	}

	if !room.acceptInGameBase(player, req.Base.Base) {
		return
	}

	// 此帧想要发生的时机晚于下一帧序号，但不允许调度过远的未来帧
	processFrameID, validFrame := room.resolveProcessFrameID(req.Base)
	requestedProcessFrameID := req.Base.ProcessFrameId
	if !validFrame {
		room.rejectOperation(player, req.Base, processFrameID, "process_frame_id 超出允许窗口")
		return
	}
	req.Base.ProcessFrameId = processFrameID
	recordOperationExecutionFrame(room, player, "starShards", req.Base, processFrameID, requestedProcessFrameID)

	if req.Cost < 0 || req.Cost > maxOperationCost || req.EnergySum < 0 || req.StarShardsSum < 0 {
		room.rejectOperation(player, req.Base, processFrameID, "非法的消耗或资源数值")
		return
	}
	operation := room.Logic.UseStarShards(player, req)
	room.recordOperationSubmission(req.Base.ProcessFrameId, operation)
	room.pruneFrameHistory()
	room.syncAllClients(true)
}

func (room *Room) HandleRequestEndGame(player *Player, req *messages.RequestEndGame) {
	// 只能在 STAGE_InGame 阶段处理
	if !room.GameStage.EqualTo(constants.STAGE_InGame) {
		return
	}
	if req == nil || req.GameResult > 1 {
		return
	}

	// 不在这里直接结算：先记录投票，等待其他玩家确认或超时，
	// 再由服务端把结束事件写入统一逻辑帧（见 scheduleAuthoritativeEnd）。
	room.RegisterEndGameVote(player, req.GameResult)
}

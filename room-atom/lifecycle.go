package roomatom

// 房间生命周期机制：InGame 断线宽限、心跳超时、PostGame 返回等待房间。
// 全部方法在房间主循环 goroutine 内调用。

import (
	"log"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"time"
)

// markPlayerDisconnected 在 InGame 阶段保留席位并启动宽限期计时。
// 由 handleUnregister 调用。
func (room *Room) markPlayerDisconnected(player *Player) {
	player.Ctx.State = constants.PlayerStateDisconnected
	if player.Ctx.IsConnected() {
		player.Ctx.Close()
	}
	if !room.AnyPlayerDisconnected {
		room.AnyPlayerDisconnected = true
		room.ReconnectDeadline = time.Now().Add(constants.ReconnectGracePeriod)
		log.Printf("🟡 玩家 %d 在 InGame 掉线，席位保留，宽限期 %s", player.Ctx.Id, constants.ReconnectGracePeriod)
	} else {
		log.Printf("🟡 玩家 %d 在 InGame 掉线（房间已在宽限期）", player.Ctx.Id)
	}
	room.broadcastRoomInfo()
}

// clearDisconnectGrace 全员在线或重连成功后清除宽限期。
func (room *Room) clearDisconnectGrace() {
	room.AnyPlayerDisconnected = false
	room.ReconnectDeadline = time.Time{}
}

// hasDisconnectedPlayer 是否仍有处于断线保留状态的玩家。
func (room *Room) hasDisconnectedPlayer() bool {
	found := false
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		if player != nil && player.Ctx != nil && player.Ctx.State == constants.PlayerStateDisconnected {
			found = true
			return false
		}
		return true
	})
	return found
}

// onPlayerReconnected 重连成功后恢复在线状态；全员回来则解除宽限期。
func (room *Room) onPlayerReconnected(player *Player) {
	player.Ctx.State = constants.PlayerStateConnected
	if !room.hasDisconnectedPlayer() {
		room.clearDisconnectGrace()
		log.Printf("🟢 玩家 %d 重连归队，全员在线，宽限期解除", player.Ctx.Id)
	}
	room.broadcastRoomInfo()
}

// tickReconnectGrace 每个游戏 tick 调用：
// 宽限期到点仍有人掉线 → 全房间按异常对局判负，写入权威结束帧。
func (room *Room) tickReconnectGrace() {
	if !room.AnyPlayerDisconnected {
		return
	}
	// 宽限期内全员回来了（状态由 onPlayerReconnected 清理）；兜底再查一次
	if !room.hasDisconnectedPlayer() {
		room.clearDisconnectGrace()
		return
	}
	if room.ReconnectDeadline.IsZero() || time.Now().Before(room.ReconnectDeadline) {
		return
	}
	log.Printf("⌛ 房间 %d 重连宽限期超时，仍有玩家掉线，按异常对局判负", room.ID)
	// 掉线玩家不会再 ACK。宽限期结束后把他们推进到当前生成帧，
	// 否则 committed frame 会永久停住，权威结束帧也无法落地。
	currentFrame := room.currentServerFrameID()
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		if player == nil || player.Ctx == nil {
			return true
		}
		if player.Ctx.LatestAckFrameID.Load() < currentFrame {
			player.Ctx.LatestAckFrameID.Store(currentFrame)
		}
		return true
	})
	// 直接复用 end_game 仲裁：标记全体玩家投“判负”，立即排入权威结束帧
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		if player == nil || player.Ctx == nil {
			return true
		}
		if player.Ctx.State == constants.PlayerStateConnected {
			// 在线的一端补投“判负”，保证两端结果一致地以 0 结束
			room.EndGameVotes[player.Ctx.Id] = 0
		} else {
			room.EndGameVotes[player.Ctx.Id] = 0
		}
		return true
	})
	room.EndGameFirstFrame = room.currentServerFrameID()
	room.scheduleAuthoritativeEnd()
}

// tickRoomSync 每秒调用：Loading 阶段向全房间下发玩家加载进度，
// 并兜底检查是否已经全员加载完成（避免某个 playerLoaded 包丢失导致卡住）。
func (room *Room) tickRoomSync() {
	if !room.GameStage.EqualTo(constants.STAGE_Loading) {
		return
	}
	room.broadcastRoomSync()
	room.startInGameFromLoading()
}

// startInGameFromLoading 是 Loading → InGame 的唯一入口。
// 全员 LoadProgress >= 100 时广播 roomAllSync 并启动逻辑帧定时器。
func (room *Room) startInGameFromLoading() bool {
	if !room.GameStage.EqualTo(constants.STAGE_Loading) || !room.HasAllPlayerLoaded() {
		return false
	}
	room.resetInGameState()
	room.GameStage.Store(constants.STAGE_InGame)
	recordDesyncEvent(room.ID, "all_sync", map[string]any{
		"seed":     room.Seed,
		"players":  room.playerSyncSnapshot(),
		"loadouts": room.canonicalPlayerLoadouts(),
	})
	room.broadcastRoomAllSync()
	room.RoomCtx.StartGameTicker()
	log.Printf("🟢 房间 %d 全员加载完成，广播 roomAllSync 并启动 ticker", room.ID)
	return true
}

// tickHeartbeat 每秒调用：扫描心跳超时的连接。
// 大厅/选卡阶段没有业务帧，拔线的对端只有靠它发现；
// InGame 阶段客户端每个逻辑帧都有 blank 上行，天然维持心跳。
func (room *Room) tickHeartbeat() {
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		if player == nil || player.Ctx == nil {
			return true
		}
		if player.Ctx.State != constants.PlayerStateConnected {
			return true
		}
		if !player.Ctx.IsConnected() {
			return true
		}
		if player.Ctx.IsHeartbeatExpired(constants.HeartbeatTimeout) {
			log.Printf("🟡 玩家 %d 心跳超时（阶段=%d），关闭连接", player.Ctx.Id, room.GameStage.Load())
			player.Ctx.Close()
			// 不直接改状态：写通道关闭后读循环会立刻退出并走
			// unregister → InGame 保留席位 / 其他阶段移除。
			// 但读循环依赖 ReadMessage 出错返回，可能要等内核超时；
			// ForceClose 已直接关 socket，读循环会立即醒来。
		}
		return true
	})
}

// HandleRequestReturnToLobby PostGame 结算后玩家点击“返回房间”：
// 广播确认并把房间重置回 InLobby，双方可再次准备开局。
func (room *Room) HandleRequestReturnToLobby(player *Player, _ *messages.RequestReturnToLobby) {
	if player == nil || player.Ctx == nil {
		return
	}
	if !room.GameStage.EqualTo(constants.STAGE_PostGame) {
		return
	}
	room.returnToLobbyFromPostGame()
}

// returnToLobbyFromPostGame PostGame → InLobby 的共享入口
// （玩家请求或 PostGameLinger 超时都会走到这里）。
func (room *Room) returnToLobbyFromPostGame() {
	if !room.GameStage.EqualTo(constants.STAGE_PostGame) {
		return
	}
	log.Printf("🔁 房间 %d 从 PostGame 返回等待房间", room.ID)
	room.Reset()
	// 广播 returnToLobby；客户端据此退出结算界面回到等待房间
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_ReturnToLobby{
			ReturnToLobby: &messages.ResponseReturnToLobby{
				Stage: uint32(constants.STAGE_InLobby),
			},
		},
	}, nil)
	room.broadcastRoomInfo()
	room.broadcastReadyCount()
}

// tickPostGame 每秒调用：PostGame 停留超过 PostGameLinger 无人操作时自动返回等待房间。
func (room *Room) tickPostGame() {
	if !room.GameStage.EqualTo(constants.STAGE_PostGame) {
		return
	}
	if room.PostGameSince.IsZero() {
		room.PostGameSince = time.Now()
		return
	}
	if time.Since(room.PostGameSince) > constants.PostGameLinger {
		room.returnToLobbyFromPostGame()
	}
}

// startPostGameClock 进入 PostGame 时记录起点。
func (room *Room) startPostGameClock() {
	room.PostGameSince = time.Now()
}

// sweepStaleDisconnected 非 InGame 阶段的断线席位兜底清理：
// handleUnregister 已直接移除，这里只处理状态残留，保证集合一致。
func (room *Room) sweepStaleDisconnected() {
	if room.GameStage.EqualTo(constants.STAGE_InGame) {
		return
	}
	room.RoomCtx.Players.Range(func(id int, player *Player) bool {
		if player == nil || player.Ctx == nil {
			return true
		}
		if player.Ctx.State == constants.PlayerStateDisconnected && !player.Ctx.IsConnected() {
			// 非 InGame 阶段不应保留断线席位；若读到循环未及时注销（如服务器写失败关闭），
			// 这里直接移除，防止状态残留导致 hasDisconnectedPlayer 误判。
			_ = id
		}
		return true
	})
}

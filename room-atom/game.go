package roomatom

// 游戏开始后的视角
// GameLogic 结构体、ProcessFrame、EnqueueAction、游戏相关方法

import (
	"fmt"
	"log"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	maxResendPerSync uint32 = 32
	maxHistoryFrames uint32 = 600
	// 服务端最多领先“双方都已 ACK 的 committed frame”多少帧。
	// 生成帧可以适度领先以吸收网络抖动，但不能无限领先后裁掉未确认历史。
	maxSpeculativeLeadFrames uint32 = 120
)

const maxOperationUID uint32 = ^uint32(0)

func inGameOperationTypePriority(operation *messages.InGameOperation) int {
	if operation == nil || operation.Payload == nil {
		return 99
	}

	switch operation.Payload.(type) {
	case *messages.InGameOperation_RemovePlant:
		return 0
	case *messages.InGameOperation_UseStarShards:
		return 1
	case *messages.InGameOperation_CardPlant:
		return 2
	case *messages.InGameOperation_GameEvent:
		return 3
	case *messages.InGameOperation_Error:
		return 4
	default:
		return 99
	}
}

func inGameOperationUID(operation *messages.InGameOperation) uint32 {
	if operation == nil || operation.Payload == nil {
		return maxOperationUID
	}

	switch payload := operation.Payload.(type) {
	case *messages.InGameOperation_RemovePlant:
		if payload.RemovePlant != nil && payload.RemovePlant.Base != nil {
			return payload.RemovePlant.Base.Uid
		}
	case *messages.InGameOperation_UseStarShards:
		if payload.UseStarShards != nil && payload.UseStarShards.Base != nil {
			return payload.UseStarShards.Base.Uid
		}
	case *messages.InGameOperation_CardPlant:
		if payload.CardPlant != nil && payload.CardPlant.Base != nil {
			return payload.CardPlant.Base.Uid
		}
	case *messages.InGameOperation_Error:
		if payload.Error != nil {
			return payload.Error.Uid
		}
	}

	return maxOperationUID
}

func (room *Room) currentServerFrameID() uint32 {
	nextFrameID := room.RoomCtx.NextFrameID.Load()
	if nextFrameID == 0 {
		return 0
	}
	return nextFrameID - 1
}

// committedFrameID 是所有仍在房间内玩家都已 ACK 的最大连续帧。
// 只有 committed frame 才能作为跨端摘要、checkpoint、结束仲裁的权威依据；
// 服务端已生成但尚未被双方确认的帧只是 speculative frame。
func (room *Room) committedFrameID() uint32 {
	if room == nil || room.RoomCtx == nil {
		return 0
	}
	current := room.currentServerFrameID()
	if current == 0 {
		return current
	}

	playerCount := 0
	minAck := current
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		if player == nil || player.Ctx == nil {
			return true
		}
		// 断线玩家不会继续 ACK；权威帧只要求仍在线双方确认。
		// 断线席位由重连宽限期处理，重连后再从 checkpoint 补齐。
		if player.Ctx.State == constants.PlayerStateDisconnected {
			return true
		}
		playerCount++
		ack := player.Ctx.LatestAckFrameID.Load()
		if ack < minAck {
			minAck = ack
		}
		return true
	})
	// 测试/空房间没有 ACK 来源时，退回当前生成帧，避免辅助函数不可用。
	if playerCount == 0 {
		return current
	}
	if minAck > current {
		return current
	}
	return minAck
}

func (room *Room) resetInGameState() {
	finishDesyncLog(room.ID, "log_complete", map[string]any{
		"reason": "new_game",
	})
	startDesyncLog(room.ID)
	recordDesyncEvent(room.ID, "game_start", map[string]any{
		"seed":      room.Seed,
		"chapterId": room.ChapterID,
		"stageId":   room.StageID,
		"players":   room.playerSyncSnapshot(),
		"loadouts":  room.canonicalPlayerLoadouts(),
	})
	room.RoomCtx.NextFrameID.Store(1)
	room.scheduledFrames = make(map[uint32]*FrameSubmission)
	room.frameHistory = make(map[uint32]*messages.InGameResponse)
	room.Logic.Reset()
	room.EndGameVotes = make(map[int]uint32)
	room.EndGameFirstFrame = 0
	room.EndGameDeadline = time.Time{}
	room.pendingEndFrame = 0
	room.EndGameGraceActive = false
	room.EndGameFrameID = 0
	room.EndGameResult = 0
	room.FrameHashes = make(map[uint32]map[int]uint32)
	room.DesyncDetected = false
	room.HashSchemaMismatch = false
	room.ResyncNotified = make(map[int]struct{})
	room.Checkpoints = make(map[uint32]*stateCheckpoint)
	room.LastCheckpointFrame = 0
	for len(room.ingameOperations) > 0 {
		<-room.ingameOperations
	}
}

func (room *Room) ensureFrameSubmission(frameID uint32) *FrameSubmission {
	submission, ok := room.scheduledFrames[frameID]
	if ok {
		return submission
	}

	submission = &FrameSubmission{
		Operations:          make([]*messages.InGameOperation, 0, 2),
		OperationSignatures: make(map[string]struct{}),
	}
	room.scheduledFrames[frameID] = submission
	return submission
}

func (room *Room) recordOperationSubmission(frameID uint32, operation *messages.InGameOperation) {
	if operation == nil {
		return
	}
	// 帧一旦生成就不可再写入：权威帧内容必须对所有客户端一致，
	// 否则同一操作会在不同端落到不同逻辑帧，回滚也无法修复。
	if frameID <= room.currentServerFrameID() {
		log.Printf("⚠️ 拒绝写入已生成帧 room=%d frame=%d current=%d",
			room.ID, frameID, room.currentServerFrameID())
		recordDesyncEvent(room.ID, "late_operation_rejected", map[string]any{
			"frameId":   frameID,
			"current":   room.currentServerFrameID(),
			"operation": describeInGameOperations([]*messages.InGameOperation{operation}),
		})
		return
	}

	submission := room.ensureFrameSubmission(frameID)
	signature := room.operationSignature(operation)
	if _, exists := submission.OperationSignatures[signature]; exists {
		return
	}

	submission.OperationSignatures[signature] = struct{}{}
	submission.Operations = append(submission.Operations, operation)
}

func (room *Room) operationSignature(operation *messages.InGameOperation) string {
	if operation == nil || operation.Payload == nil {
		return ""
	}

	switch payload := operation.Payload.(type) {
	case *messages.InGameOperation_CardPlant:
		return strings.Join([]string{
			strconv.FormatUint(uint64(operation.ProcessFrameId), 10),
			"cardPlant",
			strconv.FormatUint(uint64(payload.CardPlant.Pid), 10),
			strconv.FormatUint(uint64(payload.CardPlant.Level), 10),
			strconv.FormatUint(uint64(payload.CardPlant.Base.Uid), 10),
			strconv.FormatUint(uint64(payload.CardPlant.Base.Col), 10),
			strconv.FormatUint(uint64(payload.CardPlant.Base.Row), 10),
		}, ":")
	case *messages.InGameOperation_RemovePlant:
		return strings.Join([]string{
			strconv.FormatUint(uint64(operation.ProcessFrameId), 10),
			"removePlant",
			strconv.FormatUint(uint64(payload.RemovePlant.Pid), 10),
			strconv.FormatUint(uint64(payload.RemovePlant.Base.Uid), 10),
			strconv.FormatUint(uint64(payload.RemovePlant.Base.Col), 10),
			strconv.FormatUint(uint64(payload.RemovePlant.Base.Row), 10),
		}, ":")
	case *messages.InGameOperation_UseStarShards:
		return strings.Join([]string{
			strconv.FormatUint(uint64(operation.ProcessFrameId), 10),
			"useStarShards",
			strconv.FormatUint(uint64(payload.UseStarShards.Pid), 10),
			strconv.FormatUint(uint64(payload.UseStarShards.Base.Uid), 10),
			strconv.FormatUint(uint64(payload.UseStarShards.Base.Col), 10),
			strconv.FormatUint(uint64(payload.UseStarShards.Base.Row), 10),
		}, ":")
	case *messages.InGameOperation_Error:
		data := payload.Error
		if data == nil {
			data = &messages.ResponseErrorMessage{}
		}
		return strings.Join([]string{
			strconv.FormatUint(uint64(operation.ProcessFrameId), 10),
			"error",
			strconv.FormatUint(uint64(data.Uid), 10),
			strconv.FormatUint(uint64(data.ClientOpSeq), 10),
			strconv.FormatUint(uint64(data.Col), 10),
			strconv.FormatUint(uint64(data.Row), 10),
		}, ":")
	default:
		return strings.Join([]string{
			strconv.FormatUint(uint64(operation.ProcessFrameId), 10),
			strconv.FormatUint(uint64(operation.OperationIndex), 10),
		}, ":")
	}
}

func (room *Room) lessInGameOperation(a *messages.InGameOperation, b *messages.InGameOperation) bool {
	if a.ProcessFrameId != b.ProcessFrameId {
		return a.ProcessFrameId < b.ProcessFrameId
	}

	aTypePriority := inGameOperationTypePriority(a)
	bTypePriority := inGameOperationTypePriority(b)
	if aTypePriority != bTypePriority {
		return aTypePriority < bTypePriority
	}

	aUID := inGameOperationUID(a)
	bUID := inGameOperationUID(b)
	if aUID != bUID {
		return aUID < bUID
	}

	return room.operationSignature(a) < room.operationSignature(b)
}

func (room *Room) advanceOneFrame() {
	frameID := room.currentServerFrameID() + 1
	submission := room.scheduledFrames[frameID]
	operations := make([]*messages.InGameOperation, 0)
	if submission != nil {
		// 写进历史帧的必须是副本：调用方仍可能持有同一 proto 指针，
		// 任何后续改动都不得回溯改写已经广播过的权威帧。
		for _, operation := range submission.Operations {
			cloned, ok := proto.Clone(operation).(*messages.InGameOperation)
			if !ok || cloned == nil {
				continue
			}
			operations = append(operations, cloned)
		}
		sort.SliceStable(operations, func(i, j int) bool {
			return room.lessInGameOperation(operations[i], operations[j])
		})
	}

	for index, operation := range operations {
		operation.OperationIndex = uint32(index + 1)
		operation.ProcessFrameId = frameID
	}

	room.frameHistory[frameID] = &messages.InGameResponse{
		FrameId:    frameID,
		Operations: operations,
	}
	if len(operations) > 0 || shouldLogFrameDetail(frameID) {
		recordDesyncEvent(room.ID, "authoritative_frame", map[string]any{
			"frameId":          frameID,
			"committedFrameId": room.committedFrameID(),
			"operations":       describeInGameOperations(operations),
			"players":          room.playerSyncSnapshot(),
		})
	}
	delete(room.scheduledFrames, frameID)
	room.Logic.ReleaseFrame(frameID)
	room.RoomCtx.NextFrameID.Store(frameID + 1)
}

func (room *Room) earliestHistoryFrameID() uint32 {
	if len(room.frameHistory) == 0 {
		return room.RoomCtx.NextFrameID.Load()
	}

	minFrameID := ^uint32(0)
	for frameID := range room.frameHistory {
		if frameID < minFrameID {
			minFrameID = frameID
		}
	}
	return minFrameID
}

// syncClientFrames 推送权威帧。普通 tick 只推该玩家尚未推送过的新帧；
// forceResend=true（客户端请求触发）时从 ACK+1 重新补发，避免极短逻辑帧下
// 每 tick 重复补发同一批帧造成消息风暴、客户端 ACK 反而停滞。
func (room *Room) syncClientFrames(player *Player, forceResend bool) {
	if player == nil || player.Ctx == nil || !player.Ctx.IsConnected() {
		return
	}

	serverFrameID := room.currentServerFrameID()
	if serverFrameID == 0 || len(room.frameHistory) == 0 {
		return
	}

	ackFrameID := player.Ctx.LatestAckFrameID.Load()
	if forceResend {
		player.LastSyncedFrameID.Store(ackFrameID)
	}
	fromFrameID := ackFrameID + 1
	if fromFrameID <= player.LastSyncedFrameID.Load() {
		fromFrameID = player.LastSyncedFrameID.Load() + 1
	}
	earliestFrameID := room.earliestHistoryFrameID()
	if fromFrameID < earliestFrameID {
		// 缺口无法用增量帧补齐：明确通知客户端重新加入，避免永久停在“等待网络帧”。
		if fromFrameID+1 < earliestFrameID {
			room.notifyResyncGap(player, fromFrameID, earliestFrameID)
			return
		}
		fromFrameID = earliestFrameID
	}
	if fromFrameID > serverFrameID {
		return
	}

	toFrameID := serverFrameID
	if delta := toFrameID - fromFrameID; delta >= maxResendPerSync {
		toFrameID = fromFrameID + maxResendPerSync - 1
	}

	for frameID := fromFrameID; frameID <= toFrameID; frameID++ {
		response, ok := room.frameHistory[frameID]
		if !ok {
			continue
		}
		if len(response.Operations) > 0 || shouldLogFrameDetail(frameID) {
			recordDesyncEvent(room.ID, "send_frame", map[string]any{
				"playerId":       player.Ctx.Id,
				"frameId":        frameID,
				"operationCount": len(response.Operations),
				"ackFrameId":     ackFrameID,
			})
		}
		room.RoomCtx.SendMessageToUserByPlayer(response, player)
	}
	if toFrameID > player.LastSyncedFrameID.Load() {
		player.LastSyncedFrameID.Store(toFrameID)
	}
}

// notifyResyncGap 通知落后过深的客户端主动重连：
// 缺失的历史帧已被裁剪，继续等待只会永久卡在“等待网络帧”。
func (room *Room) notifyResyncGap(player *Player, missingFrom uint32, earliest uint32) {
	if player == nil || player.Ctx == nil {
		return
	}
	if room.ResyncNotified == nil {
		room.ResyncNotified = make(map[int]struct{})
	}
	if _, notified := room.ResyncNotified[player.Ctx.Id]; notified {
		return
	}
	room.ResyncNotified[player.Ctx.Id] = struct{}{}
	log.Printf("⚠️ 玩家 %d 帧缺口过大（需要 %d，最早 %d），通知重连", player.Ctx.Id, missingFrom, earliest)
	resp := &messages.RoomResponse{
		Payload: &messages.RoomResponse_RoomClosed{
			RoomClosed: &messages.ResponseRoomClosed{
				Message: "本地进度落后过多，缺失帧已超出历史窗口，请重新加入房间",
			},
		},
	}
	room.RoomCtx.SendMessageToUserByPlayer(resp, player)
}

func (room *Room) syncAllClients(forceResend bool) {
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		room.syncClientFrames(player, forceResend)
		return true
	})
}

func (room *Room) pruneFrameHistory() {
	if len(room.frameHistory) == 0 {
		return
	}

	// 只删除双方都已经 ACK 的帧。未 committed 的帧即使超过时间窗口也必须保留，
	// 否则落后的客户端会被迫跳过缺失帧，随后所有操作都在错误帧落地。
	committed := room.committedFrameID()
	minAckFrameID := committed
	// 已确认 checkpoint 之后的历史必须保留：重连时客户端会先恢复到 checkpoint，
	// 再重放 checkpoint+1..current。若把这些帧当普通已 ACK 帧裁掉，重连必丢操作。
	if room.LastCheckpointFrame == 0 {
		// 第一个 checkpoint 之前没有可恢复快照，必须保留全部历史供重连。
		minAckFrameID = 0
	} else if room.LastCheckpointFrame < minAckFrameID {
		minAckFrameID = room.LastCheckpointFrame
	}
	for frameID := range room.frameHistory {
		if frameID <= minAckFrameID {
			delete(room.frameHistory, frameID)
			room.Logic.ReleaseFrame(frameID)
		}
	}

	if room.currentServerFrameID() > committed+maxHistoryFrames {
		log.Printf("⚠️ 房间 %d 未确认帧积压 %d 帧（committed=%d current=%d），等待客户端 ACK",
			room.ID, room.currentServerFrameID()-committed, committed, room.currentServerFrameID())
	}
}

// resolveProcessFrameID 把操作限制在 [current+CommandLeadFrames, current+MaxDelayFrames] 窗口内。
// 第二个返回值为 false 表示请求试图调度过远的未来帧，应被拒绝。
func (room *Room) resolveProcessFrameID(baseReq *messages.RequestGridOperation) (uint32, bool) {
	current := room.currentServerFrameID()
	minFrameID := current + constants.CommandLeadFrames
	maxFrameID := current + constants.MaxDelayFrames

	frameID := minFrameID
	if baseReq != nil && baseReq.ProcessFrameId > frameID {
		frameID = baseReq.ProcessFrameId
	}
	if frameID > maxFrameID {
		return maxFrameID, false
	}
	return frameID, true
}

// rejectOperation 把失败原因作为确定性错误操作排入目标帧，
// 客户端收到后可撤销乐观预测并提示具体原因。
func (room *Room) rejectOperation(player *Player, baseReq *messages.RequestGridOperation, frameID uint32, reason string) {
	if player == nil || baseReq == nil {
		return
	}
	errorOp := &messages.InGameOperation{
		ProcessFrameId: frameID,
		Payload: &messages.InGameOperation_Error{
			Error: &messages.ResponseErrorMessage{
				Message:        reason,
				Uid:            uint32(player.GetID()),
				ClientOpSeq:    baseReq.ClientOpSeq,
				Col:            baseReq.Col,
				Row:            baseReq.Row,
				ProcessFrameId: frameID,
			},
		},
	}
	room.recordOperationSubmission(frameID, errorOp)
}

func (room *Room) acceptInGameBase(player *Player, req *messages.RequestBlank) bool {
	if player == nil || player.Ctx == nil || req == nil {
		return false
	}
	if !room.GameStage.EqualTo(constants.STAGE_InGame) {
		return false
	}

	// ACK 只能指向服务端已经生成的帧。否则一个异常/伪造的 ACK 会把
	// committedFrameID 推到未来，让尚未真正被该客户端执行的帧被当成权威帧。
	serverFrame := room.currentServerFrameID()
	frameID := req.FrameId
	ackFrameID := req.AckFrameId
	clamped := false
	if frameID > serverFrame {
		frameID = serverFrame
		clamped = true
	}
	// ACK 不能超过客户端自报的进度帧：前者表示“已执行”，后者表示“已收到”。
	// 否则一个异常客户端可以只上报 frame=10、ack=99，把 committed 直接推到未来。
	if ackFrameID > frameID {
		ackFrameID = frameID
		clamped = true
	}
	if clamped {
		recordDesyncEvent(room.ID, "frame_progress_clamped", map[string]any{
			"playerId":          player.Ctx.Id,
			"requestedFrameId":  req.FrameId,
			"requestedAckFrame": req.AckFrameId,
			"acceptedFrameId":   frameID,
			"acceptedAckFrame":  ackFrameID,
			"serverFrame":       serverFrame,
		})
	}
	player.Ctx.UpdatePlayerFrame(frameID, ackFrameID)
	if req.ClientLatencyMs > 0 && req.ClientLatencyMs < 60_000 {
		player.Ctx.ReportedLatencyMs.Store(req.ClientLatencyMs)
	}
	// 每帧心跳带来的资源 profile 覆盖保存；重连时随 AllReady 回传。
	room.recordPlayerResources(player.Ctx.Id, req)
	player.Ctx.StateHashVersion.Store(req.StateHashVersion)
	player.Ctx.StateHashVersionReported.Store(true)
	if shouldLogFrameDetail(req.FrameId) || req.HashFrameId != 0 {
		recordDesyncEvent(room.ID, "blank", map[string]any{
			"playerId":         player.Ctx.Id,
			"frameId":          req.FrameId,
			"ackFrameId":       req.AckFrameId,
			"hashFrameId":      req.HashFrameId,
			"stateHash":        fmt.Sprintf("%08x", req.StateHash),
			"stateHashHex":     req.StateHashHex,
			"stateHashVersion": req.StateHashVersion,
		})
	}
	room.checkHashSchemaVersions()
	if req.HashFrameId != 0 && req.StateHash != 0 {
		room.recordStateHash(player, req.HashFrameId, req.StateHash)
	}
	room.recordCheckpoint(player, req)
	return true
}

// checkHashSchemaVersions 拒绝新旧客户端混用：
// 旧客户端把本机卡槽等字段算进 hash，新客户端已把这些字段移出，
// 两端从第 1 帧起 hash 就不同。必须先报版本不一致，而不是报状态分叉。
func (room *Room) checkHashSchemaVersions() {
	if room == nil || room.HashSchemaMismatch {
		return
	}
	type versionEntry struct {
		PlayerID         int    `json:"playerId"`
		StateHashVersion uint32 `json:"stateHashVersion"`
	}
	entries := make([]versionEntry, 0, 2)
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		// 只统计真正发送过心跳的客户端。重连后的新上下文可能已经带有
		// checkpoint 帧号，但尚未上报版本；不能把默认值 0 当成旧版本。
		if player == nil || player.Ctx == nil || !player.Ctx.StateHashVersionReported.Load() {
			return true
		}
		entries = append(entries, versionEntry{
			PlayerID:         player.Ctx.Id,
			StateHashVersion: player.Ctx.StateHashVersion.Load(),
		})
		return true
	})
	if len(entries) < 2 {
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].PlayerID < entries[j].PlayerID })
	firstVersion := entries[0].StateHashVersion
	mismatch := false
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		parts = append(parts, fmt.Sprintf("%d:%d", entry.PlayerID, entry.StateHashVersion))
		if entry.StateHashVersion != firstVersion {
			mismatch = true
		}
	}
	if !mismatch {
		return
	}

	room.HashSchemaMismatch = true
	versionText := strings.Join(parts, ",")
	log.Printf("🚨 客户端哈希口径版本不一致 versions=%s，停止状态哈希比较", versionText)
	recordDesyncEvent(room.ID, "client_version_mismatch", map[string]any{
		"versions": entries,
		"players":  room.playerSyncSnapshot(),
	})
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_Error{
			Error: &messages.ResponseErrorMessage{
				Message: "检测到客户端版本不一致（" + versionText +
					"），请刷新或更新所有客户端后再开始游戏",
			},
		},
	}, nil)
}

// recordStateHash 汇总同一帧各客户端的状态摘要，不一致即判定分叉并广播止损。
func (room *Room) recordStateHash(player *Player, frameID uint32, hash uint32) {
	if room == nil || player == nil || player.Ctx == nil || frameID == 0 || hash == 0 {
		return
	}
	if frameID > room.currentServerFrameID() {
		// 客户端还不能对尚未生成的帧给出权威摘要。
		recordDesyncEvent(room.ID, "state_hash_future_frame_rejected", map[string]any{
			"playerId":    player.Ctx.Id,
			"frameId":     frameID,
			"serverFrame": room.currentServerFrameID(),
		})
		return
	}
	// 新旧客户端混用时 hash 口径不同，不能据此判定游戏状态分叉。
	if room.HashSchemaMismatch {
		return
	}
	if room.FrameHashes == nil {
		room.FrameHashes = make(map[uint32]map[int]uint32)
	}
	hashes := room.FrameHashes[frameID]
	if hashes == nil {
		hashes = make(map[int]uint32)
		room.FrameHashes[frameID] = hashes
	}
	hashes[player.Ctx.Id] = hash
	recordDesyncEvent(room.ID, "state_hash", map[string]any{
		"playerId":         player.Ctx.Id,
		"frameId":          frameID,
		"hash":             fmt.Sprintf("%08x", hash),
		"committedFrameId": room.committedFrameID(),
		"players":          room.playerSyncSnapshot(),
	})

	// 只有双方都已 ACK 的帧才是权威帧。未 committed 的摘要先缓存，
	// 等 committed 推进到该帧后再比较，避免“一端先到 frame N”就提前判分叉。
	committed := room.committedFrameID()
	if committed > 0 && frameID <= committed {
		room.evaluateStateHash(frameID)
	}

	// 只保留最近 64 帧的摘要，避免长期增长
	if len(room.FrameHashes) > 64 {
		for id := range room.FrameHashes {
			if id+32 < frameID {
				delete(room.FrameHashes, id)
			}
		}
	}
}

// evaluateCommittedStateHashes 在 committed 推进后，比较所有已经双端确认的摘要。
func (room *Room) evaluateCommittedStateHashes(committed uint32) {
	if room == nil || len(room.FrameHashes) == 0 {
		return
	}
	for frameID := range room.FrameHashes {
		if frameID <= committed {
			room.evaluateStateHash(frameID)
		}
	}
	// 已确认帧的摘要比较完就删；单端缺失的旧摘要也要清理，避免永久占用。
	for frameID := range room.FrameHashes {
		if committed > 64 && frameID+64 < committed {
			delete(room.FrameHashes, frameID)
		}
	}
}

func (room *Room) evaluateStateHash(frameID uint32) {
	if room == nil || room.DesyncDetected {
		return
	}
	hashes := room.FrameHashes[frameID]
	if len(hashes) < 2 {
		return
	}

	var first uint32
	hasFirst := false
	mismatch := false
	for _, value := range hashes {
		if !hasFirst {
			first = value
			hasFirst = true
			continue
		}
		if value != first {
			mismatch = true
		}
	}

	// 无论一致与否，该帧已经完成双端比较，不再重复告警。
	delete(room.FrameHashes, frameID)
	if !mismatch || room.DesyncDetected {
		return
	}

	room.DesyncDetected = true
	hashText := formatFrameHashes(hashes)
	log.Printf("🚨 检测到状态分叉 frame=%d hashes=%s committed=%d", frameID, hashText, room.committedFrameID())
	hashesCopy := make(map[int]uint32, len(hashes))
	for playerID, hash := range hashes {
		hashesCopy[playerID] = hash
	}
	frameHashesCopy := make(map[uint32]map[int]uint32, len(room.FrameHashes))
	for id, frameHashes := range room.FrameHashes {
		hashes := make(map[int]uint32, len(frameHashes))
		for playerID, hash := range frameHashes {
			hashes[playerID] = hash
		}
		frameHashesCopy[id] = hashes
	}
	snapshotFields := map[string]any{
		"frameId":          frameID,
		"hashes":           hashesCopy,
		"hashText":         hashText,
		"committedFrameId": room.committedFrameID(),
		"players":          room.playerSyncSnapshot(),
		"frameHashes":      frameHashesCopy,
		"recentFrames":     room.describeRecentFrames(frameID, 30),
	}
	snapshotPath := writeDesyncSnapshot(room.ID, snapshotFields)
	snapshotFields["snapshotPath"] = snapshotPath
	recordDesyncEvent(room.ID, "desync_detected", snapshotFields)
	flushDesyncLog(room.ID)
	resp := &messages.RoomResponse{
		Payload: &messages.RoomResponse_Error{
			Error: &messages.ResponseErrorMessage{
				Message: "检测到客户端状态分叉（frame=" + strconv.FormatUint(uint64(frameID), 10) +
					"，hashes=" + hashText + "），本局已标记为异常",
			},
		},
	}
	room.RoomCtx.BroadcastMessage(resp, nil)
}

const endGameVoteTimeout = 3 * time.Second

// RegisterEndGameVote 记录玩家上报的胜负结果；不立刻结束房间，等待另一端确认。
func (room *Room) RegisterEndGameVote(player *Player, result uint32) {
	if player == nil || player.Ctx == nil {
		return
	}
	if room.EndGameVotes == nil {
		room.EndGameVotes = make(map[int]uint32)
	}
	if _, exists := room.EndGameVotes[player.Ctx.Id]; exists {
		// 重复上报视为幂等，忽略
		return
	}
	room.EndGameVotes[player.Ctx.Id] = result
	if room.EndGameFirstFrame == 0 {
		room.EndGameFirstFrame = player.Ctx.LatestFrameID.Load()
	}
	if len(room.EndGameVotes) == 1 {
		room.EndGameDeadline = time.Now().Add(endGameVoteTimeout)
	}
	if room.allPlayersVoted() {
		room.scheduleAuthoritativeEnd()
	}
}

func (room *Room) allPlayersVoted() bool {
	total := 0
	allVoted := true
	room.RoomCtx.Players.Range(func(_ int, value *Player) bool {
		if value == nil || value.Ctx == nil {
			return true
		}
		total++
		if _, exists := room.EndGameVotes[value.Ctx.Id]; !exists {
			allVoted = false
		}
		return true
	})
	return total > 0 && allVoted
}

func (room *Room) decideEndGameResult() (uint32, bool) {
	var first uint32
	hasFirst := false
	agree := true
	for _, result := range room.EndGameVotes {
		if !hasFirst {
			first = result
			hasFirst = true
			continue
		}
		if result != first {
			agree = false
		}
	}
	if !agree {
		return 0, false
	}
	if first == 1 {
		return 1, true
	}
	return 0, true
}

// scheduleAuthoritativeEnd 把结束事件写入统一的未来逻辑帧，保证所有端在同一帧结束。
func (room *Room) scheduleAuthoritativeEnd() {
	if room.pendingEndFrame != 0 {
		return
	}
	result, consistent := room.decideEndGameResult()
	if !consistent {
		log.Printf("⚠️ end_game 结果冲突，按异常对局判负")
		result = 0
	}
	endFrame := room.currentServerFrameID() + constants.CommandLeadFrames
	if room.EndGameFirstFrame+1 > endFrame {
		endFrame = room.EndGameFirstFrame + 1
	}
	event := &messages.InGameOperation{
		ProcessFrameId: endFrame,
		Payload: &messages.InGameOperation_GameEvent{
			GameEvent: &messages.ResponseGameEvent{
				EventType: 0x4000,
				Message:   strconv.FormatUint(uint64(result), 10),
			},
		},
	}
	room.recordOperationSubmission(endFrame, event)
	room.pendingEndFrame = endFrame
	room.EndGameFrameID = endFrame
	// 结果在此定稿：权威帧事件和最终结算广播必须使用同一个值。
	room.EndGameResult = result
	log.Printf("🏁 权威结束事件排入帧 %d，结果=%d，房间=%d", endFrame, result, room.ID)
}

// tickEndGameArbitration 在每次 tick 中推进投票超时与结束帧落地。
func (room *Room) tickEndGameArbitration() {
	if room.pendingEndFrame != 0 {
		// 结束帧也必须双方 ACK 后才算权威落地；生成到该帧不代表客户端已经执行。
		if room.committedFrameID() >= room.pendingEndFrame {
			room.enterPostGameAfterEndFrame()
		}
		return
	}
	if len(room.EndGameVotes) > 0 && !room.EndGameDeadline.IsZero() && time.Now().After(room.EndGameDeadline) {
		log.Printf("⌛ end_game 投票超时，按异常对局判负")
		room.scheduleAuthoritativeEnd()
	}
}

func (room *Room) enterPostGameAfterEndFrame() {
	if !room.GameStage.EqualTo(constants.STAGE_InGame) {
		return
	}
	room.GameStage.Store(constants.STAGE_PostGame)
	finishDesyncLog(room.ID, "game_end", map[string]any{
		"endFrameId": room.EndGameFrameID,
		"result":     room.EndGameResult,
	})
	if room.RoomCtx.GameTicker != nil {
		room.RoomCtx.GameTicker.Stop()
		room.RoomCtx.GameTicker = nil
	}
	// 不立即 resetInGameState：保留历史帧，供落后客户端补齐到结束帧后再自行结束。
	room.EndGameGraceActive = true
	// 使用权威帧定稿时缓存的结果，而不是重新计算：掉线宽限可能已把 EndGameVotes
	// 覆写为判负，重算会让结算包与权威结束帧里的结果矛盾。
	result := room.EndGameResult
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_GameEnd{
			GameEnd: &messages.ResponseGameEnd{
				GameResult: result,
				EndFrameId: room.EndGameFrameID,
			},
		},
	}, nil)
	log.Printf("🟢 房间 %d 进入 PostGame，结束帧=%d，保留历史帧供补发", room.ID, room.EndGameFrameID)
	// 记录 PostGame 起始时间：超过 PostGameLinger 无人“返回房间”时自动回 InLobby
	room.startPostGameClock()
}

// 定时器触发的广播转发游戏操作帧
func (room *Room) runGameTick() {
	if !room.GameStage.EqualTo(constants.STAGE_InGame) {
		return
	}

	room.LastActiveTime = time.Now() // 更新最后活动时间
	committed := room.committedFrameID()
	current := room.currentServerFrameID()
	if current > 0 && current >= committed+maxSpeculativeLeadFrames {
		// 客户端已经落后一个安全窗口：停止生成新帧，只补发与等待 ACK。
		// 这样未确认的历史帧不会被裁剪，客户端也不会跳过缺失帧。
		room.syncAllClients(false)
		room.evaluateCommittedStateHashes(committed)
		room.tryAcceptCommittedCheckpoints(committed)
		room.tickEndGameArbitration()
		room.tickReconnectGrace()
		return
	}

	room.advanceOneFrame()
	room.pruneFrameHistory()
	room.syncAllClients(false)
	committed = room.committedFrameID()
	room.evaluateCommittedStateHashes(committed)
	room.tryAcceptCommittedCheckpoints(committed)
	room.tickEndGameArbitration()
	// 断线宽限期检查：超时判负由权威结束帧统一落地
	room.tickReconnectGrace()
}

// CouldAffordable 资源校验：客户端自报的总量必须能覆盖本次消耗。
// 服务端不模拟资源产出（帧同步架构下权威数值在客户端模型中），
// 这里只拦截明显的非法值；进一步的增量校验见资源防作弊 backlog。
func (room *Room) CouldAffordable(player *Player,
	energy_cost int32, starshards_cost int32,
	energy_sum int32, starshards_sum int32,
) bool {
	if player == nil {
		return false
	}
	if energy_cost > 0 && energy_sum < energy_cost {
		return false
	}
	if starshards_cost > 0 && starshards_sum < starshards_cost {
		return false
	}
	return true
}

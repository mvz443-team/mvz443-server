package roomatom

// 游戏开始后的视角
// GameLogic 结构体、ProcessFrame、EnqueueAction、游戏相关方法

import (
	"log"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	maxResendPerSync uint32 = 32
	maxHistoryFrames uint32 = 600
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

func (room *Room) resetInGameState() {
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
		operations = append(operations, submission.Operations...)
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

func (room *Room) syncClientFrames(player *Player) {
	if player == nil || player.Ctx == nil || !player.Ctx.IsConnected() {
		return
	}

	serverFrameID := room.currentServerFrameID()
	if serverFrameID == 0 || len(room.frameHistory) == 0 {
		return
	}

	fromFrameID := player.Ctx.LatestAckFrameID.Load() + 1
	earliestFrameID := room.earliestHistoryFrameID()
	if fromFrameID < earliestFrameID {
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
		room.RoomCtx.SendMessageToUserByPlayer(response, player)
	}
}

func (room *Room) syncAllClients() {
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		room.syncClientFrames(player)
		return true
	})
}

func (room *Room) pruneFrameHistory() {
	if len(room.frameHistory) == 0 {
		return
	}

	serverFrameID := room.currentServerFrameID()
	minAckFrameID := serverFrameID
	if room.GetPlayerCount() > 0 {
		minAckFrameID = ^uint32(0)
		room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
			if player == nil || player.Ctx == nil {
				return true
			}
			ackFrameID := player.Ctx.LatestAckFrameID.Load()
			if ackFrameID < minAckFrameID {
				minAckFrameID = ackFrameID
			}
			return true
		})
	}

	for frameID := range room.frameHistory {
		tooOldForAck := frameID <= minAckFrameID
		tooOldForWindow := serverFrameID > maxHistoryFrames && frameID <= serverFrameID-maxHistoryFrames
		if tooOldForAck || tooOldForWindow {
			delete(room.frameHistory, frameID)
			room.Logic.ReleaseFrame(frameID)
		}
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

	player.Ctx.UpdatePlayerFrame(req.FrameId, req.AckFrameId)
	return true
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
	log.Printf("🏁 权威结束事件排入帧 %d，结果=%d", endFrame, result)
}

// tickEndGameArbitration 在每次 tick 中推进投票超时与结束帧落地。
func (room *Room) tickEndGameArbitration() {
	if room.pendingEndFrame != 0 {
		if room.currentServerFrameID() >= room.pendingEndFrame {
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
	if room.RoomCtx.GameTicker != nil {
		room.RoomCtx.GameTicker.Stop()
		room.RoomCtx.GameTicker = nil
	}
	// 不立即 resetInGameState：保留历史帧，供落后客户端补齐到结束帧后再自行结束。
	room.EndGameGraceActive = true
	log.Printf("🟢 房间进入 PostGame，结束帧=%d，保留历史帧供补发", room.EndGameFrameID)
}

// 定时器触发的广播转发游戏操作帧
func (room *Room) runGameTick() {
	if !room.GameStage.EqualTo(constants.STAGE_InGame) {
		return
	}

	room.LastActiveTime = time.Now() // 更新最后活动时间
	room.advanceOneFrame()
	room.pruneFrameHistory()
	room.syncAllClients()
	room.tickEndGameArbitration()
}

func (room *Room) HasAllPlayerSync() bool {
	// 延迟等待，最多容忍 maxDelayFrames 帧的延迟
	var minFrameID uint32
	var NextRenderFrame = room.RoomCtx.NextFrameID.Load()
	if NextRenderFrame < constants.MaxDelayFrames {
		minFrameID = 0
	} else {
		minFrameID = NextRenderFrame - constants.MaxDelayFrames
	}

	synced := true
	// 遍历每个玩家的 frameID，若有任意玩家低于阈值，则返回 false
	room.RoomCtx.Players.Range(func(key int, value *Player) bool {
		// 检查玩家是否为空或玩家上下文为空
		if value == nil || value.Ctx == nil {
			synced = false
			return false
		}
		// 获取当前玩家实际的帧号
		player_current_frame := value.Ctx.LatestFrameID.Load()
		if player_current_frame < minFrameID {
			synced = false
			return false
		}
		return true
	})
	return synced
}

// 用户可能会疯狂的消耗游戏资源从而种植/使用超过自己能量的植物
// 所以需要在处理请求时进行校验
func (room *Room) CouldAffordable(player *Player, base_req *messages.RequestBlank,
	energy_cost int32, starshards_cost int32,
	energy_sum int32, starshards_sum int32,
	lastFrameID uint32, lastAckFrameID uint32,
) bool {
	// 检查 player 是否为 nil
	if player == nil {
		return false
	}

	// 简化逻辑：对于第一次种植或使用星之碎片，只通过用户报的总量和消耗来判断
	isFirstEnergyOperation := player.LastEnergySum == 0 || lastFrameID <= 0
	isFirstStarShardsOperation := player.LastStarShards == 0 || lastFrameID <= 0

	// 验证能量消耗
	energyAffordable := true
	if energy_cost > 0 {
		if isFirstEnergyOperation {
			// 第一次种植：只检查总量是否足够
			energyAffordable = energy_sum >= energy_cost
		} else {
			// 非第一次：检查客户端自报的阳光是否足够
			energyAffordable = energy_sum >= energy_cost
		}
	}

	// 验证星之碎片消耗
	starShardsAffordable := true
	if starshards_cost > 0 {
		if isFirstStarShardsOperation {
			// 第一次使用星之碎片：只检查总量是否足够
			starShardsAffordable = starshards_sum >= starshards_cost
		} else {
			// 非第一次：检查客户端自报的星之碎片是否足够
			starShardsAffordable = starshards_sum >= starshards_cost
		}
	}

	return energyAffordable && starShardsAffordable
}

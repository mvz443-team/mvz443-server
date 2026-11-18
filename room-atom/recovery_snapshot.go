package roomatom

// 重连恢复快照：服务端向仍然在线的低延迟客户端索取最近权威帧快照，
// 再转发给重连客户端，并补齐快照帧到服务端权威帧之间的全部操作。
//
// 这是 checkpoint 之外的备用能力：checkpoint 需要双端摘要一致且每 200 帧
// 才产生一次；页面刷新后可能没有可用的 inline checkpoint，或者 checkpoint
// 已经明显落后。向在线客户端即时索取快照可以让重连更快恢复。

import (
	"encoding/json"
	"log"
	"mvzserver/constants"
	"mvzserver/messages/pb"
	"time"
)

const recoverySnapshotTimeout = 2 * time.Second

type pendingRecovery struct {
	targetPlayerID int
	providerID     int
	requestID      uint32
	deadline       time.Time
}

// requestReconnectSnapshot 选择最低延迟的在线玩家并请求它的最近快照。
// 没有可用来源时直接返回，调用方应已经发送 checkpoint/历史帧作为兜底。
func (room *Room) requestReconnectSnapshot(target *Player) bool {
	if room == nil || target == nil || target.Ctx == nil {
		return false
	}
	provider := room.selectSnapshotProvider(target.Ctx.Id)
	if provider == nil {
		return false
	}
	room.nextSnapshotRequestID++
	requestID := room.nextSnapshotRequestID
	room.pendingRecoveries[requestID] = &pendingRecovery{
		targetPlayerID: target.Ctx.Id,
		providerID:     provider.Ctx.Id,
		requestID:      requestID,
		deadline:       time.Now().Add(recoverySnapshotTimeout),
	}
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_SnapshotRequest{
			SnapshotRequest: &messages.ResponseSnapshotRequest{
				RequestId:      requestID,
				TargetPlayerId: uint32(target.Ctx.Id),
			},
		},
	}, provider)
	log.Printf("📸 房间 %d 向玩家 %d 请求重连快照（目标玩家 %d，request=%d）",
		room.ID, provider.Ctx.Id, target.Ctx.Id, requestID)
	return true
}

// selectSnapshotProvider 返回仍在线、延迟最低的玩家。延迟由客户端随心跳上报；
// 未上报延迟的玩家排在已上报之后，避免把快照请求发给未知连接。
func (room *Room) selectSnapshotProvider(excludeID int) *Player {
	var best *Player
	bestLatency := ^uint32(0)
	room.RoomCtx.Players.Range(func(id int, player *Player) bool {
		if player == nil || player.Ctx == nil || id == excludeID {
			return true
		}
		if player.Ctx.State != constants.PlayerStateConnected || !player.Ctx.IsConnected() {
			return true
		}
		latency := player.Ctx.ReportedLatencyMs.Load()
		score := latency
		if score == 0 {
			score = ^uint32(0) - 1
		}
		if best == nil || score < bestLatency || (score == bestLatency && player.Ctx.Id < best.Ctx.Id) {
			best = player
			bestLatency = score
		}
		return true
	})
	return best
}

// HandleRequestSnapshotResponse 处理在线客户端上传的快照，并转发给重连目标。
func (room *Room) HandleRequestSnapshotResponse(player *Player, req *messages.RequestSnapshotResponse) {
	if room == nil || player == nil || player.Ctx == nil || req == nil {
		return
	}
	pending := room.pendingRecoveries[req.RequestId]
	if pending == nil || pending.providerID != player.Ctx.Id {
		return
	}
	delete(room.pendingRecoveries, req.RequestId)

	if req.InlineSnapshotJson == "" {
		log.Printf("⚠️ 玩家 %d 无法提供重连快照 request=%d", player.Ctx.Id, req.RequestId)
		return
	}
	if len(req.InlineSnapshotJson) > maxCheckpointInlineBytes || !json.Valid([]byte(req.InlineSnapshotJson)) {
		log.Printf("⚠️ 玩家 %d 的重连快照无效 request=%d bytes=%d",
			player.Ctx.Id, req.RequestId, len(req.InlineSnapshotJson))
		return
	}
	target, ok := room.RoomCtx.Players.Load(pending.targetPlayerID)
	if !ok || target == nil || target.Ctx == nil || !target.Ctx.IsConnected() {
		return
	}
	if req.FrameId == 0 || req.FrameId > room.currentServerFrameID() {
		log.Printf("⚠️ 玩家 %d 的重连快照帧号非法 frame=%d request=%d",
			player.Ctx.Id, req.FrameId, req.RequestId)
		return
	}
	if !room.sendSnapshotReplayTo(target, req.FrameId, req.InlineSnapshotJson) {
		log.Printf("⚠️ 房间 %d 无法把快照 frame=%d 转发给玩家 %d（历史帧缺口）",
			room.ID, req.FrameId, target.Ctx.Id)
	}
}

// sendSnapshotReplayTo 把快照和 [frame+1, 服务端权威帧] 的操作发给重连玩家。
func (room *Room) sendSnapshotReplayTo(target *Player, frameID uint32, inlineSnapshotJSON string) bool {
	if target == nil || target.Ctx == nil || frameID == 0 || inlineSnapshotJSON == "" {
		return false
	}
	toFrame := room.currentServerFrameID()
	if toFrame < frameID {
		return false
	}
	fromFrame := frameID + 1
	frames := make([]*messages.InGameResponse, 0, toFrame-fromFrame+1)
	for id := fromFrame; id <= toFrame; id++ {
		response, ok := room.frameHistory[id]
		if !ok {
			// 快照之后缺帧时无法保证连续恢复，保留调用方先前发送的
			// checkpoint/完整历史帧兜底，避免制造新的断档。
			return false
		}
		frames = append(frames, response)
	}
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_ReplayFrames{
			ReplayFrames: &messages.ResponseReplayFrames{
				Frames:             frames,
				FromFrameId:        fromFrame,
				ToFrameId:          toFrame,
				CheckpointFrameId:  frameID,
				InlineSnapshotJson: inlineSnapshotJSON,
			},
		},
	}, target)
	log.Printf("📸 房间 %d 已把快照 frame=%d 转发给玩家 %d，并补齐 %d 帧",
		room.ID, frameID, target.Ctx.Id, len(frames))
	return true
}

// tickPendingRecoveries 丢弃超时的快照请求；兜底回放已经在重连时发送。
func (room *Room) tickPendingRecoveries(now time.Time) {
	for requestID, pending := range room.pendingRecoveries {
		if now.After(pending.deadline) {
			delete(room.pendingRecoveries, requestID)
			log.Printf("⌛ 房间 %d 快照请求超时 request=%d target=%d provider=%d",
				room.ID, requestID, pending.targetPlayerID, pending.providerID)
		}
	}
}

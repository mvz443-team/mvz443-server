package roomatom

import (
	"encoding/json"
	"log"
	"mvzserver/messages/pb"
	"sort"
)

const checkpointIntervalFrames uint32 = 200
const maxCheckpointInlineBytes = 256 * 1024

type stateCheckpoint struct {
	frameID  uint32
	hash     string
	inline   string
	voters   map[int]struct{}
	accepted bool
	mismatch bool
}

func (room *Room) recordCheckpoint(player *Player, req *messages.RequestBlank) {
	if player == nil || req == nil || req.HashFrameId == 0 || req.StateHashHex == "" {
		return
	}
	if req.HashFrameId%checkpointIntervalFrames != 0 {
		return
	}
	// 只有带 inline 正文的 checkpoint 才能作为重连恢复锚点。
	// 仅 hash 的 checkpoint 会让 LastCheckpointFrame 前进，但客户端
	// 重连时无法恢复该帧状态，随后从 checkpoint+1 重放必然错位。
	if req.InlineSnapshotJson == "" {
		recordDesyncEvent(room.ID, "checkpoint_without_inline_rejected", map[string]any{
			"playerId":    player.Ctx.Id,
			"frameId":     req.HashFrameId,
			"stateHash":   req.StateHashHex,
			"inlineBytes": 0,
		})
		return
	}
	if len(req.InlineSnapshotJson) > maxCheckpointInlineBytes || !json.Valid([]byte(req.InlineSnapshotJson)) {
		recordDesyncEvent(room.ID, "checkpoint_inline_invalid_rejected", map[string]any{
			"playerId":    player.Ctx.Id,
			"frameId":     req.HashFrameId,
			"inlineBytes": len(req.InlineSnapshotJson),
		})
		return
	}
	frameID := req.HashFrameId
	checkpoint := room.Checkpoints[frameID]
	if checkpoint == nil {
		checkpoint = &stateCheckpoint{
			frameID: frameID,
			hash:    req.StateHashHex,
			inline:  req.InlineSnapshotJson,
			voters:  make(map[int]struct{}),
		}
		room.Checkpoints[frameID] = checkpoint
	}
	if checkpoint.hash != req.StateHashHex {
		log.Printf("🚨 checkpoint 摘要不一致 frame=%d", frameID)
		checkpoint.mismatch = true
		return
	}
	if checkpoint.inline == "" && req.InlineSnapshotJson != "" {
		checkpoint.inline = req.InlineSnapshotJson
	}
	checkpoint.voters[player.Ctx.Id] = struct{}{}

	// checkpoint 与状态摘要一样，只有 committed frame 才能被采纳。
	committed := room.committedFrameID()
	room.tryAcceptCheckpoint(frameID, committed)
}

// tryAcceptCommittedCheckpoints 在 committed 推进后采纳已经双端确认的 checkpoint。
func (room *Room) tryAcceptCommittedCheckpoints(committed uint32) {
	for frameID := range room.Checkpoints {
		room.tryAcceptCheckpoint(frameID, committed)
	}
}

func (room *Room) tryAcceptCheckpoint(frameID uint32, committed uint32) {
	checkpoint := room.Checkpoints[frameID]
	if checkpoint == nil || checkpoint.accepted || checkpoint.mismatch {
		return
	}
	// checkpoint 只能向前推进；乱序到达的旧 checkpoint 不能把恢复锚点回退。
	if frameID <= room.LastCheckpointFrame {
		return
	}
	if frameID > committed || len(checkpoint.voters) < 2 {
		return
	}
	if checkpoint.inline == "" {
		return
	}
	checkpoint.accepted = true
	room.LastCheckpointFrame = frameID
	kind := uint32(1)
	if checkpoint.inline != "" {
		kind = 2
	}
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_CheckpointAccepted{
			CheckpointAccepted: &messages.ResponseCheckpointAccepted{
				FrameId:      frameID,
				SnapshotKind: kind,
			},
		},
	}, nil)
	log.Printf("✅ checkpoint 已确认 frame=%d kind=%d committed=%d", frameID, kind, committed)

	if len(room.Checkpoints) > 8 {
		frames := make([]uint32, 0, len(room.Checkpoints))
		for id := range room.Checkpoints {
			frames = append(frames, id)
		}
		sort.Slice(frames, func(i, j int) bool { return frames[i] < frames[j] })
		for _, id := range frames[:len(frames)-4] {
			delete(room.Checkpoints, id)
		}
	}
}

// replayFrameRange 计算重连回放的 [from, to] 闭区间。
// toFrame 必须使用服务端已生成的最新权威帧，而不能用 committedFrameID：
// 重连瞬间新连接的 ACK 被重置到 checkpoint，committed 恰好等于 checkpoint，
// 会让回放区间为空，客户端恢复 checkpoint 后无帧可追，永久冻结。
func (room *Room) replayFrameRange(checkpointFrameID uint32) (uint32, uint32) {
	fromFrame := checkpointFrameID + 1
	if checkpointFrameID == 0 {
		fromFrame = room.earliestHistoryFrameID()
	}
	return fromFrame, room.currentServerFrameID()
}

func (room *Room) sendReplayFromCheckpoint(player *Player) bool {
	if player == nil || player.Ctx == nil {
		return false
	}
	checkpointFrameID := room.LastCheckpointFrame
	inlineSnapshotJSON := room.checkpointSnapshotJSON()
	// 防御：理论上采纳时已保证 inline 非空；若内存中的 checkpoint 不完整，
	// 必须退回“从头重放”，不能把客户端指到无法恢复的 checkpoint+1。
	if checkpointFrameID > 0 && inlineSnapshotJSON == "" {
		checkpointFrameID = 0
	}
	fromFrame, toFrame := room.replayFrameRange(checkpointFrameID)
	if fromFrame > toFrame {
		if checkpointFrameID > 0 {
			// 没有需要重放的帧时也必须把 inline 快照发给重连客户端，
			// 否则它无法从 checkpoint 恢复状态。
			room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
				Payload: &messages.RoomResponse_ReplayFrames{
					ReplayFrames: &messages.ResponseReplayFrames{
						Frames:             nil,
						FromFrameId:        fromFrame,
						ToFrameId:          toFrame,
						CheckpointFrameId:  checkpointFrameID,
						InlineSnapshotJson: inlineSnapshotJSON,
					},
				},
			}, player)
			return true
		}
		return false
	}
	frames := make([]*messages.InGameResponse, 0, toFrame-fromFrame+1)
	for frameID := fromFrame; frameID <= toFrame; frameID++ {
		if response, ok := room.frameHistory[frameID]; ok {
			frames = append(frames, response)
		}
	}
	if len(frames) == 0 {
		if checkpointFrameID > 0 {
			room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
				Payload: &messages.RoomResponse_ReplayFrames{
					ReplayFrames: &messages.ResponseReplayFrames{
						Frames:             nil,
						FromFrameId:        fromFrame,
						ToFrameId:          toFrame,
						CheckpointFrameId:  checkpointFrameID,
						InlineSnapshotJson: inlineSnapshotJSON,
					},
				},
			}, player)
			return true
		}
		return false
	}
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_ReplayFrames{
			ReplayFrames: &messages.ResponseReplayFrames{
				Frames:             frames,
				FromFrameId:        fromFrame,
				ToFrameId:          toFrame,
				CheckpointFrameId:  checkpointFrameID,
				InlineSnapshotJson: inlineSnapshotJSON,
			},
		},
	}, player)
	return true
}

func (room *Room) checkpointSnapshotJSON() string {
	if room.LastCheckpointFrame == 0 {
		return ""
	}
	checkpoint := room.Checkpoints[room.LastCheckpointFrame]
	if checkpoint == nil || !checkpoint.accepted {
		return ""
	}
	return checkpoint.inline
}

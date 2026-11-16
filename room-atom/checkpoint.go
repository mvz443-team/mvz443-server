package roomatom

import (
	"log"
	"mvzserver/messages/pb"
	"sort"
)

const checkpointIntervalFrames uint32 = 200

type stateCheckpoint struct {
	frameID  uint32
	hash     string
	inline   string
	voters   map[int]struct{}
	accepted bool
}

func (room *Room) recordCheckpoint(player *Player, req *messages.RequestBlank) {
	if player == nil || req == nil || req.HashFrameId == 0 || req.StateHashHex == "" {
		return
	}
	if req.HashFrameId%checkpointIntervalFrames != 0 {
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
		return
	}
	if checkpoint.inline == "" && req.InlineSnapshotJson != "" {
		checkpoint.inline = req.InlineSnapshotJson
	}
	checkpoint.voters[player.Ctx.Id] = struct{}{}

	if checkpoint.accepted || len(checkpoint.voters) < 2 {
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
	log.Printf("✅ checkpoint 已确认 frame=%d kind=%d", frameID, kind)

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

func (room *Room) sendReplayFromCheckpoint(player *Player) bool {
	if player == nil || player.Ctx == nil {
		return false
	}
	fromFrame := room.LastCheckpointFrame + 1
	if room.LastCheckpointFrame == 0 {
		fromFrame = room.earliestHistoryFrameID()
	}
	toFrame := room.currentServerFrameID()
	if fromFrame > toFrame {
		return false
	}
	frames := make([]*messages.InGameResponse, 0, toFrame-fromFrame+1)
	for frameID := fromFrame; frameID <= toFrame; frameID++ {
		if response, ok := room.frameHistory[frameID]; ok {
			frames = append(frames, response)
		}
	}
	if len(frames) == 0 {
		return false
	}
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_ReplayFrames{
			ReplayFrames: &messages.ResponseReplayFrames{
				Frames:       frames,
				FromFrameId:  fromFrame,
				ToFrameId:    toFrame,
			},
		},
	}, player)
	return true
}

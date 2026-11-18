package roomatom

import (
	messages "mvzserver/messages/pb"
	"testing"
)

func TestCheckpointAcceptedOnlyWhenBothPlayersAgree(t *testing.T) {
	room := newTestRoom(200)
	room.Checkpoints = make(map[uint32]*stateCheckpoint)
	p1, p2 := testHashPlayer(100), testHashPlayer(101)

	room.recordCheckpoint(p1, &messages.RequestBlank{
		HashFrameId:     200,
		StateHashHex:    "abcd",
		InlineSnapshotJson: `{"values":{}}`,
	})
	if room.LastCheckpointFrame != 0 {
		t.Fatal("单端上报不能确认 checkpoint")
	}

	room.recordCheckpoint(p2, &messages.RequestBlank{
		HashFrameId:     200,
		StateHashHex:    "abcd",
		InlineSnapshotJson: `{"values":{}}`,
	})
	if room.LastCheckpointFrame != 200 {
		t.Fatal("双端摘要一致后必须确认 checkpoint")
	}
	if !room.Checkpoints[200].accepted {
		t.Fatal("checkpoint 应标记为已确认")
	}
}

func TestCheckpointMismatchIsIgnored(t *testing.T) {
	room := newTestRoom(200)
	room.Checkpoints = make(map[uint32]*stateCheckpoint)
	room.recordCheckpoint(testHashPlayer(100), &messages.RequestBlank{HashFrameId: 200, StateHashHex: "aaaa"})
	room.recordCheckpoint(testHashPlayer(101), &messages.RequestBlank{HashFrameId: 200, StateHashHex: "bbbb"})
	if room.LastCheckpointFrame != 0 {
		t.Fatal("摘要不一致不得确认 checkpoint")
	}
}

func TestReplayRangeUsesServerFrameNotCommitted(t *testing.T) {
	// 场景：checkpoint=200，服务端已推进到 350，重连端 ACK 被重置到 200，
	// 此时 committed=200。回放必须从 201 到 350，而不是空区间。
	room := newTestRoom(350)
	room.LastCheckpointFrame = 200

	from, to := room.replayFrameRange(200)
	if from != 201 || to != 350 {
		t.Fatalf("回放区间应为 [201,350]，得到 [%d,%d]（若 to=committed=200 则回放为空，客户端会冻结）", from, to)
	}

	// 无 checkpoint 时从最早历史帧开始，但仍以服务端最新帧为上界。
	room.frameHistory = map[uint32]*messages.InGameResponse{}
	for f := uint32(180); f <= 350; f++ {
		room.frameHistory[f] = &messages.InGameResponse{FrameId: f}
	}
	from, to = room.replayFrameRange(0)
	if from != 180 || to != 350 {
		t.Fatalf("无 checkpoint 时回放区间应为 [180,350]，得到 [%d,%d]", from, to)
	}
}

func TestReplayIncludesAcceptedCheckpointSnapshot(t *testing.T) {
	room := newTestRoom(200)
	room.Checkpoints = make(map[uint32]*stateCheckpoint)
	p1, p2 := testHashPlayer(100), testHashPlayer(101)
	inline := `{"version":1,"hash":"abcd","payload":{"values":{}}}`
	room.recordCheckpoint(p1, &messages.RequestBlank{HashFrameId: 200, StateHashHex: "abcd", InlineSnapshotJson: inline})
	room.recordCheckpoint(p2, &messages.RequestBlank{HashFrameId: 200, StateHashHex: "abcd", InlineSnapshotJson: inline})
	if got := room.checkpointSnapshotJSON(); got != inline {
		t.Fatalf("replay 应携带已确认快照，得到 %q", got)
	}
}

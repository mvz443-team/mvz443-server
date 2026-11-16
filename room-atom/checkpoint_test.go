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

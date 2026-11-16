package roomatom

import (
	"mvzserver/clients"
	messages "mvzserver/messages/pb"
	"testing"
)

func testArbitrationRoom(currentFrame uint32) *Room {
	room := newTestRoom(currentFrame)
	room.scheduledFrames = make(map[uint32]*FrameSubmission)
	room.EndGameVotes = make(map[int]uint32)
	return room
}

func testVotingPlayer(id int) *Player {
	ctx := &clients.ClientCtx{}
	ctx.Id = id
	return &Player{Ctx: ctx}
}

func TestEndGameVoteWaitsForBothPlayers(t *testing.T) {
	room := testArbitrationRoom(100)
	p1, p2 := testVotingPlayer(100), testVotingPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.RegisterEndGameVote(p1, 1)
	if room.pendingEndFrame != 0 {
		t.Fatal("单个玩家上报后不应立刻结束房间")
	}

	room.RegisterEndGameVote(p2, 1)
	if room.pendingEndFrame == 0 {
		t.Fatal("双方一致上报后应排入权威结束帧")
	}
	submission := room.scheduledFrames[room.pendingEndFrame]
	if submission == nil || len(submission.Operations) != 1 {
		t.Fatalf("结束帧应包含唯一 game_event，得到 %+v", submission)
	}
	event, ok := submission.Operations[0].Payload.(*messages.InGameOperation_GameEvent)
	if !ok || event.GameEvent.Message != "1" {
		t.Fatalf("一致胜利应写入 message=1，得到 %+v", submission.Operations[0])
	}
}

func TestEndGameResultConflictIsMarkedLoss(t *testing.T) {
	room := testArbitrationRoom(50)
	p1, p2 := testVotingPlayer(100), testVotingPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.RegisterEndGameVote(p1, 1)
	room.RegisterEndGameVote(p2, 0)

	submission := room.scheduledFrames[room.pendingEndFrame]
	if submission == nil || len(submission.Operations) == 0 {
		t.Fatal("结果冲突也必须排入结束帧止损")
	}
	event := submission.Operations[0].Payload.(*messages.InGameOperation_GameEvent)
	if event.GameEvent.Message != "0" {
		t.Fatalf("结果冲突应按异常对局判负，得到 message=%s", event.GameEvent.Message)
	}
}

func TestEndGameVoteIsIdempotent(t *testing.T) {
	room := testArbitrationRoom(10)
	p1 := testVotingPlayer(100)
	room.RoomCtx.Players.Store(100, p1)

	room.RegisterEndGameVote(p1, 1)
	room.RegisterEndGameVote(p1, 0)
	if result := room.EndGameVotes[100]; result != 1 {
		t.Fatalf("重复上报不得覆盖首次结果，得到 %d", result)
	}
}

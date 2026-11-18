package roomatom

import (
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"testing"
)

func TestCommittedFrameUsesMinAckOfConnectedPlayers(t *testing.T) {
	room := lifecycleTestRoom(20)
	p1 := connectedPlayer(100)
	p2 := connectedPlayer(101)
	p1.Ctx.LatestAckFrameID.Store(20)
	p2.Ctx.LatestAckFrameID.Store(18)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	if got := room.committedFrameID(); got != 18 {
		t.Fatalf("committed frame 必须取双方最小 ACK，得到 %d", got)
	}

	p2.Ctx.State = constants.PlayerStateDisconnected
	if got := room.committedFrameID(); got != 20 {
		t.Fatalf("掉线玩家不应阻塞 committed frame，得到 %d", got)
	}
}

func TestStateHashOnlyComparedAfterBothAck(t *testing.T) {
	room := lifecycleTestRoom(20)
	p1 := connectedPlayer(100)
	p2 := connectedPlayer(101)
	p1.Ctx.LatestAckFrameID.Store(18)
	p2.Ctx.LatestAckFrameID.Store(18)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.recordStateHash(p1, 20, 0x1111)
	room.recordStateHash(p2, 20, 0x2222)
	if room.DesyncDetected {
		t.Fatal("未 committed 的帧摘要不得提前判定分叉")
	}

	p1.Ctx.LatestAckFrameID.Store(20)
	p2.Ctx.LatestAckFrameID.Store(20)
	room.evaluateCommittedStateHashes(room.committedFrameID())
	if !room.DesyncDetected {
		t.Fatal("双方 ACK 后必须比较该帧摘要并判定分叉")
	}
}

func TestPruneKeepsUncommittedFramesAndReplayWindow(t *testing.T) {
	room := lifecycleTestRoom(25)
	p1 := connectedPlayer(100)
	p2 := connectedPlayer(101)
	p1.Ctx.LatestAckFrameID.Store(18)
	p2.Ctx.LatestAckFrameID.Store(18)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)
	room.frameHistory = make(map[uint32]*messages.InGameResponse)
	for frame := uint32(1); frame <= 25; frame++ {
		room.frameHistory[frame] = &messages.InGameResponse{FrameId: frame}
	}

	// 没有 checkpoint 时必须保留全部历史，供重连从头重放。
	room.LastCheckpointFrame = 0
	room.pruneFrameHistory()
	if len(room.frameHistory) != 25 {
		t.Fatalf("首个 checkpoint 前不得裁剪历史，剩余 %d 帧", len(room.frameHistory))
	}

	// 有 checkpoint 时只裁 checkpoint 及更早，checkpoint 之后即使已 ACK 也要保留。
	room.LastCheckpointFrame = 10
	room.pruneFrameHistory()
	if _, exists := room.frameHistory[10]; exists {
		t.Fatal("checkpoint 及更早帧可以裁剪")
	}
	for frame := uint32(11); frame <= 25; frame++ {
		if _, exists := room.frameHistory[frame]; !exists {
			t.Fatalf("checkpoint 之后的帧必须保留，缺少 %d", frame)
		}
	}
}

func TestCheckpointOnlyAcceptedAfterCommitted(t *testing.T) {
	room := lifecycleTestRoom(200)
	p1 := connectedPlayer(100)
	p2 := connectedPlayer(101)
	p1.Ctx.LatestAckFrameID.Store(198)
	p2.Ctx.LatestAckFrameID.Store(198)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	req := &messages.RequestBlank{
		HashFrameId:        200,
		StateHashHex:       "deadbeef",
		InlineSnapshotJson: `{"version":5}`,
	}
	room.recordCheckpoint(p1, req)
	room.recordCheckpoint(p2, req)
	if room.LastCheckpointFrame != 0 {
		t.Fatal("未 committed 的 checkpoint 不得采纳")
	}

	p1.Ctx.LatestAckFrameID.Store(200)
	p2.Ctx.LatestAckFrameID.Store(200)
	room.tryAcceptCommittedCheckpoints(room.committedFrameID())
	if room.LastCheckpointFrame != 200 {
		t.Fatalf("committed 后应采纳 checkpoint，得到 %d", room.LastCheckpointFrame)
	}
}

// 只有 hash、没有 inline 正文的 checkpoint 不能推进恢复锚点。
func TestHashOnlyCheckpointRejected(t *testing.T) {
	room := lifecycleTestRoom(200)
	p1 := connectedPlayer(100)
	p2 := connectedPlayer(101)
	p1.Ctx.LatestAckFrameID.Store(200)
	p2.Ctx.LatestAckFrameID.Store(200)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	req := &messages.RequestBlank{
		HashFrameId:        200,
		StateHashHex:       "deadbeef",
		InlineSnapshotJson: "",
	}
	room.recordCheckpoint(p1, req)
	room.recordCheckpoint(p2, req)
	room.tryAcceptCommittedCheckpoints(room.committedFrameID())
	if room.LastCheckpointFrame != 0 {
		t.Fatalf("hash-only checkpoint 不得被采纳，得到 %d", room.LastCheckpointFrame)
	}
}

// ACK 超过服务端已生成帧时必须被钳制，否则 committed frame 会“穿越”到未来。
func TestAckBeyondGeneratedFrameClamped(t *testing.T) {
	room := lifecycleTestRoom(5)
	p1 := connectedPlayer(100)
	room.RoomCtx.Players.Store(100, p1)

	req := &messages.RequestBlank{FrameId: 99, AckFrameId: 99}
	if !room.acceptInGameBase(p1, req) {
		t.Fatal("InGame 阶段的 blank 应被接受")
	}
	if got := p1.Ctx.LatestFrameID.Load(); got != 5 {
		t.Fatalf("Future frame 必须钳制到 5，得到 %d", got)
	}
	if got := p1.Ctx.LatestAckFrameID.Load(); got != 5 {
		t.Fatalf("Future ACK 必须钳制到 5，得到 %d", got)
	}
	if got := room.committedFrameID(); got != 5 {
		t.Fatalf("committed frame 不得超过已生成帧 5，得到 %d", got)
	}
}

// ACK 不能超过该客户端自报的进度帧，否则 ACK 会凭空推进 committed。
func TestAckBeyondReportedProgressClamped(t *testing.T) {
	room := lifecycleTestRoom(20)
	p1 := connectedPlayer(100)
	room.RoomCtx.Players.Store(100, p1)

	req := &messages.RequestBlank{FrameId: 10, AckFrameId: 20}
	if !room.acceptInGameBase(p1, req) {
		t.Fatal("InGame 阶段的 blank 应被接受")
	}
	if got := p1.Ctx.LatestFrameID.Load(); got != 10 {
		t.Fatalf("进度帧应为 10，得到 %d", got)
	}
	if got := p1.Ctx.LatestAckFrameID.Load(); got != 10 {
		t.Fatalf("ACK 不得超过进度帧 10，得到 %d", got)
	}
	if got := room.committedFrameID(); got != 10 {
		t.Fatalf("committed 只能到 10，得到 %d", got)
	}
}

// 乱序到达的旧 checkpoint 不能把 LastCheckpointFrame 回退。
func TestCheckpointFrameMonotonic(t *testing.T) {
	room := lifecycleTestRoom(500)
	room.LastCheckpointFrame = 400
	room.Checkpoints[200] = &stateCheckpoint{
		frameID: 200,
		hash:    "deadbeef",
		inline:  `{"version":5}`,
		voters:  map[int]struct{}{100: {}, 101: {}},
	}
	room.tryAcceptCheckpoint(200, 500)
	if room.LastCheckpointFrame != 400 {
		t.Fatalf("旧 checkpoint 不得回退权威锚点，得到 %d", room.LastCheckpointFrame)
	}
}

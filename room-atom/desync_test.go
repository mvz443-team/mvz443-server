package roomatom

import (
	"mvzserver/clients"
	messages "mvzserver/messages/pb"
	"testing"
)

func testHashPlayer(id int) *Player {
	ctx := &clients.ClientCtx{}
	ctx.Id = id
	return &Player{Ctx: ctx}
}

func TestStateHashMismatchIsDetected(t *testing.T) {
	room := newTestRoom(21)
	room.FrameHashes = make(map[uint32]map[int]uint32)
	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	// 只有双方 ACK 的帧才会被比较：这里把 ACK 推到 21。
	p1.Ctx.LatestAckFrameID.Store(21)
	p2.Ctx.LatestAckFrameID.Store(21)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.recordStateHash(p1, 20, 1111)
	if room.DesyncDetected {
		t.Fatal("只有一端上报时不应判定分叉")
	}
	room.recordStateHash(p2, 20, 1111)
	if room.DesyncDetected {
		t.Fatal("两端摘要一致时不应判定分叉")
	}
	room.recordStateHash(p2, 21, 2222)
	room.recordStateHash(p1, 21, 3333)
	if !room.DesyncDetected {
		t.Fatal("同一帧摘要不一致必须判定分叉")
	}
}

func TestStateHashPruneKeepsMapBounded(t *testing.T) {
	room := newTestRoom(0)
	room.FrameHashes = make(map[uint32]map[int]uint32)
	for frame := uint32(1); frame <= 200; frame++ {
		room.recordStateHash(testHashPlayer(100), frame, frame)
	}
	if len(room.FrameHashes) > 64 {
		t.Fatalf("摘要表必须保持有界，当前 %d 项", len(room.FrameHashes))
	}
}

func TestHashSchemaVersionMismatchSkipsDesyncDetection(t *testing.T) {
	room := newTestRoom(0)
	room.FrameHashes = make(map[uint32]map[int]uint32)

	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	p1.Ctx.LatestFrameID.Store(1)
	p2.Ctx.LatestFrameID.Store(1)
	p1.Ctx.StateHashVersion.Store(2)
	p2.Ctx.StateHashVersion.Store(0)
	p1.Ctx.StateHashVersionReported.Store(true)
	p2.Ctx.StateHashVersionReported.Store(true)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.checkHashSchemaVersions()
	if !room.HashSchemaMismatch {
		t.Fatal("新旧客户端哈希口径不同时必须标记版本不一致")
	}

	room.recordStateHash(p1, 20, 1111)
	room.recordStateHash(p2, 20, 2222)
	if room.DesyncDetected {
		t.Fatal("版本不一致时不能再误报为游戏状态分叉")
	}
}

func TestHashSchemaSameVersionKeepsDesyncDetection(t *testing.T) {
	room := newTestRoom(20)
	room.FrameHashes = make(map[uint32]map[int]uint32)

	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	p1.Ctx.LatestFrameID.Store(1)
	p2.Ctx.LatestFrameID.Store(1)
	p1.Ctx.LatestAckFrameID.Store(20)
	p2.Ctx.LatestAckFrameID.Store(20)
	p1.Ctx.StateHashVersion.Store(2)
	p2.Ctx.StateHashVersion.Store(2)
	p1.Ctx.StateHashVersionReported.Store(true)
	p2.Ctx.StateHashVersionReported.Store(true)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.checkHashSchemaVersions()
	if room.HashSchemaMismatch {
		t.Fatal("相同哈希口径不应标记版本不一致")
	}

	room.recordStateHash(p1, 20, 1111)
	room.recordStateHash(p2, 20, 2222)
	if !room.DesyncDetected {
		t.Fatal("版本一致时仍必须检测真实状态分叉")
	}
}

func TestAcceptInGameBaseDetectsHashSchemaVersionMismatch(t *testing.T) {
	room := lifecycleTestRoom(0)
	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	if ok := room.acceptInGameBase(p1, &messages.RequestBlank{
		FrameId:          1,
		AckFrameId:       0,
		StateHashVersion: 2,
	}); !ok {
		t.Fatal("第一端心跳应被接受")
	}
	if ok := room.acceptInGameBase(p2, &messages.RequestBlank{
		FrameId:          1,
		AckFrameId:       0,
		StateHashVersion: 0,
	}); !ok {
		t.Fatal("第二端心跳应被接受")
	}
	if !room.HashSchemaMismatch {
		t.Fatal("心跳版本不一致时必须标记 HashSchemaMismatch")
	}
}

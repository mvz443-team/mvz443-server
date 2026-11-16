package roomatom

import (
	"mvzserver/clients"
	"testing"
)

func testHashPlayer(id int) *Player {
	ctx := &clients.ClientCtx{}
	ctx.Id = id
	return &Player{Ctx: ctx}
}

func TestStateHashMismatchIsDetected(t *testing.T) {
	room := newTestRoom(0)
	room.FrameHashes = make(map[uint32]map[int]uint32)
	// 不注册到 Players，避免测试中触发真实网络写
	room.recordStateHash(testHashPlayer(100), 20, 1111)
	if room.DesyncDetected {
		t.Fatal("只有一端上报时不应判定分叉")
	}
	room.recordStateHash(testHashPlayer(101), 20, 1111)
	if room.DesyncDetected {
		t.Fatal("两端摘要一致时不应判定分叉")
	}
	room.recordStateHash(testHashPlayer(101), 21, 2222)
	room.recordStateHash(testHashPlayer(100), 21, 3333)
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

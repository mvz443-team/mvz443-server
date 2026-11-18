package roomatom

import (
	"mvzserver/clients"
	"mvzserver/constants"
	"mvzserver/messages/pb"
	"testing"
	"time"
)

func addRecoveryTestPlayer(room *Room, id int, latency uint32) *clients.Player {
	ctx := clients.NewClientCtx(nil, id)
	ctx.State = constants.PlayerStateConnected
	ctx.ReportedLatencyMs.Store(latency)
	player := &clients.Player{Ctx: ctx, Name: "p"}
	room.RoomCtx.Players.Store(id, player)
	return player
}

func TestSelectSnapshotProviderPrefersLowestReportedLatency(t *testing.T) {
	room := NewRoom(1, nil)
	room.GameStage.Store(constants.STAGE_InGame)
	addRecoveryTestPlayer(room, 1, 80)
	addRecoveryTestPlayer(room, 2, 25)
	addRecoveryTestPlayer(room, 3, 55)

	best := room.selectSnapshotProvider(0)
	if best == nil || best.Ctx.Id != 2 {
		t.Fatalf("应选择延迟最低的玩家 2，得到 %v", best)
	}

	// 重连目标自己不能被选为快照来源。
	best = room.selectSnapshotProvider(2)
	if best == nil || best.Ctx.Id != 3 {
		t.Fatalf("排除玩家 2 后应选择玩家 3，得到 %v", best)
	}

	// 断线席位不能提供快照。
	player3, _ := room.RoomCtx.Players.Load(3)
	player3.Ctx.State = constants.PlayerStateDisconnected
	best = room.selectSnapshotProvider(2)
	if best == nil || best.Ctx.Id != 1 {
		t.Fatalf("断线玩家不得作为来源，应选择玩家 1，得到 %v", best)
	}
}

func TestSendSnapshotReplayRequiresContinuousHistory(t *testing.T) {
	room := NewRoom(1, nil)
	room.GameStage.Store(constants.STAGE_InGame)
	room.RoomCtx.NextFrameID.Store(11)
	room.frameHistory[8] = &messages.InGameResponse{FrameId: 8}
	room.frameHistory[9] = &messages.InGameResponse{FrameId: 9}
	room.frameHistory[10] = &messages.InGameResponse{FrameId: 10}
	target := addRecoveryTestPlayer(room, 2, 10)

	if ok := room.sendSnapshotReplayTo(target, 7, `{"frameId":7,"values":{}}`); !ok {
		t.Fatal("快照帧之后历史帧连续时应能转发")
	}
	// 缺帧时不能把客户端指到不连续的分支，应保持兜底回放。
	delete(room.frameHistory, 9)
	if ok := room.sendSnapshotReplayTo(target, 7, `{"frameId":7,"values":{}}`); ok {
		t.Fatal("历史帧缺口时不得转发快照回放")
	}
}

func TestPendingRecoveryTimesOut(t *testing.T) {
	room := NewRoom(1, nil)
	room.pendingRecoveries[9] = &pendingRecovery{
		targetPlayerID: 2,
		providerID:     1,
		requestID:      9,
		deadline:       time.Now().Add(-time.Second),
	}
	room.tickPendingRecoveries(time.Now())
	if _, ok := room.pendingRecoveries[9]; ok {
		t.Fatal("超时的快照请求应被清理")
	}
}

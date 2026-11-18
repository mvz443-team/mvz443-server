package roomatom

import (
	"mvzserver/clients"
	"mvzserver/constants"
	"mvzserver/messages/pb"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
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

// 重连目标拿到的 inline 快照与补帧必须无重叠：快照帧本身已经在 provider
// 的状态里，若再回放 frame 会让该帧的种植/星碎操作执行两次，双方立刻分叉。
func TestHandleRequestSnapshotResponseReplaysAfterSnapshotFrame(t *testing.T) {
	room := NewRoom(31, nil)
	room.GameStage.Store(constants.STAGE_InGame)
	room.RoomCtx.NextFrameID.Store(11)
	for frameID := uint32(7); frameID <= 10; frameID++ {
		room.frameHistory[frameID] = &messages.InGameResponse{
			FrameId:    frameID,
			Operations: []*messages.InGameOperation{{ProcessFrameId: frameID}},
		}
	}
	provider := addRecoveryTestPlayer(room, 1, 20)
	target := addRecoveryTestPlayer(room, 2, 90)
	targetWrites := make(chan []byte, 1)
	capture := clients.NewCapturingConnWriter(targetWrites)
	target.Ctx.SetWriterForTest(capture)
	defer capture.Close()
	room.pendingRecoveries[5] = &pendingRecovery{
		targetPlayerID: target.Ctx.Id,
		providerID:     provider.Ctx.Id,
		requestID:      5,
		deadline:       time.Now().Add(time.Second),
	}

	room.HandleRequestSnapshotResponse(provider, &messages.RequestSnapshotResponse{
		RequestId:          5,
		FrameId:            7,
		InlineSnapshotJson: `{"version":1,"hash":"0","payload":{"version":1,"values":{}}}`,
	})

	if _, ok := room.pendingRecoveries[5]; ok {
		t.Fatal("快照应答处理后 pending 请求应被消费")
	}
	select {
	case data := <-targetWrites:
		response := &messages.RoomResponse{}
		if err := proto.Unmarshal(data, response); err != nil {
			t.Fatalf("重连回放消息解码失败: %v", err)
		}
		replay := response.GetReplayFrames()
		if replay == nil || replay.CheckpointFrameId != 7 || replay.FromFrameId != 8 || replay.ToFrameId != 10 {
			t.Fatalf("快照帧后应回放 8..10，实际 %+v", replay)
		}
		if len(replay.Frames) != 3 || replay.Frames[0].FrameId != 8 || replay.Frames[2].FrameId != 10 {
			t.Fatalf("回放帧范围错误: %+v", replay.Frames)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("未向重连目标发送回放消息")
	}
}

package roomatom

import (
	"mvzserver/clients"
	"mvzserver/constants"
	"testing"
)

func TestReconnectStartsFromCheckpointAndKeepsMonotonicProgress(t *testing.T) {
	room := NewRoom(1, nil)
	room.key = "secret"
	room.GameStage.Store(constants.STAGE_InGame)
	room.resetInGameState()

	oldCtx := clients.NewClientCtx(nil, 100)
	oldCtx.LatestAckFrameID.Store(42)
	oldCtx.LatestFrameID.Store(45)
	player := &clients.Player{Ctx: oldCtx, ConnectionEpoch: 1}
	room.RoomCtx.Players.Store(100, player)

	reply := make(chan *clients.Player, 1)
	room.handleReconnect(&ReconnectRequest{PlayerID: 100, Key: "secret", Reply: reply})

	reconnected := <-reply
	if reconnected == nil {
		t.Fatal("InGame 阶段同 id + 正确密钥应允许重连")
	}
	if reconnected.Ctx == oldCtx {
		t.Fatal("重连必须替换为新的连接上下文")
	}
	// 新上下文不能继承旧 ACK：页面刷新后的客户端还没执行任何帧，
	// 继承旧 ACK 会让服务端把“未真正执行的帧”当成双方权威帧。
	if reconnected.Ctx.LatestAckFrameID.Load() != 0 || reconnected.Ctx.LatestFrameID.Load() != 0 {
		t.Fatalf("重连必须从 checkpoint（当前为 0）开始，得到 ack=%d frame=%d",
			reconnected.Ctx.LatestAckFrameID.Load(), reconnected.Ctx.LatestFrameID.Load())
	}
	if reconnected.ConnectionEpoch != 2 {
		t.Fatalf("重连后连接代数应自增，得到 %d", reconnected.ConnectionEpoch)
	}

	// socket open 后客户端会立刻上报当前真实进度；UpdatePlayerFrame 单调抬升。
	reconnected.Ctx.UpdatePlayerFrame(45, 42)
	if got := reconnected.Ctx.LatestAckFrameID.Load(); got != 42 {
		t.Fatalf("客户端上报后 ACK 应抬升到 42，得到 %d", got)
	}
	if got := reconnected.Ctx.LatestFrameID.Load(); got != 45 {
		t.Fatalf("客户端上报后 frame 应抬升到 45，得到 %d", got)
	}
}

func TestReconnectResumesFromLastCheckpoint(t *testing.T) {
	room := NewRoom(1, nil)
	room.key = "secret"
	room.GameStage.Store(constants.STAGE_InGame)
	room.resetInGameState()
	room.LastCheckpointFrame = 40
	room.RoomCtx.NextFrameID.Store(61)

	oldCtx := clients.NewClientCtx(nil, 100)
	oldCtx.LatestAckFrameID.Store(50)
	oldCtx.LatestFrameID.Store(55)
	player := &clients.Player{Ctx: oldCtx, ConnectionEpoch: 1}
	room.RoomCtx.Players.Store(100, player)

	reply := make(chan *clients.Player, 1)
	room.handleReconnect(&ReconnectRequest{PlayerID: 100, Key: "secret", Reply: reply})
	reconnected := <-reply
	if reconnected == nil {
		t.Fatal("InGame 阶段重连应成功")
	}
	if got := reconnected.Ctx.LatestAckFrameID.Load(); got != 40 {
		t.Fatalf("重连 ACK 应锚定到 checkpoint=40，得到 %d", got)
	}
	if got := reconnected.Ctx.LatestFrameID.Load(); got != 40 {
		t.Fatalf("重连帧进度应锚定到 checkpoint=40，得到 %d", got)
	}
}

func TestReconnectRejectsWrongKeyAndAllowsLobbyStages(t *testing.T) {
	room := NewRoom(1, nil)
	room.key = "secret"
	room.GameStage.Store(constants.STAGE_InGame)
	player := &clients.Player{Ctx: clients.NewClientCtx(nil, 100)}
	room.RoomCtx.Players.Store(100, player)

	reply := make(chan *clients.Player, 1)
	room.handleReconnect(&ReconnectRequest{PlayerID: 100, Key: "wrong", Reply: reply})
	if <-reply != nil {
		t.Fatal("错误密钥不得重连")
	}

	// 主页“开始游戏”会话检查后的重连需要覆盖准备阶段：
	// 非 InGame 阶段重新入座并下发房间状态，而不是保留帧补发。
	room.GameStage.Store(constants.STAGE_Preparing)
	reply2 := make(chan *clients.Player, 1)
	room.handleReconnect(&ReconnectRequest{PlayerID: 100, Key: "secret", Reply: reply2})
	if <-reply2 == nil {
		t.Fatal("Preparing 阶段持正确密钥应可重新入座")
	}
}

package roomatom

import (
	"mvzserver/clients"
	"mvzserver/constants"
	"testing"
)

func TestReconnectPreservesAckAndEpoch(t *testing.T) {
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
	if reconnected.Ctx.LatestAckFrameID.Load() != 42 || reconnected.Ctx.LatestFrameID.Load() != 45 {
		t.Fatalf("重连必须保留帧进度，得到 ack=%d frame=%d",
			reconnected.Ctx.LatestAckFrameID.Load(), reconnected.Ctx.LatestFrameID.Load())
	}
	if reconnected.ConnectionEpoch != 2 {
		t.Fatalf("重连后连接代数应自增，得到 %d", reconnected.ConnectionEpoch)
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

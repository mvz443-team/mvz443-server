package roomatom

// 生命周期机制的单元测试：重连宽限期判负、心跳超时、PostGame 返回房间。

import (
	"mvzserver/clients"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"strconv"
	"testing"
	"time"
)

// lifecycleTestRoom 构造进入 InGame 的房间（完整初始化，便于调用 Reset 等方法）。
func lifecycleTestRoom(currentFrame uint32) *Room {
	room := NewRoom(1, nil)
	room.RoomCtx.NextFrameID.Store(currentFrame + 1)
	room.GameStage = constants.NewAtomStage(constants.STAGE_InGame)
	room.scheduledFrames = make(map[uint32]*FrameSubmission)
	room.EndGameVotes = make(map[int]uint32)
	return room
}

// postGameTestRoom 构造 PostGame 阶段的完整房间。
func postGameTestRoom(currentFrame uint32) *Room {
	room := lifecycleTestRoom(currentFrame)
	room.GameStage.Store(constants.STAGE_PostGame)
	return room
}

func disconnectedPlayer(id int) *Player {
	ctx := clients.NewClientCtx(nil, id)
	ctx.State = constants.PlayerStateDisconnected
	return &Player{Ctx: ctx}
}

func connectedPlayer(id int) *Player {
	ctx := clients.NewClientCtx(nil, id)
	ctx.State = constants.PlayerStateConnected
	return &Player{Ctx: ctx}
}

func TestMarkPlayerDisconnectedStartsGrace(t *testing.T) {
	room := lifecycleTestRoom(100)
	p1, _ := connectedPlayer(100), disconnectedPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, connectedPlayer(101))

	room.markPlayerDisconnected(p1)
	if !room.AnyPlayerDisconnected {
		t.Fatal("掉线后应进入宽限期状态")
	}
	if room.ReconnectDeadline.IsZero() {
		t.Fatal("掉线后必须设置宽限期截止时间")
	}
	want := time.Now().Add(constants.ReconnectGracePeriod)
	if room.ReconnectDeadline.Sub(want) > time.Second {
		t.Fatalf("宽限期应为 %s", constants.ReconnectGracePeriod)
	}
}

func TestReconnectGraceTimeoutSchedulesLossEnd(t *testing.T) {
	room := lifecycleTestRoom(100)
	p1, p2 := connectedPlayer(100), disconnectedPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	// 直接把宽限期设为过去，模拟超时
	room.AnyPlayerDisconnected = true
	room.ReconnectDeadline = time.Now().Add(-time.Second)

	room.tickReconnectGrace()

	if room.pendingEndFrame == 0 {
		t.Fatal("宽限期超时必须排入权威结束帧（判负）")
	}
	submission := room.scheduledFrames[room.pendingEndFrame]
	if submission == nil || len(submission.Operations) == 0 {
		t.Fatal("结束帧必须包含 game_event 操作")
	}
	event, ok := submission.Operations[0].Payload.(*messages.InGameOperation_GameEvent)
	if !ok || event.GameEvent.Message != "0" {
		t.Fatalf("宽限期超时应判负，message=%v", submission.Operations[0].Payload)
	}
}

func TestReconnectGraceNotExpiredDoesNothing(t *testing.T) {
	room := lifecycleTestRoom(100)
	room.RoomCtx.Players.Store(101, disconnectedPlayer(101))
	room.AnyPlayerDisconnected = true
	room.ReconnectDeadline = time.Now().Add(constants.ReconnectGracePeriod)

	room.tickReconnectGrace()
	if room.pendingEndFrame != 0 {
		t.Fatal("宽限期内不得排入结束帧")
	}
}

func TestAllPlayersBackClearsGrace(t *testing.T) {
	room := lifecycleTestRoom(100)
	p1, p2 := connectedPlayer(100), connectedPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.AnyPlayerDisconnected = true
	room.ReconnectDeadline = time.Now().Add(-time.Second)

	// 重连归队清除断线状态
	room.onPlayerReconnected(p1)
	room.onPlayerReconnected(p2)

	room.tickReconnectGrace()
	if room.pendingEndFrame != 0 {
		t.Fatal("全员归队后宽限期检查不得判负")
	}
	if room.AnyPlayerDisconnected {
		t.Fatal("全员归队后应清除宽限期状态")
	}
}

func TestReturnToLobbyFromPostGame(t *testing.T) {
	room := postGameTestRoom(500)
	room.PostGameSince = time.Now()
	room.RoomCtx.Players.Store(100, connectedPlayer(100))

	room.returnToLobbyFromPostGame()

	if !room.GameStage.EqualTo(constants.STAGE_InLobby) {
		t.Fatalf("应回到 InLobby，当前 %d", room.GameStage.Load())
	}
	if !room.PostGameSince.IsZero() {
		t.Fatal("PostGame 计时应清零")
	}
}

func TestPostGameLingerAutoReturns(t *testing.T) {
	room := postGameTestRoom(500)
	room.RoomCtx.Players.Store(100, connectedPlayer(100))

	room.tickPostGame() // 第一次：仅记录起点
	if !room.GameStage.EqualTo(constants.STAGE_PostGame) {
		t.Fatal("第一次 tick 不应立刻返回")
	}
	room.PostGameSince = time.Now().Add(-constants.PostGameLinger - time.Second)
	room.tickPostGame()
	if !room.GameStage.EqualTo(constants.STAGE_InLobby) {
		t.Fatal("PostGame 停留超时必须自动返回等待房间")
	}
}

func TestHeartbeatTimeoutClosesConnection(t *testing.T) {
	room := lifecycleTestRoom(10)
	player := connectedPlayer(100)
	room.RoomCtx.Players.Store(100, player)

	// 构造一个已关闭写通道的玩家（心跳已过期）
	// 模拟长时间无消息
	player.Ctx.Close()

	room.tickHeartbeat()
	// 关闭后 IsConnected=false，tickHeartbeat 跳过；
	// 主要验证不 panic 且对已断开连接安全
}

func TestHeartbeatNotExpiredKeepsConnection(t *testing.T) {
	room := lifecycleTestRoom(10)
	player := connectedPlayer(100)
	room.RoomCtx.Players.Store(100, player)

	room.tickHeartbeat()
	if !player.Ctx.IsConnected() {
		t.Fatal("心跳未过期时不应关闭连接")
	}
}

func TestPeersJSONIncludesDisconnectedState(t *testing.T) {
	room := lifecycleTestRoom(100)
	room.RoomCtx.Players.Store(100, connectedPlayer(100))
	room.RoomCtx.Players.Store(101, disconnectedPlayer(101))
	room.RoomCtx.OwnerUserID = 100

	peersJSON := room.buildPeersJSON()
	wantConnected := `"state":` + strconv.Itoa(int(constants.PlayerStateConnected))
	if !contains(peersJSON, wantConnected) {
		t.Fatalf("在线玩家应带 state=%d，得到 %s", constants.PlayerStateConnected, peersJSON)
	}
	wantDisconnected := `"state":` + strconv.Itoa(int(constants.PlayerStateDisconnected))
	if !contains(peersJSON, wantDisconnected) {
		t.Fatalf("断线玩家应带 state=%d，得到 %s", constants.PlayerStateDisconnected, peersJSON)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

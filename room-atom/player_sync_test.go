package roomatom

// 加载进度同步与房间销毁的单元测试：
// playerLoaded 单调收敛、roomSync 快照、全员完成 → roomAllSync/InGame、
// 最后一个玩家离开 → 房间立即销毁并从 manager 移除。

import (
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"testing"
)

func loadingTestRoom(id int) (*Room, chan int) {
	stopChan := make(chan int, 4)
	room := NewRoom(id, stopChan)
	room.GameStage.Store(constants.STAGE_Loading)
	return room, stopChan
}

func TestPlayerLoadedProgressIsMonotonicAndClamped(t *testing.T) {
	room, _ := loadingTestRoom(10)
	player := connectedPlayer(100)
	player.Name = "host"
	room.RoomCtx.Players.Store(100, player)

	room.HandleRequestPlayerLoaded(player, &messages.RequestPlayerLoaded{Progress: 40})
	if player.LoadProgress != 40 {
		t.Fatalf("进度应更新为 40，实际 %d", player.LoadProgress)
	}

	room.HandleRequestPlayerLoaded(player, &messages.RequestPlayerLoaded{Progress: 20})
	if player.LoadProgress != 40 {
		t.Fatalf("乱序的低进度不得回退，实际 %d", player.LoadProgress)
	}

	room.HandleRequestPlayerLoaded(player, &messages.RequestPlayerLoaded{Progress: 999})
	if player.LoadProgress != 100 || !player.IsLoaded {
		t.Fatalf("进度应收敛到 100 并标记 loaded，实际 %d/%v", player.LoadProgress, player.IsLoaded)
	}
}

func TestPlayerLoadedOnlyAcceptedInLoadingStage(t *testing.T) {
	room, _ := loadingTestRoom(11)
	room.GameStage.Store(constants.STAGE_InLobby)
	player := connectedPlayer(100)
	room.RoomCtx.Players.Store(100, player)

	room.HandleRequestPlayerLoaded(player, &messages.RequestPlayerLoaded{Progress: 100})
	if player.LoadProgress != 0 || player.IsLoaded {
		t.Fatal("非 Loading 阶段的进度上报必须被忽略")
	}
}

func TestBuildPlayerSyncProgressSortedWithLoadedFlag(t *testing.T) {
	room, _ := loadingTestRoom(12)
	p2 := connectedPlayer(101)
	p2.Name = "guest"
	p2.LoadProgress = 100
	p1 := connectedPlayer(100)
	p1.Name = "host"
	p1.LoadProgress = 45
	room.RoomCtx.Players.Store(101, p2)
	room.RoomCtx.Players.Store(100, p1)

	players := room.buildPlayerSyncProgress()
	if len(players) != 2 {
		t.Fatalf("应有 2 位玩家，实际 %d", len(players))
	}
	if players[0].PlayerId != 100 || players[0].Progress != 45 || players[0].Loaded {
		t.Fatalf("host 快照错误: %+v", players[0])
	}
	if players[1].PlayerId != 101 || players[1].Progress != 100 || !players[1].Loaded {
		t.Fatalf("guest 快照错误: %+v", players[1])
	}
}

func TestAllPlayersLoadedEntersInGameAndStartsTicker(t *testing.T) {
	room, _ := loadingTestRoom(13)
	p1 := connectedPlayer(100)
	p2 := connectedPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.HandleRequestPlayerLoaded(p1, &messages.RequestPlayerLoaded{Progress: 100})
	if !room.GameStage.EqualTo(constants.STAGE_Loading) {
		t.Fatal("仅一人加载完成时不应开局")
	}

	room.HandleRequestPlayerLoaded(p2, &messages.RequestPlayerLoaded{Progress: 100})
	if !room.GameStage.EqualTo(constants.STAGE_InGame) {
		t.Fatalf("全员加载完成必须进入 InGame，当前 %d", room.GameStage.Load())
	}
	if room.RoomCtx.GameTicker == nil {
		t.Fatal("进入 InGame 必须启动逻辑帧定时器")
	}
	room.RoomCtx.GameTicker.Stop()
}

func TestTickRoomSyncFallbackStartsGame(t *testing.T) {
	room, _ := loadingTestRoom(14)
	p1 := connectedPlayer(100)
	p1.LoadProgress, p1.IsLoaded = 100, true
	p2 := connectedPlayer(101)
	p2.LoadProgress, p2.IsLoaded = 100, true
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.tickRoomSync()
	if !room.GameStage.EqualTo(constants.STAGE_InGame) {
		t.Fatal("秒级同步兜底应把全员完成的房间推进到 InGame")
	}
	room.RoomCtx.GameTicker.Stop()
}

func TestLastPlayerLeavingDestroysRoomImmediately(t *testing.T) {
	stopChan := make(chan int, 1)
	room := NewRoom(15, stopChan)
	player := connectedPlayer(100)
	room.RoomCtx.Players.Store(100, player)

	room.handleUnregister(player)

	if !room.GameStage.EqualTo(constants.STAGE_CLOSED) {
		t.Fatalf("最后一人离开后房间应关闭，当前 %d", room.GameStage.Load())
	}
	select {
	case id := <-stopChan:
		if id != 15 {
			t.Fatalf("管理器应收到房间 15 的移除信号，实际 %d", id)
		}
	default:
		t.Fatal("房间销毁必须通知 manager 移除引用")
	}
}

func TestUserMembershipAndPermanentLeave(t *testing.T) {
	stopChan := make(chan int, 1)
	room := NewRoom(16, stopChan)
	room.Name = "test"
	room.ChapterID, room.StageID = 1, 3
	player := connectedPlayer(100)
	player.Name = "alice"
	player.JoinKey = "abc"
	room.RoomCtx.Players.Store(100, player)

	membership := room.FindMembershipByName("alice")
	if membership == nil || membership.RoomID != 16 || membership.MyID != 100 || membership.Key != "abc" {
		t.Fatalf("席位查询错误: %+v", membership)
	}
	if membership.StageID != 3 || membership.RoomName != "test" {
		t.Fatalf("房间绑定信息错误: %+v", membership)
	}
	if room.FindMembershipByName("bob") != nil {
		t.Fatal("不存在的昵称不应查到席位")
	}

	reply := make(chan bool, 1)
	room.handleLeaveUser(&LeaveUserRequest{UserName: "alice", Reply: reply})
	if !<-reply {
		t.Fatal("永久退出应成功")
	}
	if room.GetPlayerCount() != 0 {
		t.Fatal("退出后房间人数应为 0")
	}
	if !room.GameStage.EqualTo(constants.STAGE_CLOSED) {
		t.Fatal("最后一人退出后房间应销毁")
	}
}

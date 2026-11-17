package roommanager

// RoomManager 单元测试：原子创建、房间列表筛选与清理。

import (
	"mvzserver/constants"
	"sync"
	"testing"
	"time"
)

func TestAddRoomConcurrentSameIDReturnsSameRoom(t *testing.T) {
	rm := &RoomManager{rooms: RoomMap{}, destroyChan: make(chan int, 16)}
	const goroutines = 16
	results := make([]*Room, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = rm.AddRoom(7, "key", "room", 1, 2)
		}(i)
	}
	wg.Wait()

	first := results[0]
	if first == nil {
		t.Fatal("应返回有效房间")
	}
	for i, room := range results {
		if room != first {
			t.Fatalf("并发同 id 创建应返回同一房间（第 %d 个不同）", i)
		}
	}
	if rm.GetRoom(7) != first {
		t.Fatal("管理器中应保存该房间")
	}
}

func TestGetRoomsFiltersByStageAndEmpty(t *testing.T) {
	rm := &RoomManager{rooms: RoomMap{}, destroyChan: make(chan int, 16)}
	r1 := rm.AddRoom(0, "", "alpha", 1, 10)
	r2 := rm.AddRoom(1, "", "beta", 1, 20)
	r1.SetName("alpha")
	r2.SetName("beta")

	// 无玩家房间不下发
	if got := rm.GetRooms(-1, -1); len(got) != 0 {
		t.Fatalf("0 人房间不应下发，得到 %d 条", len(got))
	}

	// 加入玩家（直接操作 RoomCtx）
	r1.RoomCtx.Players.Store(100, nil)
	r2.RoomCtx.Players.Store(100, nil)
	rooms := rm.GetRooms(-1, -1)
	if len(rooms) != 2 {
		t.Fatalf("应返回 2 个房间，得到 %d", len(rooms))
	}
	if rooms[0].RoomID != 0 || rooms[1].RoomID != 1 {
		t.Fatalf("房间列表应按 id 排序，得到 %+v", rooms)
	}

	stageRooms := rm.GetRooms(1, 10)
	if len(stageRooms) != 1 || stageRooms[0].RoomID != 0 {
		t.Fatalf("关卡筛选错误，得到 %+v", stageRooms)
	}
}

func TestCleanIdleLobbyRoom(t *testing.T) {
	destroyChan := make(chan int, 16)
	rm := &RoomManager{rooms: RoomMap{}, destroyChan: destroyChan}
	room := rm.AddRoom(3, "", "idle", 0, 0)
	room.RoomCtx.Players.Store(100, nil)
	room.LastActiveTime = time.Now().Add(-constants.RoomIdleTimeout - time.Second)

	rm.Clean()
	if room.GameStage.Load() != constants.STAGE_CLOSED {
		t.Fatal("超时空闲房间应被 Destroy（标记为 CLOSED）")
	}
	select {
	case id := <-destroyChan:
		if id != 3 {
			t.Fatalf("销毁信号房间号错误: %d", id)
		}
	default:
		t.Fatal("应发出销毁信号")
	}
}

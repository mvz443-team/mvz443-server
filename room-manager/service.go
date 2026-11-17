package roommanager

import (
	"log"
	"mvzserver/constants"
	roomatom "mvzserver/room-atom"
	"mvzserver/types"
	"sort"
	"time"
)

type Room = roomatom.Room

// RoomManager 管理所有房间
type RoomManager struct {
	rooms RoomMap // key 为 int 类型表示房间ID，value 为 *Room

	destroyChan chan int // 用于接收销毁房间的请求
}

func NewRoomManager() *RoomManager {
	rm := &RoomManager{
		destroyChan: make(chan int, 16), // TODO: magic number, 可根据实际情况调整
	}
	go rm.startCleaner()
	return rm
}

// GetRoom 根据 id 获取房间
func (rm *RoomManager) GetRoom(id int) *Room {
	if r, ok := rm.rooms.Load(id); ok {
		return r
	}
	return nil
}

// GetRooms 返回房间列表。
// chapterID / stageID 传负数表示不按关卡筛选；0 人房间一律不返回。
func (rm *RoomManager) GetRooms(chapterID, stageID int) []types.RoomsInfo {
	var rooms []types.RoomsInfo
	rm.rooms.Range(func(id int, room *Room) bool {
		playerCount := int(room.GetPlayerCount())
		if playerCount <= 0 {
			return true
		}
		roomChapter, roomStage := room.GetBoundStage()
		if chapterID >= 0 && int(roomChapter) != chapterID {
			return true
		}
		if stageID >= 0 && int(roomStage) != stageID {
			return true
		}
		rooms = append(rooms, types.RoomsInfo{
			RoomID:      id,
			RoomName:    room.GetName(),
			NeedKey:     room.HasKey(),
			PlayerCount: playerCount,
			GameState:   room.GameStage.Load(),
			ChapterID:   roomChapter,
			StageID:     roomStage,
		})
		return true
	})
	sort.Slice(rooms, func(i, j int) bool { return rooms[i].RoomID < rooms[j].RoomID })
	return rooms
}

// GetNewRoomId 寻找一个未使用的房间id
func (rm *RoomManager) GetNewRoomId() int {
	id := 0
	for {
		if _, ok := rm.rooms.Load(id); !ok {
			return id
		}
		id++
	}
}

// AddRoom 创建一个新的房间并添加到管理器中
// 房间名、密码与绑定关卡在创建时确定
func (rm *RoomManager) AddRoom(id int, key, name string, chapterID, stageID uint32) *Room {
	room := roomatom.NewRoom(id, rm.destroyChan)
	room.SetKey(key)
	room.SetName(name)
	room.SetBoundStage(chapterID, stageID)
	rm.rooms.Store(id, room)
	return room
}

// RemoveRoom 根据 id 移除房间引用
func (rm *RoomManager) RemoveRoom(id int) {
	rm.rooms.Delete(id)
}

// DestroyRoom 摧毁指定id房间
func (rm *RoomManager) DestroyRoom(id int) {
	if r, ok := rm.rooms.Load(id); ok {
		r.Destroy()
	}
}

// 定时清理无人房间
// 包括监听销毁信号
func (rm *RoomManager) startCleaner() {
	ticker := time.NewTicker(1 * time.Minute) // 每分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 开启一次清理
			rm.Clean()
		case id := <-rm.destroyChan:
			// 删除引用
			rm.RemoveRoom(id)
		}
	}
}

// Clean 遍历所有房间，清理人数为 0 的房间
func (rm *RoomManager) Clean() {
	rm.rooms.Range(func(id int, room *Room) bool {
		// 没有人的房间必须移除
		if room.GetPlayerCount() == 0 {
			log.Printf("🧹 Cleaning empty room %d (0 players)", id)
			rm.DestroyRoom(id)
			return true
		}

		// 检查是否超过最大空闲时间
		// 每次用户进行互动都会更新LastActiveTime
		idleTime := time.Since(room.LastActiveTime)
		if idleTime > 5*time.Minute {
			log.Printf("🧹 Cleaning idle room %d (idle for %v, %d players)", id, idleTime, room.GetPlayerCount())
			rm.DestroyRoom(id)
			return true
		}

		// 如果房间是closed状态，那么直接摧毁
		if room.GameStage.IsLaterThanOrEqual(constants.STAGE_CLOSED) {
			log.Printf("🧹 Cleaning closed room %d (stage: %d)", id, room.GameStage.Load())
			rm.DestroyRoom(id)
			return true
		}

		return true
	})
}

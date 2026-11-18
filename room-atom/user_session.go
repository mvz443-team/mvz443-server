package roomatom

// 用户维度的房间会话：按昵称查询自己所在房间、持久退出房间、
// 以及非 InGame 阶段的重连入座。HTTP 层只做查询/投递，状态变更都在 Run 主循环完成。

import (
	"log"
	"mvzserver/clients"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
)

// UserRoomMembership 是 /user-room 返回给客户端的最小会话信息。
type UserRoomMembership struct {
	RoomID    int    `json:"room_id"`
	MyID      int    `json:"my_id"`
	Key       string `json:"key"` // 玩家入座时使用的密钥，重连时用它校验
	RoomName  string `json:"room_name"`
	ChapterID uint32 `json:"chapter_id"`
	StageID   uint32 `json:"stage_id"`
	Stage     int    `json:"stage"`
}

// LeaveUserRequest 由 HTTP goroutine 投递给房间主循环。
type LeaveUserRequest struct {
	UserName string
	Reply    chan bool
}

// FindMembershipByName 查询昵称对应的玩家席位。
// Name/JoinKey 在入座后不再修改，PlayerMap 的 Range 并发安全，因此 HTTP 层可直接读取。
func (room *Room) FindMembershipByName(name string) *UserRoomMembership {
	if room == nil || name == "" {
		return nil
	}
	stage := room.GameStage.Load()
	if stage >= constants.STAGE_CLOSED {
		return nil
	}
	var found *UserRoomMembership
	room.RoomCtx.Players.Range(func(_ int, player *clients.Player) bool {
		if player == nil || player.Ctx == nil || player.Name != name {
			return true
		}
		// InGame 掉线保留席位的玩家也应该能被查到并恢复。
		found = &UserRoomMembership{
			RoomID:    room.ID,
			MyID:      player.Ctx.Id,
			Key:       player.JoinKey,
			RoomName:  room.Name,
			ChapterID: room.ChapterID,
			StageID:   room.StageID,
			Stage:     int(stage),
		}
		return false
	})
	return found
}

// RequestLeaveUserByName 请求把指定昵称的玩家从房间里彻底移除。
func (room *Room) RequestLeaveUserByName(name string) bool {
	if room == nil || name == "" {
		return false
	}
	reply := make(chan bool, 1)
	select {
	case room.leaveUsers <- &LeaveUserRequest{UserName: name, Reply: reply}:
	case <-time.After(3 * time.Second):
		return false
	}
	select {
	case ok := <-reply:
		return ok
	case <-time.After(3 * time.Second):
		return false
	}
}

// handleLeaveUser 在房间主循环里移除玩家席位；空房间随即销毁。
func (room *Room) handleLeaveUser(request *LeaveUserRequest) {
	if request == nil {
		return
	}
	ok := false
	var leavingID int
	room.RoomCtx.Players.Range(func(id int, player *clients.Player) bool {
		if player != nil && player.Name == request.UserName {
			leavingID = id
			ok = true
			return false
		}
		return true
	})
	if !ok {
		replyLeave(request, false)
		return
	}

	if target, ok := room.RoomCtx.Players.Load(leavingID); ok {
		room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
			Payload: &messages.RoomResponse_RoomClosed{
				RoomClosed: &messages.ResponseRoomClosed{Message: "你已从房间中退出"},
			},
		}, target)
		if target.Ctx != nil {
			target.Ctx.Close()
		}
	}
	room.RoomCtx.DelUser(leavingID)
	room.transferOwnerIfNeeded(leavingID)
	log.Printf("🚪 玩家 %s 从房间 %d 永久退出", request.UserName, room.ID)

	if room.GetPlayerCount() == 0 {
		room.Destroy()
	} else {
		room.broadcastRoomInfo()
		room.broadcastReadyCount()
	}
	replyLeave(request, true)
}

func replyLeave(request *LeaveUserRequest, ok bool) {
	if request.Reply != nil {
		request.Reply <- ok
	}
}

// allReadyPlayerIDs 返回排序后的玩家 ID 列表，供 AllReady / 重连复用。
func (room *Room) allReadyPlayerIDs() []uint32 {
	ids := make([]uint32, 0, 2)
	room.RoomCtx.Players.Range(func(_ int, player *clients.Player) bool {
		if player != nil && player.Ctx != nil {
			ids = append(ids, uint32(player.Ctx.Id))
		}
		return true
	})
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// sendAllReadyTo 把当前开局配置单播给指定玩家（AllReady 与 Loading 重连共用）。
func (room *Room) sendAllReadyTo(player *clients.Player) {
	if player == nil || player.Ctx == nil {
		return
	}
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_AllReady{
			AllReady: &messages.ResponseAllReady{
				AllPlayerCount: room.GetPlayerCount(),
				Seed:           room.Seed,
				MyId:           uint32(player.Ctx.Id),
				PlayerIds:      room.allReadyPlayerIDs(),
				Loadouts:       room.canonicalPlayerLoadouts(),
			},
		},
	}, player)
}

// sendRoomAllSyncTo 单播给重连玩家：让它的 LoadScene 直接进入 MainGame。
func (room *Room) sendRoomAllSyncTo(player *clients.Player) {
	if player == nil || player.Ctx == nil {
		return
	}
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_RoomAllSync{
			RoomAllSync: &messages.ResponseRoomAllSync{},
		},
	}, player)
}

// rebindPlayerInLobby 在非 InGame 阶段把新连接绑定到已有的玩家席位。
// 返回 false 表示不能在该阶段重连（例如房间已关闭）。
func (room *Room) rebindPlayerInLobby(player *clients.Player, conn *ClientCtx) bool {
	if room == nil || player == nil || conn == nil {
		return false
	}
	stage := room.GameStage.Load()
	if stage >= constants.STAGE_CLOSED {
		return false
	}

	oldCtx := player.Ctx
	player.Ctx = conn
	player.ConnectionEpoch++
	player.IsReady = false
	player.IsLoaded = false
	player.LoadProgress = 0
	if oldCtx != nil && oldCtx.IsConnected() {
		oldCtx.Close()
	}

	if data, err := proto.Marshal(&messages.LobbyResponse{
		Payload: &messages.LobbyResponse_JoinRoomSuccess{
			JoinRoomSuccess: &messages.ResponseJoinRoomSuccess{
				RoomId:  uint32(room.ID),
				MyId:    uint32(conn.Id),
				Key:     room.key,
				Message: "重连房间成功",
			},
		},
	}); err == nil {
		conn.Send(data)
	}

	switch {
	case stage == constants.STAGE_Preparing:
		room.sendChooseMapTo(player)
	case stage == constants.STAGE_Loading:
		room.sendAllReadyTo(player)
	}
	room.broadcastRoomInfo()
	room.broadcastReadyCount()
	log.Printf("🟢 玩家 %d 重连回房间 %d（阶段=%d）", conn.Id, room.ID, stage)
	return true
}

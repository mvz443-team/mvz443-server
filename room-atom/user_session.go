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
	Identity    LeaveIdentity
	// DestroyRoom 表示玩家主动退出对局：除了移除本人席位，还要取消整个房间，
	// 其他人会看到本玩家离线并收到 roomClosed，房间不可再重连。
	DestroyRoom bool
	Reply       chan bool
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

// LeaveIdentity 指定要移除的席位：按昵称，或按房间号+玩家 ID+密钥。
type LeaveIdentity struct {
	UserName string
	RoomID   int
}

// RequestLeaveUser 请求把指定玩家从房间里彻底移除。
// destroyRoom=true 时同时取消整个房间（对局中主动退出：对方看到离线且无法重连）。
func (room *Room) RequestLeaveUser(identity LeaveIdentity, destroyRoom bool) bool {
	if room == nil {
		return false
	}
	// 指定房间号时必须匹配，避免误删其他房间的同名玩家。
	if identity.RoomID >= 0 && room.ID != identity.RoomID {
		return false
	}
	if identity.UserName == "" {
		return false
	}
	reply := make(chan bool, 1)
	select {
	case room.leaveUsers <- &LeaveUserRequest{Identity: identity, DestroyRoom: destroyRoom, Reply: reply}:
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

// handleLeaveUser 在房间主循环里移除玩家席位；destroyRoom 时取消整个房间，否则空房间随即销毁。
func (room *Room) handleLeaveUser(request *LeaveUserRequest) {
	if request == nil {
		return
	}
	// 先按昵称定位席位并保存引用：DelUser 之后 Players 不再含有该 id，
	// 若删除后再 Load 会拿不到 target，导致退出方的连接永远无法关闭。
	ok := false
	var leavingID int
	var target *clients.Player
	room.RoomCtx.Players.Range(func(id int, player *clients.Player) bool {
		if player == nil {
			return true
		}
		identity := request.Identity
		if player.Name == identity.UserName && identity.UserName != "" {
			leavingID = id
			target = player
			ok = true
			return false
		}
		return true
	})
	if !ok {
		replyLeave(request, false)
		return
	}

	// 主动退出时对退出方单播 roomClosed，不能先把它标成断线再广播：
	// 那会让对方以为触发了“玩家掉线重连”，在房间取消后仍尝试重连。
	// 必须在 DelUser 之前发送，否则退出方已经不在 Players 中，收不到单播。
	leaveMessage := &messages.RoomResponse{
		Payload: &messages.RoomResponse_RoomClosed{
			RoomClosed: &messages.ResponseRoomClosed{Message: "你已退出游戏"},
		},
	}
	room.RoomCtx.SendMessageToUserByPlayer(leaveMessage, target)
	room.RoomCtx.DelUser(leavingID)
	room.transferOwnerIfNeeded(leavingID)
	log.Printf("🚪 玩家 %d (%s) 从房间 %d 永久退出", leavingID, request.Identity.UserName, room.ID)

	// 关闭退出方的底层连接：DelUser 只从集合移除，不会断开 socket。
	// 若不关闭，读循环仍阻塞，服务端会一直认为该连接活着。
	if target.Ctx != nil {
		target.Ctx.ForceClose()
	}

	// 对局中主动退出：取消整个房间，对方收到 roomClosed 且不能再重连。
	// 普通“放弃重连”不改变此行为：只在房间空了时销毁。
	if request.DestroyRoom {
		room.Destroy()
		replyLeave(request, true)
		return
	}

	if room.GetPlayerCount() == 0 {
		room.Destroy()
	} else {
		// 等待房间/选卡阶段有人永久退出：拉回等待房间并清空所有人的准备状态。
		stage := room.GameStage.Load()
		if stage == constants.STAGE_InLobby || stage == constants.STAGE_Preparing {
			room.resetWaitingRoom(stage == constants.STAGE_Preparing)
		} else {
			room.broadcastRoomInfo()
			room.broadcastReadyCount()
		}
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
				RoomId:         uint32(room.ID),
				ChapterId:      room.ChapterID,
				StageId:        room.StageID,
				Resources:      room.captureResourceProfiles(),
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

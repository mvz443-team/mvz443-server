package roomatom

// 房间信息（等待房间）相关的属性读取与广播。
// 所有方法都必须在房间主循环（Run 的 goroutine）中调用，避免并发修改玩家状态。

import (
	"encoding/json"
	"log"
	"mvzserver/clients"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"sort"
)

// peers JSON 里的地址字段目前仍是占位符，避免把玩家真实 IP 下发给对手。
const peerAddrPlaceholder = "111.111.111.111"

type peerInfo struct {
	Addr  string `json:"addr"`
	Id    int    `json:"id"`
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
	// 1=在线, 2=断线（等待重连）；见 constants.PlayerState
	State int `json:"state"`
	// Seat 房间内固定座位号，0 起；客户端按座位渲染，而不是数组下标。
	Seat int `json:"seat"`
}

func (room *Room) GetName() string {
	return room.Name
}

func (room *Room) SetName(name string) {
	room.Name = name
}

// GetBoundStage 返回房间绑定的章节与关卡。
func (room *Room) GetBoundStage() (uint32, uint32) {
	return room.ChapterID, room.StageID
}

// SetBoundStage 设置房间绑定的章节与关卡；只在创建房间时写入。
func (room *Room) SetBoundStage(chapterID, stageID uint32) {
	room.ChapterID = chapterID
	room.StageID = stageID
}

func (room *Room) buildPeersJSON() string {
	peers := make([]peerInfo, 0, 2)
	room.RoomCtx.Players.Range(func(_ int, player *clients.Player) bool {
		if player == nil || player.Ctx == nil {
			return true
		}
		// 掉线/离线席位（state=2）也要下发，让对手看到断线提示：
		// InGame 由重连宽限保留席位；其他阶段由主动退出在销毁前短暂标记。
		if player.Ctx.State == constants.PlayerStateDisconnected {
			peers = append(peers, peerInfo{
				Addr:  peerAddrPlaceholder,
				Id:    player.Ctx.Id,
				Name:  player.Name,
				Ready: player.IsReady,
				State: int(constants.PlayerStateDisconnected),
				Seat:  player.SeatIndex,
			})
			return true
		}
		if !player.Ctx.IsConnected() {
			return true
		}
		peers = append(peers, peerInfo{
			Addr:  peerAddrPlaceholder,
			Id:    player.Ctx.Id,
			Name:  player.Name,
			Ready: player.IsReady,
			State: int(constants.PlayerStateConnected),
			Seat:  player.SeatIndex,
		})
		return true
	})
	// 按座位排序，保证 peers 数组顺序稳定；客户端也用 seat 字段定位格子。
	sort.Slice(peers, func(i, j int) bool { return peers[i].Seat < peers[j].Seat })
	data, err := json.Marshal(peers)
	if err != nil {
		log.Printf("buildPeersJSON failed: %v", err)
		return "[]"
	}
	return string(data)
}

func (room *Room) buildRoomInfoPayload(myID int, peersJSON string) *messages.ResponseRoomInfo {
	return &messages.ResponseRoomInfo{
		Peers:     peersJSON,
		RoomId:    uint32(room.ID),
		LordId:    uint32(room.RoomCtx.OwnerUserID),
		MyId:      uint32(myID),
		RoomName:  room.Name,
		ChapterId: room.ChapterID,
		StageId:   room.StageID,
	}
}

func (room *Room) sendRoomInfoTo(player *clients.Player, peersJSON string) {
	if player == nil || player.Ctx == nil {
		return
	}
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_RoomInfo{
			RoomInfo: room.buildRoomInfoPayload(player.Ctx.Id, peersJSON),
		},
	}, player)
}

// broadcastRoomInfo 把房间名、房主、人数、玩家昵称与准备状态同步给所有玩家。
func (room *Room) broadcastRoomInfo() {
	peersJSON := room.buildPeersJSON()
	room.RoomCtx.Players.Range(func(_ int, player *clients.Player) bool {
		room.sendRoomInfoTo(player, peersJSON)
		return true
	})
}

func (room *Room) broadcastReadyCount() {
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_UpdateReadyCount{
			UpdateReadyCount: &messages.ResponseUpdateReadyCount{
				Count:          room.PlayerReadyCount(),
				AllPlayerCount: room.GetPlayerCount(),
			},
		},
	}, nil)
}

// buildPlayerSyncProgress 构造每位玩家的加载进度快照（Loading 阶段使用）。
func (room *Room) buildPlayerSyncProgress() []*messages.PlayerSyncProgress {
	players := make([]*messages.PlayerSyncProgress, 0, 2)
	room.RoomCtx.Players.Range(func(_ int, player *clients.Player) bool {
		if player == nil || player.Ctx == nil {
			return true
		}
		progress := player.LoadProgress
		players = append(players, &messages.PlayerSyncProgress{
			PlayerId: uint32(player.Ctx.Id),
			Progress: progress,
			Name:     player.Name,
			Loaded:   progress >= 100,
		})
		return true
	})
	sort.Slice(players, func(i, j int) bool { return players[i].PlayerId < players[j].PlayerId })
	return players
}

// broadcastRoomSync 每秒把各玩家加载进度下发给房间内所有人。
func (room *Room) broadcastRoomSync() {
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_RoomSync{
			RoomSync: &messages.ResponseRoomSync{Players: room.buildPlayerSyncProgress()},
		},
	}, nil)
}

// broadcastRoomAllSync 全员加载完成后广播，客户端据此瞬间切到 MainGame。
func (room *Room) broadcastRoomAllSync() {
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_RoomAllSync{
			RoomAllSync: &messages.ResponseRoomAllSync{},
		},
	}, nil)
}

func (room *Room) broadcastChooseMap() {
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_ChooseMap{
			ChooseMap: &messages.ResponseChooseMap{
				ChapterId: room.ChapterID,
				StageId:   room.StageID,
			},
		},
	}, nil)
}

func (room *Room) sendChooseMapTo(player *clients.Player) {
	room.RoomCtx.SendMessageToUserByPlayer(&messages.RoomResponse{
		Payload: &messages.RoomResponse_ChooseMap{
			ChooseMap: &messages.ResponseChooseMap{
				ChapterId: room.ChapterID,
				StageId:   room.StageID,
			},
		},
	}, player)
}

func (room *Room) resetReadyFlags() {
	room.RoomCtx.Players.Range(func(_ int, player *clients.Player) bool {
		if player != nil {
			player.IsReady = false
			player.IsLoaded = false
			player.LoadProgress = 0
		}
		return true
	})
}

// startPreparing 等待房间内全员准备后进入选卡阶段。
func (room *Room) startPreparing() {
	room.GameStage.Store(constants.STAGE_Preparing)
	// 选卡阶段还要再确认一次，因此这里清空准备标记
	room.resetReadyFlags()
	room.broadcastRoomInfo()
	room.broadcastChooseMap()
	room.broadcastReadyCount()
	log.Printf("🟢 Room %d enter Preparing (chapter=%d stage=%d)", room.ID, room.ChapterID, room.StageID)
}

// returnToLobby 从选卡阶段退回等待房间（房主返回或有人离开）。
func (room *Room) returnToLobby() {
	room.resetWaitingRoom(true)
}

// resetWaitingRoom 把房间拉回等待房间：阶段归位、清空所有人准备状态并广播。
// emitQuitChooseMap 仅在从选卡阶段退回时为真；等待房间内有人离开不需要该事件。
func (room *Room) resetWaitingRoom(emitQuitChooseMap bool) {
	room.GameStage.Store(constants.STAGE_InLobby)
	room.resetReadyFlags()
	if emitQuitChooseMap {
		room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
			Payload: &messages.RoomResponse_QuitChooseMap{
				QuitChooseMap: &messages.ResponseQuitChooseMap{},
			},
		}, nil)
	}
	room.broadcastRoomInfo()
	room.broadcastReadyCount()
}

// transferOwnerIfNeeded 房主离开后把房主身份交给座位号最小的剩余玩家。
func (room *Room) transferOwnerIfNeeded(leavingID int) {
	if room.RoomCtx.OwnerUserID != leavingID {
		return
	}
	room.RoomCtx.OwnerUserID = 0
	bestSeat := constants.RoomCapacity
	room.RoomCtx.Players.Range(func(_ int, player *clients.Player) bool {
		if player == nil || player.Ctx == nil || player.Ctx.Id == leavingID {
			return true
		}
		seat := player.SeatIndex
		if seat < 0 {
			seat = constants.RoomCapacity
		}
		if room.RoomCtx.OwnerUserID == 0 || seat < bestSeat {
			room.RoomCtx.OwnerUserID = player.Ctx.Id
			bestSeat = seat
		}
		return true
	})
}

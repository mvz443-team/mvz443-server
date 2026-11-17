package roomatom

// 房间信息（等待房间）相关的属性读取与广播。
// 所有方法都必须在房间主循环（Run 的 goroutine）中调用，避免并发修改玩家状态。

import (
	"encoding/json"
	"log"
	"mvzserver/clients"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
)

// peers JSON 里的地址字段目前仍是占位符，避免把玩家真实 IP 下发给对手。
const peerAddrPlaceholder = "111.111.111.111"

type peerInfo struct {
	Addr  string `json:"addr"`
	Id    int    `json:"id"`
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
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
		if player == nil || player.Ctx == nil || !player.Ctx.IsConnected() {
			return true
		}
		peers = append(peers, peerInfo{
			Addr:  peerAddrPlaceholder,
			Id:    player.Ctx.Id,
			Name:  player.Name,
			Ready: player.IsReady,
		})
		return true
	})
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
	room.GameStage.Store(constants.STAGE_InLobby)
	room.resetReadyFlags()
	room.RoomCtx.BroadcastMessage(&messages.RoomResponse{
		Payload: &messages.RoomResponse_QuitChooseMap{
			QuitChooseMap: &messages.ResponseQuitChooseMap{},
		},
	}, nil)
	room.broadcastRoomInfo()
	room.broadcastReadyCount()
}

// transferOwnerIfNeeded 房主离开后把房主身份交给房间里剩下的玩家。
func (room *Room) transferOwnerIfNeeded(leavingID int) {
	if room.RoomCtx.OwnerUserID != leavingID {
		return
	}
	room.RoomCtx.OwnerUserID = 0
	room.RoomCtx.Players.Range(func(_ int, player *clients.Player) bool {
		if player != nil && player.Ctx != nil && player.Ctx.Id != leavingID {
			room.RoomCtx.OwnerUserID = player.Ctx.Id
			return false
		}
		return true
	})
}

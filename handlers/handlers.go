package handlers

import (
	"log"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	roommanager "mvzserver/room-manager"
	"mvzserver/types"
	"mvzserver/utils"
	"runtime/debug"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
)

// 处理器结构体
// 管理依赖
type ServerHandler struct {
	RoomMnager *roommanager.RoomManager
}

func NewServerHandler(roomMnager *roommanager.RoomManager) *ServerHandler {
	return &ServerHandler{RoomMnager: roomMnager}
}

// 解析可选的整数查询参数；缺省或非法时返回 fallback
func parseIntOr(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

// HandleListRoom 房间列表。
// 支持 ?chapter_id=&stage_id= 按关卡筛选；0 人房间不下发。
func (sh *ServerHandler) HandleListRoom(c *fiber.Ctx) error {
	chapterID := parseIntOr(c.Query("chapter_id"), -1)
	stageID := parseIntOr(c.Query("stage_id"), -1)

	rooms := sh.RoomMnager.GetRooms(chapterID, stageID)
	if len(rooms) == 0 {
		rooms = []types.RoomsInfo{}
	}
	return c.JSON(rooms)
}

// HandleUserRoom 主页“开始游戏”前置检查：按昵称返回玩家当前所在房间。
func (sh *ServerHandler) HandleUserRoom(c *fiber.Ctx) error {
	name := utils.NormalizeUserName(c.Query("user"))
	membership := sh.RoomMnager.FindUserRoom(name)
	if membership == nil {
		return c.JSON(nil)
	}
	return c.JSON(membership)
}

// HandleLeaveUserRoom 玩家选择“不重连”时永久退出所在房间。
func (sh *ServerHandler) HandleLeaveUserRoom(c *fiber.Ctx) error {
	name := utils.NormalizeUserName(c.Query("user"))
	return c.JSON(fiber.Map{"ok": sh.RoomMnager.LeaveUserRoom(name)})
}

func (sh *ServerHandler) HandleWS(c *websocket.Conn) {
	// 协议包很小，限制单个 WS 消息大小，防止超大包直接进内存
	// checkpoint 的 inline 快照是重连恢复的唯一锚点。128KB 的正文加上
	// protobuf 包装仍远小于该上限；普通帧消息只有几十字节。
	c.SetReadLimit(512 * 1024)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("处理玩家注册时捕获到 Panic: %v\n", r)
			log.Printf("堆栈信息:\n%s", string(debug.Stack()))
		}
	}()

	// 获取查询参数中的房间ID
	roomId := -1
	if id := c.Query("id"); id != "" {
		if parsedId, err := strconv.Atoi(id); err == nil {
			roomId = parsedId
		}
	}

	_key := utils.NormalizeRoomKey(c.Query("key")) // 密钥
	userName := c.Query("user")

	// 断线重连：InGame 阶段保留席位，按房间号 + 玩家 id + 密钥重新绑定连接。
	if c.Query("reconnect") == "1" {
		myId, err := strconv.Atoi(c.Query("my_id"))
		if err != nil || roomId == -1 {
			utils.WriteLobbyResponse(c, &messages.LobbyResponse{
				Payload: &messages.LobbyResponse_JoinRoomFailed{
					JoinRoomFailed: &messages.ResponseJoinRoomFailed{Message: "重连参数不完整"},
				},
			})
			c.Close()
			return
		}
		targetRoom := sh.RoomMnager.GetRoom(roomId)
		if targetRoom == nil {
			utils.WriteLobbyResponse(c, &messages.LobbyResponse{
				Payload: &messages.LobbyResponse_JoinRoomFailed{
					JoinRoomFailed: &messages.ResponseJoinRoomFailed{Message: "房间不存在，无法重连"},
				},
			})
			c.Close()
			return
		}
		player := targetRoom.ReconnectPlayer(c, myId, _key)
		if player == nil {
			utils.WriteLobbyResponse(c, &messages.LobbyResponse{
				Payload: &messages.LobbyResponse_JoinRoomFailed{
					JoinRoomFailed: &messages.ResponseJoinRoomFailed{Message: "重连失败或已超出可恢复窗口"},
				},
			})
			c.Close()
			return
		}
		// 重连不发送 JoinRoomSuccess，客户端保持 InGame 阶段等待补帧。
		targetRoom.StartServeClient(player)
		return
	}

	// 没有指定房间号：创建新房间。
	// create=1 只是显式声明，便于排查问题；老客户端不带该参数也能建房。
	if roomId == -1 {
		roomId = sh.RoomMnager.GetNewRoomId()
	}

	// 获取或创建房间
	room := sh.RoomMnager.GetRoom(roomId)
	if room == nil {
		// 房间不存在,创建新房间
		roomName := utils.NormalizeRoomName(c.Query("name"))
		if roomName == "" {
			roomName = "房间" + strconv.Itoa(roomId)
		}
		chapterID := uint32(parseIntOr(c.Query("chapter_id"), 0))
		stageID := uint32(parseIntOr(c.Query("stage_id"), 0))
		room = sh.RoomMnager.AddRoom(roomId, _key, roomName, chapterID, stageID)
		// 房间开始运行
		go room.Run()
	} else {
		// 房间存在,检查密钥
		var payload = messages.ResponseJoinRoomFailed{}
		var resp = messages.LobbyResponse{
			Payload: &messages.LobbyResponse_JoinRoomFailed{
				JoinRoomFailed: &payload,
			},
		}
		if room.CheckKeyCorrect(_key) == false {
			payload.Message = "密钥错误"
			utils.WriteLobbyResponse(c, &resp)
			c.Close()
			return
		}

		// 房间存在,检查是否已满
		if room.GetPlayerCount() >= 2 {
			payload.Message = "房间已满"
			utils.WriteLobbyResponse(c, &resp)
			c.Close()
			return
		}

		// 加载与游戏中不允许再加入；等待房间与选卡阶段可以进
		if room.GameStage.IsLaterThanOrEqual(constants.STAGE_Loading) {
			payload.Message = "房间已开始"
			utils.WriteLobbyResponse(c, &resp)
			c.Close()
			return
		}
	}

	// 服务用户连接
	room.HandleClientJoin(c, userName)
}

package roomatom

// RoomCtx：房间内玩家表、阶段广播与消息发送。
// 所有方法都必须在房间主循环（Run 的 goroutine）中调用，避免并发修改玩家状态；
// 广播写经 ClientCtx 的异步写通道，不会阻塞主循环。

import (
	"log"
	"mvzserver/clients"
	"mvzserver/constants"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gofiber/websocket/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ClientCtx = clients.ClientCtx

// RoomCtx 房间级共享状态。
// 除 NextFrameID 为 atomic 外，其余字段由房间主循环独占。
type RoomCtx struct {
	OwnerUserID int       // 房主 id
	Players     PlayerMap // key: int, value: *Player
	nextId      int32     // 玩家 id 生成器，初始 100

	// 下一帧渲染 id（atomic；GameTicker 触发的主循环写，HTTP goroutine 读）
	NextFrameID *atomic.Uint32

	// game logic ticker；InGame 前为 nil
	GameTicker *time.Ticker
}

func NewRoomCtx() *RoomCtx {
	NextRenderFrame := &atomic.Uint32{}
	NextRenderFrame.Store(1) // 下一帧渲染为 1，当前都在 0
	return &RoomCtx{
		NextFrameID: NextRenderFrame,
		OwnerUserID: 0,
		nextId:      100,
		GameTicker:  nil,
	}
}

// Reset 一局结束后重置，允许下一场游戏。
func (m *RoomCtx) Reset() {
	m.NextFrameID.Store(1)
	if m.GameTicker != nil {
		m.GameTicker.Stop()
	}
	m.GameTicker = nil
	m.Players.Range(func(_ int, player *clients.Player) bool {
		if player != nil {
			player.ResetData()
		}
		return true
	})
}

// StartGameTicker 启动逻辑帧定时器（房间主循环独占）。
func (m *RoomCtx) StartGameTicker() {
	if m.GameTicker != nil {
		m.GameTicker.Stop()
	}
	m.GameTicker = time.NewTicker(time.Duration(constants.FrameIntervalMs) * time.Millisecond)
	log.Printf("⏱️ 逻辑帧间隔=%dms，命令窗口=[+%d,+%d]帧", constants.FrameIntervalMs, constants.CommandLeadFrames, constants.MaxDelayFrames)
}

// --------
// user

func (m *RoomCtx) CreateClientCtxFromConn(conn *websocket.Conn) *ClientCtx {
	newId := int(atomic.AddInt32(&m.nextId, 1))
	return clients.NewClientCtx(conn, newId)
}

// AddUser 注册玩家：分配最小空闲座位，首位加入者成为房主。
func (m *RoomCtx) AddUser(p *clients.Player) {
	if p == nil || p.Ctx == nil {
		return
	}
	p.SeatIndex = m.allocateSeat()
	m.Players.Store(p.Ctx.Id, p)
	if m.OwnerUserID == 0 {
		m.OwnerUserID = p.Ctx.Id
	}
}

// allocateSeat 返回 [0, RoomCapacity) 中最小的空闲座位号。
// 房间已满时返回 -1；WS 入口已按人数拒绝，这里只做兜底。
func (m *RoomCtx) allocateSeat() int {
	used := make(map[int]struct{}, constants.RoomCapacity)
	m.Players.Range(func(_ int, player *clients.Player) bool {
		if player != nil && player.SeatIndex >= 0 {
			used[player.SeatIndex] = struct{}{}
		}
		return true
	})
	for seat := 0; seat < constants.RoomCapacity; seat++ {
		if _, occupied := used[seat]; !occupied {
			return seat
		}
	}
	return -1
}

// DelUser 删除指定用户。
func (m *RoomCtx) DelUser(id int) {
	m.Players.Delete(id)
}

// CloseAll 关闭所有用户连接。
func (m *RoomCtx) CloseAll() {
	m.Players.Range(func(_ int, player *clients.Player) bool {
		if player != nil && player.Ctx != nil {
			player.Ctx.Close()
		}
		return true
	})
}

// CloseAllGracefully 关闭所有连接，但先把已入队的广播消息写出再断开。
// 房间销毁时用，确保 roomClosed 能送达客户端。
func (m *RoomCtx) CloseGracefully() {
	m.Players.Range(func(_ int, player *clients.Player) bool {
		if player != nil && player.Ctx != nil {
			player.Ctx.CloseGracefully()
		}
		return true
	})
}

// -------------------
// 发送信息
// -------------------

// BroadcastMessage 向房间内所有在线玩家广播。
// 写入经各连接的异步发送通道，慢连接不会阻塞房间主循环。
func (m *RoomCtx) BroadcastMessage(msg protoreflect.ProtoMessage, exclude_ids []int) {
	data, err := proto.Marshal(msg)
	if err != nil {
		// protobuf 契约消息不会失败；出现说明代码 bug，记录并放弃本条，
		// 不能因一条编码失败炸掉整个房间
		log.Printf("🔴 BroadcastMessage marshal 失败: %v", err)
		return
	}

	excludeSet := mapset.NewSet[int]()
	for _, id := range exclude_ids {
		excludeSet.Add(id)
	}

	m.Players.Range(func(_ int, player *clients.Player) bool {
		if player == nil || player.Ctx == nil || !player.Ctx.IsConnected() {
			return true
		}
		if excludeSet.Contains(player.Ctx.Id) {
			return true
		}
		player.Write(data)
		return true
	})
}

// SendMessageToUser 单播指定玩家。
func (m *RoomCtx) SendMessageToUser(msg protoreflect.ProtoMessage, userId int) {
	if value, ok := m.Players.Load(userId); ok {
		m.SendMessageToUserByPlayer(msg, value)
	}
}

func (m *RoomCtx) SendMessageToUserByPlayer(msg protoreflect.ProtoMessage, player *Player) {
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Printf("🔴 SendMessageToUserByPlayer marshal 失败: %v", err)
		return
	}
	if player != nil && player.Ctx != nil && player.Ctx.IsConnected() {
		player.Write(data)
	}
}

// GetPlayerCount 当前玩家数量。
func (m *RoomCtx) GetPlayerCount() uint32 {
	return uint32(m.Players.Len())
}

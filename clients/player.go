package clients

import (
	messages "mvzserver/messages/pb"
	"sync"
	"sync/atomic"
)

// PlayerMessage 房间主循环收到的玩家上行消息。
type PlayerMessage struct {
	Player *Player // 发送消息的玩家
	Data   []byte  // ws 二进制消息内容
}

var playerMessagePool = sync.Pool{
	New: func() interface{} {
		return new(PlayerMessage)
	},
}

func GetPlayerMessage(player *Player, data []byte) *PlayerMessage {
	msg := playerMessagePool.Get().(*PlayerMessage)
	msg.Player = player
	msg.Data = data
	return msg
}

func ReleasePlayerMessage(msg *PlayerMessage) {
	msg.Player = nil
	msg.Data = nil
	playerMessagePool.Put(msg)
}

// Player 房间内一名玩家的会话状态。
// 并发约定：除标明 atomic 的字段外，全部由房间主循环 goroutine 独占。
type Player struct {
	Ctx *ClientCtx // 客户端连接上下文

	// SeatIndex 房间内固定座位号，0 起；-1 表示尚未分配。
	// 由 RoomCtx.AddUser 分配最小空闲座位；退出后自动释放，供后来者补位。
	SeatIndex int

	IsReady  bool // 是否准备好（等待房间 / 选卡确认共用）
	IsLoaded bool // 是否加载完毕
	// LoadProgress Loading 阶段加载进度，1..100；100 表示完全加载好等待同步。
	// 由房间主循环独占读写，仅通过 playerLoaded 请求推进。
	LoadProgress uint32

	// 玩家昵称，由客户端连接时携带；仅用于房间内展示
	Name string
	// JoinKey 加入房间时使用的密钥；房主改密码后旧成员仍可用它重连
	JoinKey string

	// 已主动推送给该玩家的最大帧：普通 tick 只推新帧，避免重复补发风暴。
	LastSyncedFrameID atomic.Uint32
	// Loadout 开局确定性配置，由 ready 请求上报，AllReady 时规范化广播
	Loadout *messages.LoadoutSnapshot
	// ConnectionEpoch 连接代数：重连后自增，旧读循环退出时不得注销新连接
	ConnectionEpoch int
}

func NewPlayer(ctx *ClientCtx) Player {
	return Player{
		Ctx:          ctx,
		SeatIndex:    -1,
		IsReady:      false,
		IsLoaded:     false,
		LoadProgress: 0,
	}
}

// ResetData 一局结束后清空对局相关状态（房间主循环独占调用）。
func (p *Player) ResetData() {
	p.IsReady = false
	p.IsLoaded = false
	p.LoadProgress = 0
	p.Loadout = nil
	p.LastSyncedFrameID.Store(0)
	if p.Ctx != nil {
		p.Ctx.LatestFrameID.Store(0)
		p.Ctx.LatestAckFrameID.Store(0)
		p.Ctx.StateHashVersion.Store(0)
		p.Ctx.StateHashVersionReported.Store(false)
	}
}

func (p *Player) GetID() int {
	if p == nil || p.Ctx == nil {
		return -1
	}
	return p.Ctx.Id
}

// Write 把序列化后的消息送入该玩家的发送通道。
// 连接假死时返回 false；调用方无需处理，断开事件由读循环产生。
func (p *Player) Write(bytes []byte) bool {
	if p == nil || p.Ctx == nil {
		return false
	}
	return p.Ctx.Send(bytes)
}

package clients

// client player context
// 封装长连接等信息。游戏逻辑无关，帧同步游戏不记录玩家行为。
//
// 并发约定：
//   - writer 只由本类型内部方法访问；外部统一走 Send/Close/IsConnected。
//   - 写路径独占在 ConnWriter goroutine（无阻塞主循环风险）。
//   - LatestFrameID / LatestAckFrameID / lastMessageAtMs 为 atomic，
//     读循环与房间主循环都会访问。
//   - State 由房间主循环独占写。

import (
	"mvzserver/constants"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/websocket/v2"
)

type ClientCtx struct {
	Id   int    // 用户 id
	Name string // 展示名（仅日志用）

	// Conn 连接句柄。只在读循环 goroutine 内使用（StartServeClient 独占读）；
	// 关闭统一走 writer，不触碰该字段。
	Conn *websocket.Conn

	// 帧同步信息（atomic）
	LatestFrameID    atomic.Uint32 // 最近服务器获知的该用户所在帧
	LatestAckFrameID atomic.Uint32 // 最近该用户 ACK 的帧
	// StateHashVersion 客户端确定性哈希口径版本；0 表示旧客户端未上报。
	StateHashVersion atomic.Uint32
	// StateHashVersionReported 是否已经从该客户端的心跳中收到过版本字段。
	// 不能用 LatestFrameID > 0 代替：重连后的新上下文会从 checkpoint 帧
	// 开始，但客户端尚未上报版本，此时把默认值 0 当成“旧客户端”会误报
	// 版本不一致。
	StateHashVersionReported atomic.Bool
	// ReportedLatencyMs 客户端自报的往返延迟（毫秒），用于重连时挑选快照来源。
	// 0 表示客户端尚未上报。
	ReportedLatencyMs atomic.Uint32

	// 上次收到该连接任意消息的时间戳（毫秒）；由读循环更新，主循环读取。
	// 用于心跳超时判定：大厅阶段没有业务帧，靠它发现假死连接。
	lastMessageAtMs atomic.Int64

	// 写通道：独立 goroutine 独占写连接，慢连接不阻塞房间主循环
	writer *ConnWriter

	// closeOnce 保护 writer 只关闭一次
	closeOnce sync.Once

	// 其他信息
	State constants.PlayerState // 当前状态（在线/断线），房间主循环独占写
}

// NewClientCtx 创建客户端上下文并启动异步写 goroutine。
func NewClientCtx(conn *websocket.Conn, id int) *ClientCtx {
	ctx := &ClientCtx{
		Id:     id,
		Conn:   conn,
		writer: NewConnWriter(id, conn),
		State:  constants.PlayerStateConnected,
	}
	// 连接建立时刻即视为一次心跳，避免“从未收到消息”时永不超时
	ctx.TouchHeartbeat()
	return ctx
}

// ForceClose 直接关闭底层连接，不等待读循环退出。
// 用于踢人：目标玩家的读循环仍阻塞在 ReadMessage 上。
func (c *ClientCtx) ForceClose() {
	c.Close()
}

// Close 关闭连接与写通道；幂等。
func (c *ClientCtx) Close() {
	if c == nil || c.writer == nil {
		return
	}
	c.closeOnce.Do(func() {
		c.writer.Close()
	})
}

// CloseGracefully 关闭写通道，但先把已入队的消息（如 roomClosed）写出去。
func (c *ClientCtx) CloseGracefully() {
	if c == nil || c.writer == nil {
		return
	}
	c.closeOnce.Do(func() {
		c.writer.CloseGracefully()
	})
}

// Send 经写通道异步发送二进制消息；连接假死（队列满/已关）返回 false。
func (c *ClientCtx) Send(data []byte) bool {
	if c == nil || c.writer == nil {
		return false
	}
	return c.writer.Send(data)
}

// IsConnected 判断写通道是否仍然存活。
func (c *ClientCtx) IsConnected() bool {
	if c == nil || c.writer == nil {
		return false
	}
	return !c.writer.isClosedFlag()
}

// SafeReadMessage 从连接读取一条消息；连接关闭后返回错误。
func (c *ClientCtx) SafeReadMessage(conn *websocket.Conn) (messageType int, p []byte, err error) {
	if conn == nil {
		return 0, nil, ErrConnClosed
	}
	messageType, p, err = conn.ReadMessage()
	if err == nil {
		c.TouchHeartbeat()
	}
	return messageType, p, err
}

// TouchHeartbeat 记录最近一次收到消息的时间。
func (c *ClientCtx) TouchHeartbeat() {
	c.lastMessageAtMs.Store(time.Now().UnixMilli())
}

// IsHeartbeatExpired 判断该连接是否已超过 timeout 没有任何消息。
// 大厅阶段客户端不发业务帧，靠该函数发现拔线/假死连接。
func (c *ClientCtx) IsHeartbeatExpired(timeout time.Duration) bool {
	last := c.lastMessageAtMs.Load()
	if last == 0 {
		return false
	}
	return time.Now().UnixMilli()-last > timeout.Milliseconds()
}

// UpdatePlayerFrame 单调更新该客户端的帧进度与 ACK。
func (c *ClientCtx) UpdatePlayerFrame(frameId, ackFrameId uint32) {
	for {
		old := c.LatestFrameID.Load()
		if frameId <= old || c.LatestFrameID.CompareAndSwap(old, frameId) {
			break
		}
	}
	for {
		old := c.LatestAckFrameID.Load()
		if ackFrameId <= old || c.LatestAckFrameID.CompareAndSwap(old, ackFrameId) {
			break
		}
	}
}

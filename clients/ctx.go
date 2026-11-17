package clients

import (
	"fmt"
	"mvzserver/constants"
	"net"
	"sync"

	"sync/atomic"

	"github.com/gofiber/websocket/v2"
)

// client player context
// 封装如长连接等信息
// 游戏逻辑无关, 因为帧同步游戏暂时不记录游戏玩家行为

type ClientCtx struct {
	mu   sync.RWMutex    // 保护连接的读写锁
	Conn *websocket.Conn // 长连接
	// 底层 TCP 连接：读循环会长时间持读锁阻塞在 ReadMessage 上，
	// 踢人等需要立刻断开连接的场景只能绕过读写锁直接关闭它。
	rawConn net.Conn

	// 信息
	Id int // 用户id

	// 帧同步信息

	// 最近服务器获知的该用户的所在帧（原子变量）
	LatestFrameID atomic.Uint32

	// 最近该用户ACK的帧（原子变量）
	LatestAckFrameID atomic.Uint32

	// 其他信息
	ReconnectionToken string                // 重连令牌， 用服务器对称加密得到的包含有用户id,房间信息的token           // 用于重连验证的票据
	State             constants.PlayerState // 当前状态（在线/断线）
}

// unsafeConner 由 fasthttp 的 hijackConn 实现，可取出真正持有 socket 的连接。
type unsafeConner interface {
	UnsafeConn() net.Conn
}

func NewClientCtx(conn *websocket.Conn, id int) *ClientCtx {
	var rawConn net.Conn
	if conn != nil {
		rawConn = conn.UnderlyingConn()
		// hijackConn.Close 在 KeepHijackedConns=false 时是空实现，必须取到内部真实连接
		if unwrapper, ok := rawConn.(unsafeConner); ok {
			rawConn = unwrapper.UnsafeConn()
		}
	}
	return &ClientCtx{
		Conn:    conn,
		rawConn: rawConn,
		Id:      id,
		// atomic.Uint32 默认零值即可，无需额外初始化
	}
}

// ForceClose 直接关闭底层连接，不等待读循环释放读锁。
// 用于房主踢人：目标玩家的读循环仍阻塞在 ReadMessage 上，常规 Close 会阻塞房间主循环。
func (c *ClientCtx) ForceClose() {
	if c.rawConn != nil {
		_ = c.rawConn.Close()
	}
}

func (c *ClientCtx) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Conn != nil {
		fmt.Printf("🔴 Closing connection for client %d\n", c.Id)
		c.Conn.Close()
		c.Conn = nil // 设置为 nil 以防止后续使用
		fmt.Printf("🔴 Connection closed for client %d\n", c.Id)
	} else {
		fmt.Printf("🔴 Connection already nil for client %d\n", c.Id)
	}
}

// Deprecated: conn只写二进制
func (c *ClientCtx) WriteJSON(v interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.Conn == nil {
		return fmt.Errorf("connection is nil")
	}
	return c.Conn.WriteJSON(v)
}

// SafeReadMessage 安全地读取WebSocket消息
func (c *ClientCtx) SafeReadMessage() (messageType int, p []byte, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.Conn == nil {
		return 0, nil, fmt.Errorf("connection is nil")
	}

	// 尝试读取消息
	messageType, p, err = c.Conn.ReadMessage()
	if err != nil {
		// 只在非正常关闭时输出详细错误
		if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure, websocket.CloseNoStatusReceived) {
			fmt.Printf("� SafeReadMessage error for client %d: %v (type: %T)\n", c.Id, err, err)
		}
	}
	return messageType, p, err
}

// SafeWriteMessage 安全地写入WebSocket消息
func (c *ClientCtx) SafeWriteMessage(messageType int, data []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.Conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// 添加调试日志
	err := c.Conn.WriteMessage(messageType, data)
	if err != nil {
		fmt.Printf("🔴 SafeWriteMessage error for client %d: %v\n", c.Id, err)
	}
	return err
}

// IsConnected 安全地检查连接是否有效
func (c *ClientCtx) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Conn != nil
}

// GetRemoteAddr 安全地获取远程地址
func (c *ClientCtx) GetRemoteAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.Conn == nil {
		return ""
	}
	return c.Conn.RemoteAddr().String()
}

func (c *ClientCtx) UpdatePlayerFrame(frameId, ackFrameId uint32) {
	old1 := c.LatestFrameID.Load()
	if frameId > old1 {
		c.LatestFrameID.Store(frameId)
	}
	old2 := c.LatestAckFrameID.Load()
	if ackFrameId > old2 {
		c.LatestAckFrameID.Store(ackFrameId)
	}
}

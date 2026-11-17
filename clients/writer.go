package clients

// 每个连接独立的发送队列与 writer goroutine。
// 房间主循环只做非阻塞入队，慢连接永远无法阻塞房间主循环；
// 队列满或写失败即关闭连接，由读循环触发 unregister/保留席位逻辑。

import (
	"errors"
	"log"
	"sync"

	"github.com/gofiber/websocket/v2"
)

// ErrConnClosed 连接已关闭或从未建立时读/写的哨兵错误。
var ErrConnClosed = errors.New("connection is closed")

// outboundQueueSize 单连接发送队列容量。
// 正常联机每秒约 20~40 帧小包；512 可容忍数秒网络抖动。
// 超过即认为连接已假死，主动断开等待重连。
const outboundQueueSize = 512

// ConnWriter 绑定一条 WS 连接的异步发送器。
// Start 后由内部 goroutine 独占写连接；Close 幂等并终止 goroutine。
type ConnWriter struct {
	// ctx 归属的 ClientCtx；仅用于日志与关闭回调，不访问其锁
	id int

	mu       sync.Mutex
	conn     *websocket.Conn
	queue    chan []byte
	closed   bool
	done     chan struct{}
	finished chan struct{}
}

// NewConnWriter 创建异步发送器并启动 writer goroutine。
func NewConnWriter(id int, conn *websocket.Conn) *ConnWriter {
	w := &ConnWriter{
		id:       id,
		conn:     conn,
		queue:    make(chan []byte, outboundQueueSize),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	go w.run()
	return w
}

// Send 把消息放入发送队列；队列满或已关闭返回 false，调用方应视为连接死亡。
// 非阻塞：绝不等待慢连接。
func (w *ConnWriter) Send(data []byte) bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()
	if closed {
		return false
	}

	select {
	case w.queue <- data:
		return true
	default:
		// 队列满：连接假死，立刻关闭，让读循环退出触发席位保留/清理
		log.Printf("🔴 玩家 %d 发送队列已满，判定连接假死并关闭", w.id)
		w.Close()
		return false
	}
}

// isClosedFlag 返回关闭标记（无锁读，仅用于存活探测；存在瞬时误差可接受）。
func (w *ConnWriter) isClosedFlag() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// Close 幂等关闭：终止 goroutine 并关闭底层 WS 连接。
func (w *ConnWriter) Close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()

	close(w.done)
	// 关闭 WS 连接让阻塞中的 WriteMessage 立刻出错返回
	if w.conn != nil {
		_ = w.conn.Close()
	}
}

// Wait 等待 writer goroutine 退出；主要用于测试与关停确认。
func (w *ConnWriter) Wait() {
	if w == nil {
		return
	}
	<-w.finished
}

func (w *ConnWriter) run() {
	defer close(w.finished)
	for {
		select {
		case <-w.done:
			return
		case data := <-w.queue:
			if w.conn == nil {
				return
			}
			if err := w.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				log.Printf("🔴 玩家 %d 写入失败: %v", w.id, err)
				// 写失败：连接不可用。关闭自身触发读循环退出。
				w.Close()
				// 丢弃剩余队列，尽快退出
				return
			}
		}
	}
}

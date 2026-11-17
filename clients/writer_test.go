package clients

// ConnWriter 单元测试：并发安全、队列满判定、幂等关闭。

import (
	"sync"
	"testing"
	"time"
)

func TestConnWriterSendAndClose(t *testing.T) {
	// nil conn（测试环境无真实 WS）：Send 入队成功，Close 幂等
	w := NewConnWriter(1, nil)
	if !w.Send([]byte("frame")) {
		t.Fatal("新建 writer 应能入队")
	}
	w.Close()
	w.Close() // 幂等
	if w.Send([]byte("after-close")) {
		t.Fatal("关闭后不应再接受消息")
	}
	w.Wait()
}

func TestConnWriterConcurrentSendAndClose(t *testing.T) {
	w := NewConnWriter(2, nil)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 64; j++ {
				w.Send([]byte{byte(j)})
			}
		}()
	}
	// 并发关闭不应 panic 或 data race
	go func() {
		w.Close()
	}()
	wg.Wait()
	w.Close()
	w.Wait()
}

func TestClientCtxHeartbeat(t *testing.T) {
	ctx := NewClientCtx(nil, 3)
	if ctx.IsHeartbeatExpired(time.Hour) {
		t.Fatal("刚创建的连接心跳不应过期")
	}
	// 模拟很久没有消息
	ctx.lastMessageAtMs.Store(1)
	if !ctx.IsHeartbeatExpired(time.Second) {
		t.Fatal("长时间无消息应判定心跳过期")
	}
	ctx.TouchHeartbeat()
	if ctx.IsHeartbeatExpired(time.Hour) {
		t.Fatal("TouchHeartbeat 后不应过期")
	}
	if !ctx.IsConnected() {
		t.Fatal("正常创建的上下文应显示已连接")
	}
	ctx.Close()
	if ctx.IsConnected() {
		t.Fatal("关闭后应显示未连接")
	}
}

func TestClientCtxFrameProgressMonotonic(t *testing.T) {
	ctx := NewClientCtx(nil, 4)
	ctx.UpdatePlayerFrame(10, 8)
	ctx.UpdatePlayerFrame(5, 12) // 更小的 frame 不应回退
	if got := ctx.LatestFrameID.Load(); got != 10 {
		t.Fatalf("frame 不应回退，得到 %d", got)
	}
	if got := ctx.LatestAckFrameID.Load(); got != 12 {
		t.Fatalf("ack 应前进到 12，得到 %d", got)
	}
}

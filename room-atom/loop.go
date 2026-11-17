package roomatom

// 房间状态机主循环。

import (
	"log"
	"mvzserver/clients"
	"mvzserver/constants"
	"runtime/debug"
	"time"

	"github.com/gofiber/websocket/v2"
)

// Run 房间主循环。所有房间状态的修改都串行发生在这里。
func (room *Room) Run() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("运行房间 %d 捕获到 Panic: %v\n", room.ID, r)
			log.Printf("堆栈信息:\n%s", string(debug.Stack()))
			log.Println("程序已从 panic 中恢复，将销毁房间。")
		}
		// 摧毁本房间
		room.Destroy()
	}()

	// 初始房间状态, 大厅中等待玩家
	room.GameStage.Store(constants.STAGE_InLobby)

	// 秒级维护：心跳扫描、重连宽限、PostGame 超时
	maintenanceTicker := time.NewTicker(1 * time.Second)
	defer maintenanceTicker.Stop()

	/* 状态机主循环：根据用户输入和房间的当前状态来进行分支 */
	for {
		// InGame 状态才有逻辑帧定时器
		var tickerChan (<-chan time.Time)
		if room.GameStage.EqualTo(constants.STAGE_InGame) && room.RoomCtx.GameTicker != nil {
			tickerChan = room.RoomCtx.GameTicker.C
		}

		select {
		// 1. 客户端管理事件
		case player := <-room.register:
			room.handleRegister(player)

		case player := <-room.unregister:
			room.handleUnregister(player)

		case request := <-room.reconnect:
			room.handleReconnect(request)

		// 2. 玩家业务消息
		case message := <-room.incomingMessages:
			room.handlePlayerMessage(message)

		// 3. 逻辑帧（仅 InGame）
		case <-tickerChan:
			room.runGameTick()

		// 4. 秒级维护
		case <-maintenanceTicker.C:
			room.tickMaintenance()
		}
	}
}

// tickMaintenance 每秒一次的房间维护。
func (room *Room) tickMaintenance() {
	room.tickHeartbeat()
	room.tickPostGame()
	room.sweepStaleDisconnected()
}

// StartServeClient 该连接的读循环：把上行消息投递给房间主循环，
// 阻塞直到连接关闭（正常断开 / 心跳超时 / 写失败关闭 / 被踢）。
func (room *Room) StartServeClient(player *clients.Player) {
	ctx := player.Ctx
	epoch := player.ConnectionEpoch

	if ctx == nil {
		return
	}

	defer func() {
		// 连接代数已变化说明玩家已重连，旧读循环不得注销新连接。
		if player.ConnectionEpoch != epoch {
			log.Printf("🟡 Skip unregister for player %d (reconnected, epoch %d -> %d)",
				player.GetID(), epoch, player.ConnectionEpoch)
			return
		}

		select {
		case room.unregister <- player:
		default:
			// channel 满说明房间可能正在关闭；直接关闭连接兜底
			log.Printf("🔴 Failed to send unregister signal for player %d (channel full)", player.GetID())
			if player != nil && player.Ctx != nil {
				player.Ctx.Close()
			}
		}

		if r := recover(); r != nil {
			log.Printf("服务用户 %d 时捕获到 Panic: %v\n", player.GetID(), r)
			log.Printf("堆栈信息:\n%s", string(debug.Stack()))
		}
	}()

	for {
		messageType, data, err := ctx.SafeReadMessage(ctx.Conn)
		if err != nil {
			// 读错误即连接结束：断开（客户端主动 / 心跳超时 / 写通道关闭 / 被踢）。
			// unregister 事件由 defer 发给主循环，按阶段决定保留席位或移除。
			return
		}

		// 热路径不打印日志，忽略非二进制消息
		if messageType != websocket.BinaryMessage {
			continue
		}

		msg := clients.GetPlayerMessage(player, data)
		// 发送到主循环管道；管道瞬时积压时稍等重试，保持顺序。
		for {
			select {
			case room.incomingMessages <- msg:
				goto delivered
			default:
				time.Sleep(2 * time.Millisecond)
			}
		}
	delivered:
	}
}

package constants

import "time"

// --- 联机机制参数 ---
// 集中管理原先散落的魔法数，调整联机手感/资源策略时只改这里。

// ReconnectGracePeriod InGame 掉线后的重连宽限期。
// 宽限期内席位保留、可补帧恢复；超时后服务端写入权威判负帧并结束对局。
const ReconnectGracePeriod = 60 * time.Second

// HeartbeatTimeout 连接心跳超时：超过该时长没有任何上行消息即判定假死。
// 大厅阶段没有业务帧，靠它发现拔线客户端。要求客户端 WSHeartbeatInterval 上报。
const HeartbeatTimeout = 15 * time.Second

// PostGameLinger 结束帧落地后保留房间与历史帧的时长，
// 供落后客户端补帧到结束帧；之后房间回 InLobby 等待“再来一局”。
const PostGameLinger = 10 * time.Second

// RoomIdleTimeout 房间空闲回收时间（房间管理器清理用）。
const RoomIdleTimeout = 5 * time.Minute

// MaxHistoryFrames 权威帧保留窗口（帧数）。
const MaxHistoryFrames uint32 = 600

// MaxResendPerSync 单次同步最多补发的帧数。
const MaxResendPerSync uint32 = 32

package constants

import (
	"os"
	"strconv"
)

// DefaultFrameIntervalMs 是生产环境的逻辑帧间隔：20 tick/s。
const DefaultFrameIntervalMs = 50

// FrameIntervalMs 是逻辑帧间隔（毫秒）。
// DEBUG 测试可通过 MVZ_DEBUG_FRAME_MS 把它压到最短（例如 2ms），
// 帧内业务窗口（CommandLeadFrames / MaxDelayFrames）保持不变，
// 因此网络延迟也应同步按帧长缩放。
var FrameIntervalMs = resolveFrameIntervalMs()

func resolveFrameIntervalMs() int {
	raw := os.Getenv("MVZ_DEBUG_FRAME_MS")
	if raw == "" {
		return DefaultFrameIntervalMs
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 1 || ms > DefaultFrameIntervalMs {
		return DefaultFrameIntervalMs
	}
	return ms
}

const MaxDelayFrames uint32 = 10 // 最大延迟帧数
const CommandLeadFrames uint32 = 4

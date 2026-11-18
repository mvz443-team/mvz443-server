package roomatom

// 每帧资源 profile：客户端在 RequestBlank 中上报自己的能量、星之碎片与
// 卡槽冷却，服务端按玩家保存最近一份。页面刷新后本地模拟状态全部丢失，
// 重连时随 AllReady 回传，作为快照之外的第二条恢复路径。

import (
	"sort"

	"mvzserver/messages/pb"
)

// recordPlayerResources 覆盖保存玩家最近上报的资源 profile。
// 旧客户端不会置 resources_reported，不能用零值覆盖已有数据。
func (room *Room) recordPlayerResources(playerID int, req *messages.RequestBlank) {
	if room == nil || req == nil || !req.ResourcesReported {
		return
	}
	profile := &messages.PlayerResourceState{
		PlayerId:      uint32(playerID),
		Energy:        req.Energy,
		StarShards:    req.StarShards,
		FrameId:       req.FrameId,
		CardCooldowns: make([]*messages.CardCooldownState, 0, len(req.CardCooldowns)),
	}
	for _, cooldown := range req.CardCooldowns {
		if cooldown == nil {
			continue
		}
		profile.CardCooldowns = append(profile.CardCooldowns, &messages.CardCooldownState{
			Pid:     cooldown.Pid,
			Level:   cooldown.Level,
			LeftMs:  cooldown.LeftMs,
			TotalMs: cooldown.TotalMs,
		})
	}
	room.resourceProfiles[playerID] = profile
}

// captureResourceProfiles 返回按玩家 ID 排序的副本，供 AllReady 广播/单播。
// 未上报过的玩家不出现在结果中，客户端据此区分“无 profile”和“资源为 0”。
func (room *Room) captureResourceProfiles() []*messages.PlayerResourceState {
	if room == nil || len(room.resourceProfiles) == 0 {
		return nil
	}
	ids := make([]int, 0, len(room.resourceProfiles))
	for id := range room.resourceProfiles {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	profiles := make([]*messages.PlayerResourceState, 0, len(ids))
	for _, id := range ids {
		profile := room.resourceProfiles[id]
		if profile == nil {
			continue
		}
		clone := &messages.PlayerResourceState{
			PlayerId:   profile.PlayerId,
			Energy:     profile.Energy,
			StarShards: profile.StarShards,
			FrameId:    profile.FrameId,
		}
		clone.CardCooldowns = make([]*messages.CardCooldownState, 0, len(profile.CardCooldowns))
		for _, cooldown := range profile.CardCooldowns {
			if cooldown == nil {
				continue
			}
			clone.CardCooldowns = append(clone.CardCooldowns, &messages.CardCooldownState{
				Pid:     cooldown.Pid,
				Level:   cooldown.Level,
				LeftMs:  cooldown.LeftMs,
				TotalMs: cooldown.TotalMs,
			})
		}
		profiles = append(profiles, clone)
	}
	return profiles
}

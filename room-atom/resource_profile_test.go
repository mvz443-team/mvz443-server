package roomatom

import (
	"mvzserver/messages/pb"
	"testing"
)

func TestResourceProfileRecordsLatestReportedValues(t *testing.T) {
	room := NewRoom(1, nil)
	room.recordPlayerResources(102, &messages.RequestBlank{
		ResourcesReported: true,
		Energy:            75,
		StarShards:        3,
		CardCooldowns: []*messages.CardCooldownState{
			{Pid: 1, Level: 2, LeftMs: 350, TotalMs: 6000},
		},
	})
	profiles := room.captureResourceProfiles()
	if len(profiles) != 1 {
		t.Fatalf("应有 1 份 profile，得到 %d", len(profiles))
	}
	profile := profiles[0]
	if profile.PlayerId != 102 || profile.Energy != 75 || profile.StarShards != 3 {
		t.Fatalf("profile 不匹配: %+v", profile)
	}
	if len(profile.CardCooldowns) != 1 || profile.CardCooldowns[0].LeftMs != 350 {
		t.Fatalf("卡槽冷却不匹配: %+v", profile.CardCooldowns)
	}

	// 旧客户端不置 resources_reported：零值心跳不能覆盖已有 profile。
	room.recordPlayerResources(102, &messages.RequestBlank{Energy: 0})
	if got := room.captureResourceProfiles()[0].Energy; got != 75 {
		t.Fatalf("旧客户端心跳不得覆盖 profile，能量变为 %d", got)
	}

	// 新的一帧应覆盖为最新值。
	room.recordPlayerResources(102, &messages.RequestBlank{
		ResourcesReported: true,
		Energy:            50,
	})
	if got := room.captureResourceProfiles()[0].Energy; got != 50 {
		t.Fatalf("新心跳应覆盖 profile，能量为 %d", got)
	}
}

func TestResourceProfileClearedOnReset(t *testing.T) {
	room := NewRoom(1, nil)
	room.recordPlayerResources(101, &messages.RequestBlank{ResourcesReported: true, Energy: 10})
	room.Reset()
	if profiles := room.captureResourceProfiles(); len(profiles) != 0 {
		t.Fatalf("Reset 后不得保留上一局资源 profile: %+v", profiles)
	}
}

func TestResourceProfileCopiesAreIsolated(t *testing.T) {
	room := NewRoom(1, nil)
	room.recordPlayerResources(101, &messages.RequestBlank{
		ResourcesReported: true,
		Energy:            10,
		CardCooldowns:     []*messages.CardCooldownState{{Pid: 1, LeftMs: 100, TotalMs: 6000}},
	})
	profiles := room.captureResourceProfiles()
	profiles[0].Energy = 999
	profiles[0].CardCooldowns[0].LeftMs = 1
	fresh := room.captureResourceProfiles()[0]
	if fresh.Energy != 10 || fresh.CardCooldowns[0].LeftMs != 100 {
		t.Fatalf("返回副本被修改污染了内部 profile: %+v", fresh)
	}
}

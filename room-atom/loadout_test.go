package roomatom

import (
	messages "mvzserver/messages/pb"
	"testing"
)

func TestCanonicalLoadoutHash(t *testing.T) {
	src := &messages.LoadoutSnapshot{
		Cards: []*messages.LoadoutCard{
			{Pid: 3, Level: 1},
			{Pid: 1, Level: 2},
			{Pid: 3, Level: 9}, // 重复 pid 应被忽略
		},
		Level:             5,
		InitialEnergy:     0,
		InitialStarShards: 0,
		GlobalModifiers: []*messages.LoadoutModifier{
			{Key: "atk", Value: 10},
		},
	}

	canonical, hashValue := canonicalLoadout(src)
	if len(canonical.Cards) != 2 || canonical.Cards[0].Pid != 1 || canonical.Cards[1].Pid != 3 {
		t.Fatalf("卡片必须按 pid 排序并去重，得到 %+v", canonical.Cards)
	}
	if hashValue != "a90a6c47" {
		t.Fatalf("哈希必须与客户端一致，期望 a90a6c47，得到 %s", hashValue)
	}

	// 输入顺序不同应得到同一摘要
	shuffled := &messages.LoadoutSnapshot{
		Cards: []*messages.LoadoutCard{{Pid: 1, Level: 2}, {Pid: 3, Level: 1}},
		Level: 5,
		GlobalModifiers: []*messages.LoadoutModifier{
			{Key: "atk", Value: 10},
		},
	}
	_, hashValue2 := canonicalLoadout(shuffled)
	if hashValue2 != hashValue {
		t.Fatalf("规范化的 loadout 必须产生相同哈希：%s != %s", hashValue2, hashValue)
	}
}

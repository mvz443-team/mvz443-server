package roomatom

import (
	"fmt"
	"hash/fnv"
	messages "mvzserver/messages/pb"
	"sort"
	"strconv"
	"strings"
)

// canonicalLoadout 对客户端上报的 loadout 去重、排序，生成所有端一致的开局配置。
func canonicalLoadout(src *messages.LoadoutSnapshot) (*messages.LoadoutSnapshot, string) {
	if src == nil {
		src = &messages.LoadoutSnapshot{}
	}

	seenCards := make(map[uint32]struct{}, len(src.Cards))
	cards := make([]*messages.LoadoutCard, 0, len(src.Cards))
	for _, card := range src.Cards {
		if card == nil {
			continue
		}
		if _, exists := seenCards[card.Pid]; exists {
			continue
		}
		seenCards[card.Pid] = struct{}{}
		cards = append(cards, &messages.LoadoutCard{Pid: card.Pid, Level: card.Level})
	}
	sort.Slice(cards, func(i, j int) bool { return cards[i].Pid < cards[j].Pid })

	modifiers := make([]*messages.LoadoutModifier, 0, len(src.GlobalModifiers))
	seenModifiers := make(map[string]struct{}, len(src.GlobalModifiers))
	for _, modifier := range src.GlobalModifiers {
		if modifier == nil {
			continue
		}
		if _, exists := seenModifiers[modifier.Key]; exists {
			continue
		}
		seenModifiers[modifier.Key] = struct{}{}
		modifiers = append(modifiers, &messages.LoadoutModifier{Key: modifier.Key, Value: modifier.Value})
	}
	sort.Slice(modifiers, func(i, j int) bool { return modifiers[i].Key < modifiers[j].Key })

	canonical := &messages.LoadoutSnapshot{
		Cards:             cards,
		Level:             src.Level,
		InitialEnergy:     src.InitialEnergy,
		InitialStarShards: src.InitialStarShards,
		GlobalModifiers:   modifiers,
	}
	return canonical, loadoutHash(canonical)
}

// loadoutHash 必须与客户端 utils/net/loadout.ts 中的实现保持一致。
func loadoutHash(loadout *messages.LoadoutSnapshot) string {
	cardParts := make([]string, 0, len(loadout.Cards))
	for _, card := range loadout.Cards {
		cardParts = append(cardParts, strconv.FormatUint(uint64(card.Pid), 10)+":"+strconv.FormatUint(uint64(card.Level), 10))
	}
	modifierParts := make([]string, 0, len(loadout.GlobalModifiers))
	for _, modifier := range loadout.GlobalModifiers {
		modifierParts = append(modifierParts, modifier.Key+"="+strconv.FormatInt(int64(modifier.Value), 10))
	}

	canonical := strings.Join([]string{
		"cards=" + strings.Join(cardParts, ","),
		"level=" + strconv.FormatUint(uint64(loadout.Level), 10),
		"energy=" + strconv.FormatInt(int64(loadout.InitialEnergy), 10),
		"stars=" + strconv.FormatUint(uint64(loadout.InitialStarShards), 10),
		"mods=" + strings.Join(modifierParts, ","),
	}, "|")

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(canonical))
	return fmt.Sprintf("%08x", hasher.Sum32())
}

// canonicalPlayerLoadouts 汇总所有玩家的规范化开局配置，按玩家 ID 排序。
func (room *Room) canonicalPlayerLoadouts() []*messages.PlayerLoadout {
	result := make([]*messages.PlayerLoadout, 0)
	room.RoomCtx.Players.Range(func(_ int, value *Player) bool {
		if value == nil || value.Ctx == nil {
			return true
		}
		canonical, hashValue := canonicalLoadout(value.Loadout)
		result = append(result, &messages.PlayerLoadout{
			PlayerId: uint32(value.Ctx.Id),
			Loadout:  canonical,
			Hash:     hashValue,
		})
		return true
	})
	sort.Slice(result, func(i, j int) bool { return result[i].PlayerId < result[j].PlayerId })
	return result
}

// api 信息定义
package types

import "mvzserver/constants"

type RoomsInfo struct {
	RoomID      int             `json:"room_id"`
	RoomName    string          `json:"room_name"`
	NeedKey     bool            `json:"need_key"`
	PlayerCount int             `json:"player_count"`
	GameState   constants.Stage `json:"game_state"` // 游戏状态
	// 房间绑定的章节与关卡，创建房间时确定，用于按关卡筛选房间
	ChapterID uint32 `json:"chapter_id"`
	StageID   uint32 `json:"stage_id"`

	// Deprecated: 是否开始游戏
	// GameStarted bool `json:"game_started"`
}

package utils

import "strings"

// 房间名与密码的字符上限（按 rune 计算，与客户端限制保持一致）
const (
	MaxRoomNameRunes = 6
	MaxRoomKeyRunes  = 8
	MaxUserNameRunes = 16
)

// TruncateRunes 按字符截断，避免把多字节字符截成半个
func TruncateRunes(value string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

// stripControl 去掉控制字符，防止房间名/密码里混入换行等异常内容
func stripControl(value string) string {
	var builder strings.Builder
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			continue
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

// NormalizeRoomName 房间名：去首尾空白与控制字符，最多 6 字符
func NormalizeRoomName(value string) string {
	return TruncateRunes(strings.TrimSpace(stripControl(value)), MaxRoomNameRunes)
}

// NormalizeRoomKey 房间密码：去掉所有空白与控制字符，最多 8 字符
func NormalizeRoomKey(value string) string {
	return TruncateRunes(strings.Join(strings.Fields(stripControl(value)), ""), MaxRoomKeyRunes)
}

// NormalizeUserName 玩家昵称：去首尾空白，最多 16 字符
func NormalizeUserName(value string) string {
	return TruncateRunes(strings.TrimSpace(stripControl(value)), MaxUserNameRunes)
}

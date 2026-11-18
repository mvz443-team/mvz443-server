package roomatom

// TEMP-DESYNC-LOG: 房间级全量诊断日志。
// 每个房间写入 desync-logs/room-<id>.jsonl，定位完成后可整体删除本文件及调用点。

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"mvzserver/constants"
	messages "mvzserver/messages/pb"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const desyncLogDir = "desync-logs"

type desyncLogWriter struct {
	mu     sync.Mutex
	file   *os.File
	buffer *bufio.Writer
	roomID int
}

var desyncLogsMu sync.RWMutex
var desyncLogs = map[int]*desyncLogWriter{}

func startDesyncLog(roomID int) *desyncLogWriter {
	if roomID <= 0 {
		return nil
	}
	if err := os.MkdirAll(desyncLogDir, 0o755); err != nil {
		log.Printf("🔴 创建分叉日志目录失败: %v", err)
		return nil
	}
	path := DesyncLogPath(roomID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		log.Printf("🔴 打开分叉日志失败 %s: %v", path, err)
		return nil
	}
	writer := &desyncLogWriter{
		file:   file,
		buffer: bufio.NewWriterSize(file, 256*1024),
		roomID: roomID,
	}
	desyncLogsMu.Lock()
	if previous := desyncLogs[roomID]; previous != nil {
		previous.flush()
		_ = previous.file.Close()
	}
	desyncLogs[roomID] = writer
	desyncLogsMu.Unlock()
	log.Printf("🟢 分叉诊断日志已开启: %s", path)
	writer.write("log_start", map[string]any{
		"roomId": roomID,
		"path":   path,
	})
	return writer
}

func stopDesyncLog(roomID int) {
	desyncLogsMu.Lock()
	writer := desyncLogs[roomID]
	delete(desyncLogs, roomID)
	desyncLogsMu.Unlock()
	if writer == nil {
		return
	}
	writer.write("log_stop", map[string]any{"roomId": roomID})
	writer.flush()
	writer.mu.Lock()
	if writer.file != nil {
		_ = writer.file.Close()
		writer.file = nil
	}
	writer.mu.Unlock()
}

func recordDesyncEvent(roomID int, event string, fields map[string]any) {
	desyncLogsMu.RLock()
	writer := desyncLogs[roomID]
	desyncLogsMu.RUnlock()
	if writer != nil {
		writer.write(event, fields)
	}
}

// shouldLogFrameDetail 控制逐帧明细的采样密度：
// 生产帧率（50ms）下全量记录；只有 MVZ_DEBUG_FRAME_MS 压到 2ms 的压测模式
// 才采样（前 60 帧全量，之后每 50 帧一次），避免日志拖慢权威循环。
// 有实际操作、hash 上报或分叉事件的帧不受此限制，始终完整记录。
func shouldLogFrameDetail(frameID uint32) bool {
	if constants.FrameIntervalMs >= constants.DefaultFrameIntervalMs {
		return true
	}
	return frameID <= 60 || frameID%50 == 0
}

// shouldLogRequest 决定是否记录一条上行请求的完整 payload。
// 非 blank 请求（种植/铲除/星屑/结算等）数量很少，全部记录；
// blank 心跳在 2ms 帧率下数量巨大，只采样记录。
func shouldLogRequest(request *messages.Request) bool {
	if request == nil {
		return false
	}
	blank, ok := request.Payload.(*messages.Request_Blank)
	if !ok {
		return true
	}
	if blank.Blank == nil {
		return true
	}
	return shouldLogFrameDetail(blank.Blank.FrameId) || blank.Blank.HashFrameId != 0
}

func (writer *desyncLogWriter) write(event string, fields map[string]any) {
	if writer == nil || writer.file == nil {
		return
	}
	record := map[string]any{
		"ts":     time.Now().Format(time.RFC3339Nano),
		"roomId": writer.roomID,
		"event":  event,
	}
	for key, value := range fields {
		record[key] = value
	}
	data, err := json.Marshal(record)
	if err != nil {
		log.Printf("🔴 分叉日志序列化失败 event=%s: %v", event, err)
		return
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.buffer == nil {
		return
	}
	_, _ = writer.buffer.Write(append(data, '\n'))
	if event == "desync_detected" || writer.buffer.Buffered() > 128*1024 {
		_ = writer.buffer.Flush()
	}
}

func (writer *desyncLogWriter) flush() {
	if writer == nil {
		return
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.buffer != nil {
		_ = writer.buffer.Flush()
	}
}

// DesyncLogPath 返回房间诊断日志路径。
func DesyncLogPath(roomID int) string {
	return filepath.Join(desyncLogDir, fmt.Sprintf("room-%d.jsonl", roomID))
}

// ReadDesyncLog 供临时 HTTP 下载接口读取日志。
func ReadDesyncLog(roomID int) ([]byte, error) {
	if roomID <= 0 {
		return nil, fmt.Errorf("invalid room id")
	}
	desyncLogsMu.RLock()
	writer := desyncLogs[roomID]
	desyncLogsMu.RUnlock()
	if writer != nil {
		writer.mu.Lock()
		if writer.buffer != nil {
			_ = writer.buffer.Flush()
		}
		writer.mu.Unlock()
	}
	return os.ReadFile(DesyncLogPath(roomID))
}

func protoJSON(value proto.Message) any {
	if value == nil {
		return nil
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(value)
	if err != nil {
		return map[string]any{"marshalError": err.Error()}
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return string(data)
	}
	return decoded
}

// describeRequest 把任意上行请求转成可读 JSON；blank 心跳也会完整记录。
func describeRequest(request *messages.Request) any {
	if request == nil {
		return nil
	}
	return protoJSON(request)
}

func describeInGameOperation(operation *messages.InGameOperation) any {
	if operation == nil {
		return nil
	}
	return protoJSON(operation)
}

func describeInGameOperations(operations []*messages.InGameOperation) []any {
	result := make([]any, 0, len(operations))
	for _, operation := range operations {
		result = append(result, describeInGameOperation(operation))
	}
	return result
}

// describeRecentFrames 导出分叉点之前最近若干权威帧，便于和客户端逐帧对比。
func (room *Room) describeRecentFrames(upTo uint32, count int) []any {
	if room == nil || len(room.frameHistory) == 0 || count <= 0 {
		return nil
	}
	ids := make([]uint32, 0, len(room.frameHistory))
	for id := range room.frameHistory {
		if id <= upTo {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > count {
		ids = ids[len(ids)-count:]
	}
	result := make([]any, 0, len(ids))
	for _, id := range ids {
		response := room.frameHistory[id]
		if response == nil {
			continue
		}
		result = append(result, map[string]any{
			"frameId":    id,
			"operations": describeInGameOperations(response.Operations),
		})
	}
	return result
}

// playerSyncSnapshot 记录每个玩家当前的帧/ACK/连接状态，用于和客户端日志对齐。
func (room *Room) playerSyncSnapshot() []map[string]any {
	if room == nil {
		return nil
	}
	result := make([]map[string]any, 0)
	room.RoomCtx.Players.Range(func(_ int, player *Player) bool {
		if player == nil || player.Ctx == nil {
			return true
		}
		result = append(result, map[string]any{
			"playerId":          player.Ctx.Id,
			"name":              player.Name,
			"latestFrameId":     player.Ctx.LatestFrameID.Load(),
			"latestAckFrameId":  player.Ctx.LatestAckFrameID.Load(),
			"lastSyncedFrameId": player.LastSyncedFrameID.Load(),
			"state":             player.Ctx.State,
			"stateHashVersion":  player.Ctx.StateHashVersion.Load(),
			"connectionEpoch":   player.ConnectionEpoch,
			"isLoaded":          player.IsLoaded,
			"loadProgress":      player.LoadProgress,
		})
		return true
	})
	sort.Slice(result, func(i, j int) bool {
		return result[i]["playerId"].(int) < result[j]["playerId"].(int)
	})
	return result
}

// formatFrameHashes 按玩家 ID 排序输出，保证两端/两次日志可比较。
func formatFrameHashes(hashes map[int]uint32) string {
	ids := make([]int, 0, len(hashes))
	for id := range hashes {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	result := ""
	for _, id := range ids {
		if result != "" {
			result += ","
		}
		result += strconv.Itoa(id) + ":" + fmt.Sprintf("%08x", hashes[id])
	}
	return result
}

// logStage 记录阶段变化与房间基础信息。
func (room *Room) logStage(event string) {
	if room == nil {
		return
	}
	recordDesyncEvent(room.ID, event, map[string]any{
		"stage":     room.GameStage.Load(),
		"stageName": stageName(room.GameStage.Load()),
		"players":   room.playerSyncSnapshot(),
	})
}

func stageName(stage constants.Stage) string {
	switch stage {
	case constants.STAGE_InLobby:
		return "InLobby"
	case constants.STAGE_Preparing:
		return "Preparing"
	case constants.STAGE_Loading:
		return "Loading"
	case constants.STAGE_InGame:
		return "InGame"
	case constants.STAGE_PostGame:
		return "PostGame"
	case constants.STAGE_CLOSED:
		return "Closed"
	default:
		return "Unknown"
	}
}

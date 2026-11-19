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
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const defaultDesyncLogDir = "desync-logs"

// NetDebugEnabled 由服务器 --netDebug 启动参数开启；开启后每个战局日志都会
// 记录用户命令抵达时的服务端权威帧号，用于和两名客户端日志做离线复现。
var NetDebugEnabled bool

var desyncLogDirMu sync.RWMutex
var desyncLogDirPath = defaultDesyncLogDir
var desyncLogSession = time.Now().Format("20060102T150405.000")
var latestDesyncLogPathMu sync.RWMutex
var latestDesyncLogPath string

const desyncSnapshotFileSuffix = ".desync.json"
const netDebugLogDirName = "netDebug-logs"

func SetDesyncLogDir(path string) {
	desyncLogDirMu.Lock()
	defer desyncLogDirMu.Unlock()
	trimmed := ""
	if path != "" {
		if absolute, err := filepath.Abs(path); err == nil {
			trimmed = absolute
		} else {
			trimmed = path
		}
	}
	if trimmed == "" {
		if cwd, err := os.Getwd(); err == nil {
			trimmed = filepath.Join(cwd, netDebugLogDirName)
		} else {
			trimmed = netDebugLogDirName
		}
	}
	if absolute, err := filepath.Abs(trimmed); err == nil {
		trimmed = absolute
	}
	desyncLogDirPath = trimmed
}

func DesyncLogDir() string {
	desyncLogDirMu.RLock()
	defer desyncLogDirMu.RUnlock()
	return desyncLogDirPath
}

func DesyncLogSession() string {
	return desyncLogSession
}

func currentDesyncLogPath(roomID int) string {
	desyncLogsMu.RLock()
	writer := desyncLogs[roomID]
	desyncLogsMu.RUnlock()
	if writer != nil {
		return writer.path
	}
	latestDesyncLogPathMu.RLock()
	path := latestDesyncLogPath
	latestDesyncLogPathMu.RUnlock()
	if path != "" {
		return path
	}
	return DesyncLogPath(roomID)
}

func flushDesyncLog(roomID int) {
	desyncLogsMu.RLock()
	writer := desyncLogs[roomID]
	desyncLogsMu.RUnlock()
	if writer != nil {
		writer.write("desync_snapshot_saved", nil)
		writer.flush()
	}
}

// writeDesyncSnapshot 在检测到分叉时立即生成独立 JSON 文件。
// 房间级 jsonl 会继续记录，但分叉证据不能等到房间关闭才落盘。
func writeDesyncSnapshot(roomID int, fields map[string]any) string {
	path := currentDesyncLogPath(roomID)
	snapshotPath := strings.TrimSuffix(path, filepath.Ext(path)) + desyncSnapshotFileSuffix
	record := map[string]any{
		"ts":           time.Now().Format(time.RFC3339Nano),
		"type":         "mvz443-server-desync-snapshot",
		"roomId":       roomID,
		"logPath":      path,
		"snapshotPath": snapshotPath,
	}
	for key, value := range fields {
		record[key] = value
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		log.Printf("🔴 分叉快照序列化失败 room=%d: %v", roomID, err)
		return ""
	}
	data = append(data, '\n')
	if err := os.WriteFile(snapshotPath, data, 0o644); err != nil {
		log.Printf("🔴 分叉快照落盘失败 room=%d path=%s: %v", roomID, snapshotPath, err)
		return ""
	}
	if file, err := os.OpenFile(snapshotPath, os.O_WRONLY, 0); err == nil {
		_ = file.Sync()
		_ = file.Close()
	}
	log.Printf("💾 分叉快照已保存 room=%d path=%s", roomID, snapshotPath)
	return snapshotPath
}

type desyncLogWriter struct {
	mu     sync.Mutex
	file   *os.File
	buffer *bufio.Writer
	roomID int
	path   string
}

var desyncLogsMu sync.RWMutex
var desyncLogs = map[int]*desyncLogWriter{}

func startDesyncLog(roomID int) *desyncLogWriter {
	if roomID <= 0 {
		return nil
	}
	dir := DesyncLogDir()
	if dir == "" {
		executable, err := os.Executable()
		if err == nil {
			dir = filepath.Join(filepath.Dir(executable), defaultDesyncLogDir)
		} else {
			dir = defaultDesyncLogDir
		}
		SetDesyncLogDir(dir)
	}
	if absolute, err := filepath.Abs(dir); err == nil {
		dir = absolute
		SetDesyncLogDir(dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("🔴 创建分叉日志目录失败: %v", err)
		return nil
	}
	path := DesyncLogPath(roomID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if os.IsExist(err) {
		// 同一进程内房间 id 会复用；每局独立文件名，绝不覆盖上一局。
		path = fmt.Sprintf("%s.%d.jsonl", filepath.Join(dir, fmt.Sprintf("room-%d", roomID)), time.Now().UnixNano())
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	}
	if err != nil {
		log.Printf("🔴 打开分叉日志失败 %s: %v", path, err)
		return nil
	}
	writer := &desyncLogWriter{
		file:   file,
		buffer: bufio.NewWriterSize(file, 256*1024),
		roomID: roomID,
		path:   path,
	}
	desyncLogsMu.Lock()
	if previous := desyncLogs[roomID]; previous != nil {
		previous.flush()
		_ = previous.file.Close()
	}
	desyncLogs[roomID] = writer
	desyncLogsMu.Unlock()
	latestDesyncLogPathMu.Lock()
	latestDesyncLogPath = path
	latestDesyncLogPathMu.Unlock()
	log.Printf("🟢 分叉诊断日志已开启 room=%d netDebug=%v path=%s", roomID, NetDebugEnabled, path)
	record := map[string]any{
		"roomId":   roomID,
		"path":     path,
		"netDebug": NetDebugEnabled,
	}
	if NetDebugEnabled {
		record["note"] = "all requests and rewritten execution frames are logged"
	}
	writer.write("log_start", record)
	writer.flush()
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
	log.Printf("🟢 分叉诊断日志已关闭 room=%d path=%s", roomID, writer.path)
	writer.mu.Lock()
	if writer.file != nil {
		_ = writer.file.Close()
		writer.file = nil
	}
	writer.mu.Unlock()
}

// finishDesyncLog 在战局结束时落一条终态事件并立即刷盘，但不关闭日志：
// PostGame 宽限期内仍可能有补发命令，日志需要继续记录到房间真正结束。
func finishDesyncLog(roomID int, event string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["roomId"] = roomID
	recordDesyncEvent(roomID, event, fields)
	desyncLogsMu.RLock()
	writer := desyncLogs[roomID]
	desyncLogsMu.RUnlock()
	if writer != nil {
		writer.flush()
	}
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
	if NetDebugEnabled {
		return true
	}
	return shouldLogFrameDetail(blank.Blank.FrameId) || blank.Blank.HashFrameId != 0
}

// recordOperationExecutionFrame 在服务端改写 process_frame_id 后记录目标执行帧。
// 高延迟玩家上传的期望帧会被夹到当前窗口；离线复现必须按改写后的帧号注入
// 命令，否则无法还原实际联机顺序。
func recordOperationExecutionFrame(room *Room, player *Player, kind string, base *messages.RequestGridOperation, effectiveFrameID, requestedFrameID uint32) {
	if room == nil || base == nil {
		return
	}
	base.ProcessFrameId = effectiveFrameID
	recordDesyncEvent(room.ID, "operation_execution_frame", map[string]any{
		"playerId":                 playerIDOrZero(player),
		"kind":                     kind,
		"clientOpSeq":              base.ClientOpSeq,
		"requestedProcessFrameId":  requestedFrameID,
		"effectiveProcessFrameId":  effectiveFrameID,
		"receivedServerFrameId":    room.currentServerFrameID(),
		"receivedCommittedFrameId": room.committedFrameID(),
	})
}

func playerIDOrZero(player *Player) int {
	if player == nil {
		return 0
	}
	return player.GetID()
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
	if writer.file != nil {
		_ = writer.file.Sync()
	}
}

// DesyncLogPath 返回房间诊断日志路径。
func DesyncLogPath(roomID int) string {
	return filepath.Join(DesyncLogDir(), fmt.Sprintf("room-%d-%s.jsonl", roomID, desyncLogSession))
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
		if writer.file != nil {
			_ = writer.file.Sync()
		}
		writer.mu.Unlock()
	}
	if writer != nil {
		return os.ReadFile(writer.path)
	}
	latestDesyncLogPathMu.RLock()
	path := latestDesyncLogPath
	latestDesyncLogPathMu.RUnlock()
	if path != "" {
		return os.ReadFile(path)
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

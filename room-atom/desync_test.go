package roomatom

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"mvzserver/clients"
	messages "mvzserver/messages/pb"
	"testing"
)

func testHashPlayer(id int) *Player {
	ctx := &clients.ClientCtx{}
	ctx.Id = id
	return &Player{Ctx: ctx}
}

func TestStateHashMismatchIsDetected(t *testing.T) {
	room := newTestRoom(21)
	room.FrameHashes = make(map[uint32]map[int]uint32)
	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	// 只有双方 ACK 的帧才会被比较：这里把 ACK 推到 21。
	p1.Ctx.LatestAckFrameID.Store(21)
	p2.Ctx.LatestAckFrameID.Store(21)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.recordStateHash(p1, 20, 1111)
	if room.DesyncDetected {
		t.Fatal("只有一端上报时不应判定分叉")
	}
	room.recordStateHash(p2, 20, 1111)
	if room.DesyncDetected {
		t.Fatal("两端摘要一致时不应判定分叉")
	}
	room.recordStateHash(p2, 21, 2222)
	room.recordStateHash(p1, 21, 3333)
	if !room.DesyncDetected {
		t.Fatal("同一帧摘要不一致必须判定分叉")
	}
}

func TestStateHashMismatchWritesSnapshotImmediately(t *testing.T) {
	dir := t.TempDir()
	SetDesyncLogDir(dir)
	t.Cleanup(func() { SetDesyncLogDir("") })

	room := newTestRoom(22)
	room.ID = 22
	writer := startDesyncLog(room.ID)
	if writer == nil {
		t.Fatal("测试环境必须能打开房间诊断日志")
	}
	t.Cleanup(func() {
		flushDesyncLog(room.ID)
		stopDesyncLog(room.ID)
	})
	room.FrameHashes = make(map[uint32]map[int]uint32)
	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	p1.Ctx.LatestAckFrameID.Store(22)
	p2.Ctx.LatestAckFrameID.Store(22)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.recordStateHash(p1, 22, 1111)
	room.recordStateHash(p2, 22, 2222)
	if !room.DesyncDetected {
		t.Fatal("同一帧摘要不一致必须判定分叉")
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.desync.json"))
	if err != nil {
		t.Fatalf("查找分叉快照失败: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("分叉时必须立即落盘独立快照文件")
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("读取分叉快照失败: %v", err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("分叉快照必须是合法 JSON: %v", err)
	}
	if snapshot["frameId"] != float64(22) {
		t.Fatalf("快照帧号不正确: %v", snapshot["frameId"])
	}
	logData, err := os.ReadFile(currentDesyncLogPath(room.ID))
	if err != nil {
		t.Fatalf("读取房间日志失败: %v", err)
	}
	if !strings.Contains(string(logData), "\"event\":\"desync_snapshot_saved\"") {
		t.Fatal("分叉快照事件必须写入房间日志")
	}
}

func TestWriteDesyncSnapshotCreatesMissingDirectory(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "missing", "netDebug-logs")
	SetDesyncLogDir(dir)
	t.Cleanup(func() { SetDesyncLogDir("") })

	latestDesyncLogPathMu.Lock()
	previous := latestDesyncLogPath
	latestDesyncLogPath = filepath.Join(dir, "room-0-session.jsonl")
	latestDesyncLogPathMu.Unlock()
	t.Cleanup(func() {
		latestDesyncLogPathMu.Lock()
		latestDesyncLogPath = previous
		latestDesyncLogPathMu.Unlock()
	})

	path := writeDesyncSnapshot(0, map[string]any{"frameId": 1})
	if path == "" {
		t.Fatal("分叉快照目录不存在时必须自动创建并落盘")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("分叉快照文件未生成: %v", err)
	}
}

func TestNetDebugLogsEveryRequest(t *testing.T) {
	dir := t.TempDir()
	SetDesyncLogDir(dir)
	previous := NetDebugEnabled
	NetDebugEnabled = false
	t.Cleanup(func() {
		SetDesyncLogDir("")
		NetDebugEnabled = previous
	})

	room := newTestRoom(23)
	room.ID = 23
	writer := startDesyncLog(room.ID)
	if writer == nil {
		t.Fatal("测试环境必须能打开房间诊断日志")
	}
	t.Cleanup(func() {
		flushDesyncLog(room.ID)
		stopDesyncLog(room.ID)
	})

	NetDebugEnabled = true
	if !shouldLogRequest(&messages.Request{
		Payload: &messages.Request_Blank{Blank: &messages.RequestBlank{FrameId: 5000}},
	}) {
		t.Fatal("开启 netDebug 时必须记录每个用户请求的服务端帧号")
	}
}

func TestStateHashPruneKeepsMapBounded(t *testing.T) {
	room := newTestRoom(0)
	room.FrameHashes = make(map[uint32]map[int]uint32)
	for frame := uint32(1); frame <= 200; frame++ {
		room.recordStateHash(testHashPlayer(100), frame, frame)
	}
	if len(room.FrameHashes) > 64 {
		t.Fatalf("摘要表必须保持有界，当前 %d 项", len(room.FrameHashes))
	}
}

func TestHashSchemaVersionMismatchSkipsDesyncDetection(t *testing.T) {
	room := newTestRoom(0)
	room.FrameHashes = make(map[uint32]map[int]uint32)

	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	p1.Ctx.LatestFrameID.Store(1)
	p2.Ctx.LatestFrameID.Store(1)
	p1.Ctx.StateHashVersion.Store(2)
	p2.Ctx.StateHashVersion.Store(0)
	p1.Ctx.StateHashVersionReported.Store(true)
	p2.Ctx.StateHashVersionReported.Store(true)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.checkHashSchemaVersions()
	if !room.HashSchemaMismatch {
		t.Fatal("新旧客户端哈希口径不同时必须标记版本不一致")
	}

	room.recordStateHash(p1, 20, 1111)
	room.recordStateHash(p2, 20, 2222)
	if room.DesyncDetected {
		t.Fatal("版本不一致时不能再误报为游戏状态分叉")
	}
}

func TestHashSchemaSameVersionKeepsDesyncDetection(t *testing.T) {
	room := newTestRoom(20)
	room.FrameHashes = make(map[uint32]map[int]uint32)

	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	p1.Ctx.LatestFrameID.Store(1)
	p2.Ctx.LatestFrameID.Store(1)
	p1.Ctx.LatestAckFrameID.Store(20)
	p2.Ctx.LatestAckFrameID.Store(20)
	p1.Ctx.StateHashVersion.Store(2)
	p2.Ctx.StateHashVersion.Store(2)
	p1.Ctx.StateHashVersionReported.Store(true)
	p2.Ctx.StateHashVersionReported.Store(true)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.checkHashSchemaVersions()
	if room.HashSchemaMismatch {
		t.Fatal("相同哈希口径不应标记版本不一致")
	}

	room.recordStateHash(p1, 20, 1111)
	room.recordStateHash(p2, 20, 2222)
	if !room.DesyncDetected {
		t.Fatal("版本一致时仍必须检测真实状态分叉")
	}
}

func TestAcceptInGameBaseDetectsHashSchemaVersionMismatch(t *testing.T) {
	room := lifecycleTestRoom(0)
	p1 := testHashPlayer(100)
	p2 := testHashPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	if ok := room.acceptInGameBase(p1, &messages.RequestBlank{
		FrameId:          1,
		AckFrameId:       0,
		StateHashVersion: 2,
	}); !ok {
		t.Fatal("第一端心跳应被接受")
	}
	if ok := room.acceptInGameBase(p2, &messages.RequestBlank{
		FrameId:          1,
		AckFrameId:       0,
		StateHashVersion: 0,
	}); !ok {
		t.Fatal("第二端心跳应被接受")
	}
	if !room.HashSchemaMismatch {
		t.Fatal("心跳版本不一致时必须标记 HashSchemaMismatch")
	}
}

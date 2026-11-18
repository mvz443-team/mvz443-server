package roomatom

import (
	messages "mvzserver/messages/pb"
	"testing"
)

func testCardPlantOperation(frameID, pid, col, row, uid uint32) *messages.InGameOperation {
	return &messages.InGameOperation{
		ProcessFrameId: frameID,
		Payload: &messages.InGameOperation_CardPlant{
			CardPlant: &messages.ResponseCardPlant{
				Pid: pid,
				Base: &messages.ResponseGridOperation{
					Uid: uid,
					Col: col,
					Row: row,
				},
			},
		},
	}
}

// 已生成的权威帧必须不可变：调用方仍持有 operation 指针时，
// 任何后续修改都不能回溯改写 frameHistory。
func TestGeneratedFrameIsImmutable(t *testing.T) {
	room := lifecycleTestRoom(0)
	op := testCardPlantOperation(1, 2, 2, 0, 101)
	room.recordOperationSubmission(1, op)
	room.advanceOneFrame()

	history := room.frameHistory[1]
	if history == nil || len(history.Operations) != 1 {
		t.Fatalf("frame 1 应包含 1 个操作，实际 %+v", history)
	}
	if history.Operations[0] == op {
		t.Fatal("frameHistory 必须保存操作副本，不能与提交方共享指针")
	}

	op.ProcessFrameId = 99
	op.OperationIndex = 7
	op.Payload.(*messages.InGameOperation_CardPlant).CardPlant.Pid = 5

	if history.Operations[0].ProcessFrameId != 1 {
		t.Fatalf("历史帧 processFrameId 被回溯改写为 %d", history.Operations[0].ProcessFrameId)
	}
	if got := history.Operations[0].Payload.(*messages.InGameOperation_CardPlant).CardPlant.Pid; got != 2 {
		t.Fatalf("历史帧操作内容被回溯改写，pid=%d", got)
	}
}

// 帧一旦生成，迟到的提交必须被拒绝，而不是落进一个永远不会再推进的槽位。
func TestSubmissionToGeneratedOrCurrentFrameRejected(t *testing.T) {
	room := lifecycleTestRoom(5)

	room.recordOperationSubmission(5, testCardPlantOperation(5, 2, 2, 0, 101))
	if _, exists := room.scheduledFrames[5]; exists {
		t.Fatal("已生成帧不得接受新操作")
	}

	room.recordOperationSubmission(6, testCardPlantOperation(6, 2, 2, 0, 101))
	if _, exists := room.scheduledFrames[6]; !exists {
		t.Fatal("未来帧必须正常接受操作")
	}
}

// 未 committed（双方未 ACK）的摘要不得提前参与分叉判定。
func TestStateHashNotComparedBeforeBothAck(t *testing.T) {
	room := lifecycleTestRoom(1)
	p1 := connectedPlayer(100)
	p2 := connectedPlayer(101)
	room.RoomCtx.Players.Store(100, p1)
	room.RoomCtx.Players.Store(101, p2)

	room.recordStateHash(p1, 1, 0x1111)
	room.recordStateHash(p2, 1, 0x2222)
	if room.DesyncDetected {
		t.Fatal("双方 ACK 之前不得判定分叉")
	}

	p1.Ctx.LatestAckFrameID.Store(1)
	p2.Ctx.LatestAckFrameID.Store(1)
	room.evaluateCommittedStateHashes(room.committedFrameID())
	if !room.DesyncDetected {
		t.Fatal("committed 后必须比较该帧摘要")
	}
}

package roomatom

import (
	messages "mvzserver/messages/pb"
	"sync/atomic"
	"testing"
)

func newTestRoom(currentFrame uint32) *Room {
	nextFrame := &atomic.Uint32{}
	nextFrame.Store(currentFrame + 1)
	return &Room{RoomCtx: &RoomCtx{NextFrameID: nextFrame}}
}

func TestResolveProcessFrameIDWindow(t *testing.T) {
	room := newTestRoom(10)

	frameID, ok := room.resolveProcessFrameID(&messages.RequestGridOperation{ProcessFrameId: 0})
	if !ok || frameID != 14 {
		t.Fatalf("过早的操作应被抬升到 current+4，得到 frameID=%d ok=%v", frameID, ok)
	}

	frameID, ok = room.resolveProcessFrameID(&messages.RequestGridOperation{ProcessFrameId: 18})
	if !ok || frameID != 18 {
		t.Fatalf("窗口内的操作应被保留，得到 frameID=%d ok=%v", frameID, ok)
	}

	_, ok = room.resolveProcessFrameID(&messages.RequestGridOperation{ProcessFrameId: 1_000_000})
	if ok {
		t.Fatal("过远未来帧必须被拒绝")
	}
}

func TestErrorOperationSignatureIncludesSequence(t *testing.T) {
	room := newTestRoom(0)
	makeError := func(seq uint32) *messages.InGameOperation {
		return &messages.InGameOperation{
			ProcessFrameId: 4,
			Payload: &messages.InGameOperation_Error{
				Error: &messages.ResponseErrorMessage{
					Uid:         100,
					ClientOpSeq: seq,
					Col:         1,
					Row:         2,
				},
			},
		}
	}

	if room.operationSignature(makeError(1)) == room.operationSignature(makeError(2)) {
		t.Fatal("不同 client_op_seq 的错误操作必须拥有不同签名")
	}
}

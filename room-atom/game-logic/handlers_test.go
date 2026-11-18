package gamelogic

import (
	"mvzserver/clients"
	messages "mvzserver/messages/pb"
	"testing"
)

func testPlayer() *clients.Player {
	ctx := &clients.ClientCtx{}
	ctx.Id = 100
	return &clients.Player{Ctx: ctx}
}

func TestGridConflictReturnsErrorOperation(t *testing.T) {
	logic := NewGameLogic(nil)
	player := testPlayer()
	base := &messages.RequestGridOperation{
		Col: 1, Row: 1, ProcessFrameId: 4, ClientOpSeq: 7,
	}

	first := logic.PlantCard(player, &messages.RequestCardPlant{Base: base, Pid: 1, Level: 1})
	if first == nil {
		t.Fatal("首次种植应成功")
	}

	second := logic.PlantCard(player, &messages.RequestCardPlant{Base: base, Pid: 2, Level: 1})
	if second == nil {
		t.Fatal("同帧同格冲突必须返回错误操作")
	}
	rejection, ok := second.Payload.(*messages.InGameOperation_Error)
	if !ok {
		t.Fatalf("冲突结果应为 error 操作，实际为 %T", second.Payload)
	}
	if rejection.Error.ClientOpSeq != 7 || rejection.Error.Col != 1 || rejection.Error.Row != 1 {
		t.Fatalf("错误回包必须携带请求定位信息，得到 %+v", rejection.Error)
	}
}

func TestAcceptedOperationEchoesClientOpSeq(t *testing.T) {
	logic := NewGameLogic(nil)
	player := testPlayer()
	base := &messages.RequestGridOperation{
		Col: 1, Row: 1, ProcessFrameId: 4, ClientOpSeq: 7,
	}

	plant := logic.PlantCard(player, &messages.RequestCardPlant{Base: base, Pid: 1, Level: 1})
	if plant == nil || plant.ResponseClientOpSeq != 7 {
		t.Fatalf("种植成功回包必须回传 client_op_seq，得到 %+v", plant)
	}

	removeBase := &messages.RequestGridOperation{
		Col: 2, Row: 1, ProcessFrameId: 4, ClientOpSeq: 8,
	}
	remove := logic.RemoveCard(player, &messages.RequestRemovePlant{Base: removeBase, Pid: 1})
	if remove == nil || remove.ResponseClientOpSeq != 8 {
		t.Fatalf("移除成功回包必须回传 client_op_seq，得到 %+v", remove)
	}

	starBase := &messages.RequestGridOperation{
		Col: 3, Row: 1, ProcessFrameId: 4, ClientOpSeq: 9,
	}
	star := logic.UseStarShards(player, &messages.RequestStarShards{Base: starBase, Pid: 1, Cost: 1})
	if star == nil || star.ResponseClientOpSeq != 9 {
		t.Fatalf("星之碎片成功回包必须回传 client_op_seq，得到 %+v", star)
	}
}

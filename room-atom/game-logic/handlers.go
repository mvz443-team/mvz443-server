package gamelogic

import (
	"mvzserver/clients"
	messages "mvzserver/messages/pb"
)

func invalidGridOperation(player *clients.Player, request *messages.RequestGridOperation, reason string) *messages.InGameOperation {
	return &messages.InGameOperation{
		ProcessFrameId: request.ProcessFrameId,
		Payload: &messages.InGameOperation_Error{
			Error: &messages.ResponseErrorMessage{
				Message:        reason,
				Uid:            uint32(player.GetID()),
				ClientOpSeq:    request.ClientOpSeq,
				Col:            request.Col,
				Row:            request.Row,
				ProcessFrameId: request.ProcessFrameId,
			},
		},
	}
}

func (g *GameLogic) PlantCard(player *clients.Player, request *messages.RequestCardPlant) *messages.InGameOperation {
	gridOp := &messages.ResponseGridOperation{
		Uid: uint32(player.GetID()),
		Col: request.Base.Col,
		Row: request.Base.Row,
	}
	plantOp := &messages.ResponseCardPlant{
		Pid:   request.Pid,
		Level: request.Level,
		Cost:  request.Cost,
		Base:  gridOp,
	}

	if request.Base.Col < 0 || request.Base.Col >= MAXCOLS ||
		request.Base.Row < 0 || request.Base.Row >= MAXROWS {
		return invalidGridOperation(player, request.Base, "目标格子超出战场范围")
	}
	grid := g.getFrameGrid(request.Base.ProcessFrameId)
	if grid[request.Base.Row][request.Base.Col] {
		return invalidGridOperation(player, request.Base, "该格子在目标帧已被其他操作占用")
	}

	grid[request.Base.Row][request.Base.Col] = true

	return &messages.InGameOperation{
		Payload: &messages.InGameOperation_CardPlant{
			CardPlant: plantOp,
		},
		ProcessFrameId: request.Base.ProcessFrameId,
	}
}

func (g *GameLogic) RemoveCard(player *clients.Player, request *messages.RequestRemovePlant) *messages.InGameOperation {
	gridOp := &messages.ResponseGridOperation{
		Uid: uint32(player.GetID()),
		Col: request.Base.Col,
		Row: request.Base.Row,
	}
	removeOp := &messages.ResponseRemovePlant{
		Pid:  request.Pid,
		Base: gridOp,
	}

	if request.Base.Col < 0 || request.Base.Col >= MAXCOLS ||
		request.Base.Row < 0 || request.Base.Row >= MAXROWS {
		return invalidGridOperation(player, request.Base, "目标格子超出战场范围")
	}

	grid := g.getFrameGrid(request.Base.ProcessFrameId)
	if grid[request.Base.Row][request.Base.Col] {
		return invalidGridOperation(player, request.Base, "该格子在目标帧已被其他操作占用")
	}

	grid[request.Base.Row][request.Base.Col] = true
	return &messages.InGameOperation{
		Payload: &messages.InGameOperation_RemovePlant{
			RemovePlant: removeOp,
		},
		ProcessFrameId: request.Base.ProcessFrameId,
	}
}

func (g *GameLogic) UseStarShards(player *clients.Player, request *messages.RequestStarShards) *messages.InGameOperation {
	gridOp := &messages.ResponseGridOperation{
		Uid: uint32(player.GetID()),
		Col: request.Base.Col,
		Row: request.Base.Row,
	}
	starshardsOp := &messages.ResponseUseStarShards{
		Pid:  request.Pid,
		Cost: request.Cost,
		Base: gridOp,
	}

	if request.Base.Col < 0 || request.Base.Col >= MAXCOLS ||
		request.Base.Row < 0 || request.Base.Row >= MAXROWS {
		return invalidGridOperation(player, request.Base, "目标格子超出战场范围")
	}

	grid := g.getFrameGrid(request.Base.ProcessFrameId)
	if grid[request.Base.Row][request.Base.Col] {
		return invalidGridOperation(player, request.Base, "该格子在目标帧已被其他操作占用")
	}

	grid[request.Base.Row][request.Base.Col] = true

	return &messages.InGameOperation{
		Payload: &messages.InGameOperation_UseStarShards{
			UseStarShards: starshardsOp,
		},
		ProcessFrameId: request.Base.ProcessFrameId,
	}
}

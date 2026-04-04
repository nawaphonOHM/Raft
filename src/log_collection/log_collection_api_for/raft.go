package log_collection_api_for

import (
	"raft/src/dto"
	"raft/src/log_collection/talk_to"
)

type Raft interface {
	Add(log talk_to.Log)
	Validate(candidateTerm int, index int) (bool, error)
	GetLatestLogIndexMinusOneAndLatestTermMinusOne() *dto.LatestLogIndexMinusOneAndLatestLogMinusOne
	SubscribeLogChange(channel interface{}, logSize int) bool
}

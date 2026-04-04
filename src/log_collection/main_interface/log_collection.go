package main_interface

import (
	"raft/src/dto"
	"raft/src/log_collection/talk_to"
)

type LogCollection interface {
	LogAt(index int) talk_to.Log
	Validate(candidateTerm int, index int) (bool, error)
	Add(newLog talk_to.Log)
	SubscribeLogChange(channel interface{}, logSize int) bool
	Size() int
	GetLatestLogIndexMinusOneAndLatestTermMinusOne() *dto.LatestLogIndexMinusOneAndLatestLogMinusOne
}

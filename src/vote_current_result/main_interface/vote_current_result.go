package main_interface

import (
	"raft/src/vote_current_result/talk_to"
)

type VoteCurrentResult interface {
	ElectionResult() talk_to.ElectionResult
	AddPoint(numToAdd int)
	SetError(err error)
	KnownResultChannel() <-chan bool
}

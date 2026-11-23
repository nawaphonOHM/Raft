package main_interface

import "raft/src/election_result/election_result_api_for"

type VoteCurrentResult interface {
	ElectionResult() election_result_api_for.VoteCurrentResult
	AddPoint(numToAdd int)
	SetError(err error)
	KnownResultChannel() <-chan bool
}

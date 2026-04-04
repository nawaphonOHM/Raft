package vote_current_result_api_for

import "raft/src/vote_current_result/talk_to"

type ElectionProcess interface {
	ElectionResult() talk_to.ElectionResult
	SetError(err error)
	KnownResultChannel() <-chan bool
}

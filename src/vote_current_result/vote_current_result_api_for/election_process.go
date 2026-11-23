package vote_current_result_api_for

import "raft/src/election_result/election_result_api_for"

type ElectionProcess interface {
	ElectionResult() election_result_api_for.VoteCurrentResult
	SetError(err error)
	KnownResultChannel() <-chan bool
}

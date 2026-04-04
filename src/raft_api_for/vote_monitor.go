package raft_api_for

type VoteMonitor interface {
	SetCurrentTermAndVoteFor(newCurrentTerm int, voteFor ...int)
	CurrentTerm() int
}

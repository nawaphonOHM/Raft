package raft_api_for

import "raft/src/channel_envelop/channel_envelop_api_for"

type VoteMonitor interface {
	SetCurrentTermAndVoteFor(newCurrentTerm int, voteFor ...int)
	CurrentTerm() int
	SubscribeCurrentTermChange(subObj channel_envelop_api_for.Raft, expectedTermNumber int) bool
}

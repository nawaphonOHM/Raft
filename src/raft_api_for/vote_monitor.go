package raft_api_for

import "raft/src/raft_talk_to"

type VoteMonitor interface {
	SetCurrentTermAndVoteFor(newCurrentTerm int, voteFor ...int)
	CurrentTerm() int
	SubscribeCurrentTermChange(subObj raft_talk_to.ChannelEnvelop, expectedTermNumber int) bool
}

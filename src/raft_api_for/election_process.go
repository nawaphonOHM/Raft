package raft_api_for

import (
	"raft/src/raft_talk_to"
	"raft/src/raft_type"
)

type ElectionProcess interface {
	SubscribeStateChange(subObj raft_talk_to.LimitedTimeUsageChannel, expectedState raft_type.State) bool
	SetNextIndexAndMatchIndex(nextIndex []int, matchIndex []int)
	SetState(newState raft_type.State)
}

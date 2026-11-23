package raft_api_for

import (
	"raft/src/channel_envelop/channel_envelop_api_for"
	"raft/src/raft_type"
)

type ElectionProcess interface {
	SubscribeStateChange(subObj channel_envelop_api_for.Raft, expectedState raft_type.State) bool
	SetNextIndexAndMatchIndex(nextIndex []int, matchIndex []int)
	SetState(newState raft_type.State)
}

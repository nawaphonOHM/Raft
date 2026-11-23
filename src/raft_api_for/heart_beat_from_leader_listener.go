package raft_api_for

import (
	"raft/src/channel_envelop/channel_envelop_api_for"
	"raft/src/raft_type"
)

type HeartBeatFromLeaderListener interface {
	SubscribeCurrentTermChange(subObj channel_envelop_api_for.Raft, expectedTermNumber int) bool
	SubscribeStateChange(subObj channel_envelop_api_for.Raft, expectedState raft_type.State) bool
	SetState(newState raft_type.State)
}

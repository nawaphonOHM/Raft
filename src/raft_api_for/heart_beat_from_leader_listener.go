package raft_api_for

import (
	"raft/src/raft_talk_to"
	"raft/src/raft_type"
)

type HeartBeatFromLeaderListener interface {
	SubscribeCurrentTermChange(subObj raft_talk_to.ChannelEnvelop, expectedTermNumber int) bool
	SubscribeStateChange(subObj raft_talk_to.ChannelEnvelop, expectedState raft_type.State) bool
	SetState(newState raft_type.State)
}

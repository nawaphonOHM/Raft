package dto

import "raft/src/raft_type"

type VoteCommand struct {
	Point            int
	ChannelOperation raft_type.ChannelOperation
	Sender           string
}

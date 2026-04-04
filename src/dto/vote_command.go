package dto

import "raft/src/raft_type"

type VoteCommand struct {
	Point            int
	ChannelOperation raft_type.ChannelOperation
	Sender           string
}

func NewVoteCommand(point int, channelOperation raft_type.ChannelOperation, sender string) *VoteCommand {
	return &VoteCommand{Point: point, ChannelOperation: channelOperation, Sender: sender}
}

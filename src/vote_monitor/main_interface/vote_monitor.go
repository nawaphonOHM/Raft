package main_interface

import (
	"context"

	"raft/src/dto"
	"raft/src/vote_monitor/talk_to"
)

type VoteMonitor interface {
	Start()
	SetApproveChannel(channel talk_to.LimitedTimeUsageChannel)
	SetTheContext(theContext context.Context)
	SetRequestVoteArgs(requestParam *dto.RequestVoteArgs)
}

package main_interface

import (
	"context"

	"raft/src/dto"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
)

type VoteMonitor interface {
	Start()
	SetApproveChannel(channel limited_time_usage_channel_api_for.VoteMonitor)
	SetTheContext(theContext context.Context)
	SetRequestVoteArgs(requestParam *dto.RequestVoteArgs)
}

package vote_monitor_api_for

import (
	"context"

	"raft/src/dto"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
)

type ElectionProcess interface {
	SetApproveChannel(channel limited_time_usage_channel_api_for.VoteMonitor)
	Start()
	SetRequestVoteArgs(requestParam *dto.RequestVoteArgs)
	SetTheContext(theContext context.Context)
}

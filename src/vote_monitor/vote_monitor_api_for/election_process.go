package vote_monitor_api_for

import (
	"context"

	"raft/src/dto"
	"raft/src/vote_monitor/talk_to"
)

type ElectionProcess interface {
	SetApproveChannel(channel talk_to.LimitedTimeUsageChannel)
	Start()
	SetRequestVoteArgs(requestParam *dto.RequestVoteArgs)
	SetTheContext(theContext context.Context)
}

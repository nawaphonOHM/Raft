package election_consideration_builder_api_for

import (
	"context"

	"raft/src/election_consideration/builder/election_consideration_builder_talk_to"
)

type ElectionProcess interface {
	SetContext(context context.Context)
	SetApproveChannel(ch election_consideration_builder_talk_to.LimitedTimeUsageChannel)
	Build() election_consideration_builder_talk_to.ElectionConsideration
}

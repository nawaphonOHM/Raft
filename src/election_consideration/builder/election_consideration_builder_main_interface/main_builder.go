package election_consideration_builder_main_interface

import (
	"context"

	"raft/src/election_consideration/builder/election_consideration_builder_talk_to"
)

type Builder interface {
	Build() election_consideration_builder_talk_to.ElectionConsideration
	SetApproveChannel(ch election_consideration_builder_talk_to.LimitedTimeUsageChannel)
	SetContext(context context.Context)
}

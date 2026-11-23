package election_consideration_builder_api_for

import (
	"context"

	"raft/src/election_consideration/election_consideration_api_for"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
)

type ElectionProcess interface {
	SetContext(context context.Context)
	SetApproveChannel(ch limited_time_usage_channel_api_for.ElectionConsideration)
	Build() election_consideration_api_for.ElectionConsiderationBuilder
}

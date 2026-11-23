package election_consideration_builder_main_interface

import (
	"context"

	"raft/src/election_consideration/election_consideration_api_for"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
)

type Builder interface {
	Build() election_consideration_api_for.ElectionConsiderationBuilder
	SetApproveChannel(ch limited_time_usage_channel_api_for.ElectionConsideration)
	SetContext(context context.Context)
}

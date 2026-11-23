package election_consideration_builder_api_for

import (
	"context"
	"log"

	"raft/src/election_consideration"
	"raft/src/election_consideration/builder/election_consideration_builder_main_interface"
	"raft/src/election_consideration/election_consideration_api_for"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
)

type builder struct {
	voteCurrentResult any

	approveChannel limited_time_usage_channel_api_for.ElectionConsideration
	context        context.Context
}

func (b *builder) Build() election_consideration_api_for.ElectionConsiderationBuilder {

	if b.approveChannel == nil {
		log.Fatalln("approve channel is nil")
	}

	if b.context == nil {
		log.Fatalln("context is nil")
	}

	defer b.clearAfterBuild()

	return election_consideration.NewElectionConsideration(
		b.approveChannel,
		b.context,
		b.voteCurrentResult,
	)
}

func (b *builder) clearAfterBuild() {
	b.approveChannel = nil
	b.context = nil
}

func (b *builder) SetApproveChannel(ch limited_time_usage_channel_api_for.ElectionConsideration) {
	b.approveChannel = ch
}

func (b *builder) SetContext(context context.Context) {
	b.context = context
}

func NewBuilder(voteCurrentResult any) election_consideration_builder_main_interface.Builder {
	return &builder{voteCurrentResult: voteCurrentResult}
}

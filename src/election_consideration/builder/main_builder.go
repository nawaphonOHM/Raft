package election_consideration_builder_api_for

import (
	"context"
	"log"

	"raft/src/election_consideration"
	"raft/src/election_consideration/builder/election_consideration_builder_main_interface"
	"raft/src/election_consideration/builder/election_consideration_builder_talk_to"
)

type builder struct {
	voteCurrentResult interface{}

	approveChannel election_consideration_builder_talk_to.LimitedTimeUsageChannel
	context        context.Context
}

func (b *builder) Build() election_consideration_builder_talk_to.ElectionConsideration {

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

func (b *builder) SetApproveChannel(ch election_consideration_builder_talk_to.LimitedTimeUsageChannel) {
	b.approveChannel = ch
}

func (b *builder) SetContext(context context.Context) {
	b.context = context
}

func NewBuilder(voteCurrentResult interface{}) election_consideration_builder_main_interface.Builder {
	return &builder{voteCurrentResult: voteCurrentResult}
}

package election_consideration

import (
	"context"
	"log"

	"raft/src/dto"
	"raft/src/election_consideration/election_consideration_main_interface"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
	"raft/src/raft_state"
	"raft/src/vote_current_result/vote_current_result_api_for"
)

type electionConsideration struct {
	approveChannel limited_time_usage_channel_api_for.ElectionConsideration

	ctx context.Context

	currentSituation vote_current_result_api_for.ElectionConsideration
}

func (e *electionConsideration) Consider() {

	go func(
		approveChannel limited_time_usage_channel_api_for.ElectionConsideration,
		currentSituation vote_current_result_api_for.ElectionConsideration,
		ctx context.Context,
		self *electionConsideration,
	) {

		newContext, cancel := context.WithCancel(ctx)

		for {
			select {
			case result, open := <-approveChannel.Channel():
				{

					if !open {
						continue
					}

					typedResult := result.(*dto.VoteCommand)

					log.Printf(
						"[electionConsideration %p]: Received data from approve channel which is sent from %v. the data tells to add point = %v, then %v\n",
						self,
						typedResult.Sender,
						typedResult.Point,
						raft_state.ConvertToChannelOperationString(typedResult.ChannelOperation),
					)

					currentSituation.AddPoint(typedResult.Point)

					if typedResult.ChannelOperation == raft_state.Close {
						cancel()
						return

					}

					continue

				}
			case <-newContext.Done():
				{
					cancel()

					log.Printf(
						"[electionConsideration %p]: Received cancel Signal... Stop\n",
						self,
					)

					return
				}
			}
		}
	}(e.approveChannel, e.currentSituation, e.ctx, e)
}

func NewElectionConsideration(
	approveChannel limited_time_usage_channel_api_for.ElectionConsideration,
	ctx context.Context,
	currentSituation any,
) election_consideration_main_interface.ElectionConsideration {

	return &electionConsideration{
		approveChannel:   approveChannel,
		ctx:              ctx,
		currentSituation: currentSituation.(vote_current_result_api_for.ElectionConsideration),
	}
}

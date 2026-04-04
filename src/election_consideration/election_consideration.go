package election_consideration

import (
	"context"
	"log"

	"raft/src/dto"
	"raft/src/election_consideration/election_consideration_main_interface"
	"raft/src/election_consideration/talk_to"
	"raft/src/raft_state"
)

type electionConsideration struct {
	approveChannel talk_to.LimitedTimeUsageChannel

	ctx context.Context

	currentSituation talk_to.VoteCurrentResult
}

func (e *electionConsideration) Consider() {

	go func(
		approveChannel talk_to.LimitedTimeUsageChannel,
		currentSituation talk_to.VoteCurrentResult,
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
	approveChannel talk_to.LimitedTimeUsageChannel,
	ctx context.Context,
	currentSituation interface{},
) election_consideration_main_interface.ElectionConsideration {

	return &electionConsideration{
		approveChannel:   approveChannel,
		ctx:              ctx,
		currentSituation: currentSituation.(talk_to.VoteCurrentResult),
	}
}

package election_process

import (
	"context"
	"errors"
	"log"
	"math/rand"

	"raft/src/change_checker"
	"raft/src/dto"
	"raft/src/election_process/election_process_required_params"
	"raft/src/election_process/main_interface"
	"raft/src/election_process/talk_to"
	customerror "raft/src/errors"
	"raft/src/limited_time_usage_channel"
	"raft/src/raft_state"
)

type electionProcess struct {
	raftParams election_process_required_params.Raft

	voteMonitor []talk_to.VoteMonitor

	voteCurrentResult talk_to.VoteCurrentResult

	electionConsiderationBuilder talk_to.ElectionConsiderationBuilder

	stateChangeChannelObj talk_to.LimitedTimeUsageChannel
}

func (e *electionProcess) StartElection() {

	if ok := e.raftParams.SubscribeStateChange(e.stateChangeChannelObj, raft_state.CANDIDATE); !ok {
		log.Printf("[WorkerId %v][term %v][State: %v]: I think the others is already a leader so I should stop voting\n",
			e.raftParams.WorkerId(),
			e.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(e.raftParams.State()),
		)

		return
	}

	log.Printf("[WorkerId %v][term %v][State: %v]: I'm starting an electionProcess\n",
		e.raftParams.WorkerId(),
		e.raftParams.CurrentTerm(),
		raft_state.ConvertToStateString(e.raftParams.State()),
	)

	e.requestOtherWorkersAcceptLeader()

	if electionResult := e.voteCurrentResult.ElectionResult().(talk_to.ElectionResult); !electionResult.Accept() {
		e.raftParams.SetNextIndexAndMatchIndex(nil, nil)

		if expectedError := customerror.NewTimeOutErrorImplementationForComparingType(); !errors.As(electionResult.Why(), expectedError) {
			log.Printf("[WorkerId %v][term %v][State: %v]: leader denied. I will change to FOLLOWER\n",
				e.raftParams.WorkerId(),
				e.raftParams.CurrentTerm(),
				raft_state.ConvertToStateString(e.raftParams.State()),
			)
			e.raftParams.SetState(raft_state.FOLLOWER)
		} else {
			log.Printf("[WorkerId %v][term %v][State: %v]: I will hold electionProcess next term\n",
				e.raftParams.WorkerId(),
				e.raftParams.CurrentTerm(),
				raft_state.ConvertToStateString(e.raftParams.State()),
			)
		}

		return
	}

	nextIndex := make([]int, e.raftParams.PeersSize())

	for index := 0; index < e.raftParams.NextIndexSize(); index++ {
		nextIndex[index] = e.raftParams.Log().Size()
	}

	matchIndex := make([]int, e.raftParams.PeersSize())

	for index := 0; index < e.raftParams.MatchIndexSize(); index++ {
		matchIndex[index] = 0
	}

	e.raftParams.SetNextIndexAndMatchIndex(nextIndex, matchIndex)

	if !change_checker.IsChange(e.stateChangeChannelObj.Channel()) {
		log.Printf("[WorkerId %v][term %v][State: %v]: leader granted. I will change to LEADER\n",
			e.raftParams.WorkerId(),
			e.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(e.raftParams.State()),
		)

	} else {
		log.Printf("[WorkerId %v][term %v][State: ???]: Someone has send heartbeat already. I'm lost to be a LEADER\n",
			e.raftParams.WorkerId(),
			e.raftParams.CurrentTerm(),
		)
	}
}

func (e *electionProcess) requestOtherWorkersAcceptLeader() {

	theContext, cancelContext := context.WithCancel(context.Background())

	for _, monitor := range e.voteMonitor {
		monitor.SetTheContext(theContext)
	}

	var timeOutSignal chan bool

	timeOutSignal = make(chan bool, 1)

	e.raftParams.Clock().StartNewClockCycle(timeOutSignal)

	if accept := voteItself(); accept {

		if change_checker.IsChange(timeOutSignal) {
			cancelContext()

			e.voteCurrentResult.SetError(customerror.NewTimeOutError())

			return
		}

		if voteFor := e.raftParams.VoteFor(); voteFor != nil && *voteFor != e.raftParams.WorkerId() {
			cancelContext()

			log.Printf("[WorkerId %v][term %v][State: %v]: I have voted the other... Leave\n",
				e.raftParams.WorkerId(),
				e.raftParams.CurrentTerm(),
				raft_state.ConvertToStateString(e.raftParams.State()),
			)

			e.voteCurrentResult.SetError(customerror.NewNoNeedToElectionAnymoreError())

			return
		}

		log.Printf("[WorkerId %v][term %v][State: %v]: vote itself\n",
			e.raftParams.WorkerId(),
			e.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(e.raftParams.State()),
		)

		timeOutSignal = make(chan bool, 1)
		e.raftParams.Clock().StartNewClockCycle(timeOutSignal)

	}

	if change_checker.IsChange(e.stateChangeChannelObj.Channel()) {
		log.Printf("[WorkerId %v][term %v][State: %v]: I think the others is already a leader so I should stop request vote from others\n",
			e.raftParams.WorkerId(),
			e.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(e.raftParams.State()),
		)

		cancelContext()

		e.voteCurrentResult.SetError(customerror.NewNoNeedToElectionAnymoreError())

		return
	}

	var requestVoteArgs *dto.RequestVoteArgs

	{

		var lastLogIndex int
		var lastLogTerm int

		if e.raftParams.Log().Size() == 0 {
			lastLogIndex = -1
			lastLogTerm = 1
		} else {
			lastLogIndex = e.raftParams.Log().Size() - 1

			theLog := e.raftParams.Log().LogAt(lastLogIndex)

			lastLogTerm = theLog.Term()

		}

		requestVoteArgs = &dto.RequestVoteArgs{
			Term:         e.raftParams.CurrentTerm(),
			CandidateId:  e.raftParams.WorkerId(),
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		for _, monitor := range e.voteMonitor {
			monitor.SetRequestVoteArgs(requestVoteArgs)
		}
	}

	channels := make([]talk_to.LimitedTimeUsageChannel, 0)

	for index := 0; index < e.raftParams.PeersSize(); index++ {

		if index == e.raftParams.WorkerId() {
			continue
		}

		log.Printf("[WorkerId %v][term %v][State: %v]: I'm going to request vote from peer %v\n",
			requestVoteArgs.CandidateId,
			requestVoteArgs.Term,
			raft_state.ConvertToStateString(e.raftParams.State()),
			index,
		)

		channel := limited_time_usage_channel.NewLimitedTimeUsageChannel(2)

		channels = append(channels, channel)

		voteMonitor := e.voteMonitor[index]

		voteMonitor.SetApproveChannel(channel)
		voteMonitor.Start()
	}

	for _, channel := range channels {
		e.electionConsiderationBuilder.SetContext(theContext)
		e.electionConsiderationBuilder.SetApproveChannel(channel)
		e.electionConsiderationBuilder.Build().Consider()
	}

	log.Printf(
		"[WorkerId %v][term %v][State: %v]: As I vote my self successfully, so I will doing add 1 point\n",
		e.raftParams.WorkerId(),
		e.raftParams.CurrentTerm(),
		raft_state.ConvertToStateString(e.raftParams.State()),
	)

	channels[rand.Intn(len(channels))].Notify(dto.NewVoteCommand(1, raft_state.NoClose, "MY_SELF"))

	log.Printf(
		"[WorkerId %v][term %v][State: %v]: As I vote my self successfully, so I will doing add 1 point... Done!!!\n",
		e.raftParams.WorkerId(),
		e.raftParams.CurrentTerm(),
		raft_state.ConvertToStateString(e.raftParams.State()),
	)

	for {
		select {
		case <-timeOutSignal:
			{
				log.Printf("[WorkerId %v][term %v][State: %v]: timeout\n",
					e.raftParams.WorkerId(),
					e.raftParams.CurrentTerm(),
					raft_state.ConvertToStateString(e.raftParams.State()),
				)

				cancelContext()

				e.voteCurrentResult.SetError(customerror.NewTimeOutError())

				return
			}
		case _, open := <-e.voteCurrentResult.KnownResultChannel():
			{

				if !open {
					continue
				}

				log.Printf("[WorkerId %v][term %v][State: %v]: The election result is now concluded. Will Stop an election for this term\n",
					e.raftParams.WorkerId(),
					e.raftParams.CurrentTerm(),
					raft_state.ConvertToStateString(e.raftParams.State()),
				)

				cancelContext()

				return

			}
		}
	}

}

func voteItself() bool {
	return true
}

func NewElection(
	raftParams election_process_required_params.Raft,
	voteMonitor []interface{},
	voteCurrentResult talk_to.VoteCurrentResult,
	electionConsiderationBuilder talk_to.ElectionConsiderationBuilder,
) main_interface.ElectionProcess {

	newVoteMonitor := make([]talk_to.VoteMonitor, 0)

	for _, monitor := range voteMonitor {
		newVoteMonitor = append(newVoteMonitor, monitor.(talk_to.VoteMonitor))
	}

	return &electionProcess{
		raftParams:                   raftParams,
		voteMonitor:                  newVoteMonitor,
		voteCurrentResult:            voteCurrentResult,
		electionConsiderationBuilder: electionConsiderationBuilder,
		stateChangeChannelObj:        limited_time_usage_channel.NewLimitedTimeUsageChannel(1),
	}
}

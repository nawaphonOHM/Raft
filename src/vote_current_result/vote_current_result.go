package vote_current_result

import (
	"log"
	"sync"

	"raft/src/raft_state"
	"raft/src/raft_type"
	"raft/src/vote_current_result/main_interface"
	"raft/src/vote_current_result/talk_to"
)

type voteCurrentResult struct {
	point       int
	all         int
	probability float32

	electionResult talk_to.ElectionResult

	mu sync.Mutex

	knownResultChannel       chan bool
	knownResultChannelIsOpen bool

	workerId    int
	state       raft_type.State
	currentTerm int
}

func (v *voteCurrentResult) ElectionResult() talk_to.ElectionResult {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.electionResult
}

func (v *voteCurrentResult) KnownResultChannel() <-chan bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.knownResultChannel
}

func (v *voteCurrentResult) calculateProbability() {

	oldProbability := v.probability

	v.probability = float32(v.point) / float32(v.all)

	log.Printf("[WorkerId %v][term %v][State: %v]: Calculate probability: Old %v; New %v\n",
		v.workerId,
		v.currentTerm,
		raft_state.ConvertToStateString(v.state),
		oldProbability,
		v.probability,
	)
}

func (v *voteCurrentResult) checkIsApproved() {
	if v.probability > 0.5 && v.knownResultChannelIsOpen {
		v.electionResult.SetAccept(true)
		v.knownResultChannelIsOpen = false
		v.knownResultChannel <- true
		close(v.knownResultChannel)
	}
}

func (v *voteCurrentResult) AddPoint(numToAdd int) {

	v.mu.Lock()
	defer v.mu.Unlock()

	v.point += numToAdd

	v.calculateProbability()
	v.checkIsApproved()
}

func (v *voteCurrentResult) SetError(err error) {

	v.mu.Lock()
	defer v.mu.Unlock()

	v.electionResult.SetWhy(err)
	v.electionResult.SetAccept(false)

	if v.knownResultChannelIsOpen {
		v.knownResultChannelIsOpen = false
		v.knownResultChannel <- true
		close(v.knownResultChannel)
	}
}

func NewVoteCurrentResult(
	all int,
	electionResult talk_to.ElectionResult,
	currentTerm int,
	workerId int,
	state raft_type.State,
) main_interface.VoteCurrentResult {

	return &voteCurrentResult{
		all:                      all,
		point:                    0,
		probability:              0,
		electionResult:           electionResult,
		knownResultChannel:       make(chan bool, 1),
		knownResultChannelIsOpen: true,
		currentTerm:              currentTerm,
		workerId:                 workerId,
		state:                    state,
	}
}

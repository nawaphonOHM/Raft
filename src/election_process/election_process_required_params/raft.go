package election_process_required_params

import (
	"raft/libs/labrpc"
	"raft/src/clock/clock_api_for"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
	"raft/src/log_collection/log_collection_api_for"
	"raft/src/raft_api_for"
	"raft/src/raft_type"
)

type Raft interface {
	Clock() clock_api_for.ElectionProcess
	WorkerId() int
	CurrentTerm() int
	State() raft_type.State
	PeersSize() int
	NextIndexSize() int
	MatchIndexSize() int
	Peers() []*labrpc.ClientEnd
	Log() log_collection_api_for.ElectionProcess
	VoteFor() *int

	SetNextIndexAndMatchIndex(nextIndex []int, matchIndex []int)
	SetState(newState raft_type.State)

	SubscribeStateChange(subObj limited_time_usage_channel_api_for.ElectionProcess, expectedState raft_type.State) bool
}

type raft struct {
	workerId       int
	currentTerm    int
	state          raft_type.State
	clock          clock_api_for.ElectionProcess
	peersSize      int
	nextIndexSize  int
	matchIndexSize int
	peers          []*labrpc.ClientEnd
	log            log_collection_api_for.ElectionProcess

	voteFor *int

	instance raft_api_for.ElectionProcess
}

func (r *raft) VoteFor() *int {
	return r.voteFor
}

func (r *raft) SetState(newState raft_type.State) {
	r.instance.SetState(newState)
}

func (r *raft) SubscribeStateChange(subObj limited_time_usage_channel_api_for.ElectionProcess, expectedState raft_type.State) bool {
	return r.instance.SubscribeStateChange(subObj.(limited_time_usage_channel_api_for.Raft), expectedState)
}

func (r *raft) MatchIndexSize() int {
	return r.matchIndexSize
}

func (r *raft) NextIndexSize() int {
	return r.nextIndexSize
}

func (r *raft) SetNextIndexAndMatchIndex(nextIndex []int, matchIndex []int) {
	r.instance.SetNextIndexAndMatchIndex(nextIndex, matchIndex)
}

func (r *raft) Clock() clock_api_for.ElectionProcess {
	return r.clock
}

func (r *raft) WorkerId() int {
	return r.workerId
}

func (r *raft) CurrentTerm() int {
	return r.currentTerm
}

func (r *raft) State() raft_type.State {
	return r.state
}

func (r *raft) PeersSize() int {
	return r.peersSize
}

func (r *raft) Peers() []*labrpc.ClientEnd {
	return r.peers
}

func (r *raft) Log() log_collection_api_for.ElectionProcess {
	return r.log
}

func NewRaft(
	workerId int,
	currentTerm int,
	state raft_type.State,
	clock clock_api_for.ElectionProcess,
	peersSize int,
	peers []*labrpc.ClientEnd,
	log log_collection_api_for.ElectionProcess,
	nextIndexSize int,
	matchIndexSize int,
	instance raft_api_for.ElectionProcess,
	voteFor *int,
) Raft {
	return &raft{
		workerId:       workerId,
		currentTerm:    currentTerm,
		state:          state,
		clock:          clock,
		peersSize:      peersSize,
		peers:          peers,
		log:            log,
		nextIndexSize:  nextIndexSize,
		matchIndexSize: matchIndexSize,
		instance:       instance,
		voteFor:        voteFor,
	}
}

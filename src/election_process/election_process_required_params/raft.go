package election_process_required_params

import (
	"raft/libs/labrpc"
	"raft/src/election_process/talk_to"
	"raft/src/raft_talk_to"
	"raft/src/raft_type"
)

type Raft interface {
	Clock() talk_to.Clock
	WorkerId() int
	CurrentTerm() int
	State() raft_type.State
	PeersSize() int
	NextIndexSize() int
	MatchIndexSize() int
	Peers() []*labrpc.ClientEnd
	Log() talk_to.LogCollection
	VoteFor() *int

	SetNextIndexAndMatchIndex(nextIndex []int, matchIndex []int)
	SetState(newState raft_type.State)

	SubscribeStateChange(subObj talk_to.LimitedTimeUsageChannel, expectedState raft_type.State) bool
}

type raft struct {
	workerId       int
	currentTerm    int
	state          raft_type.State
	clock          talk_to.Clock
	peersSize      int
	nextIndexSize  int
	matchIndexSize int
	peers          []*labrpc.ClientEnd
	log            talk_to.LogCollection

	voteFor *int

	instance talk_to.Raft
}

func (r *raft) VoteFor() *int {
	return r.voteFor
}

func (r *raft) SetState(newState raft_type.State) {
	r.instance.SetState(newState)
}

func (r *raft) SubscribeStateChange(subObj talk_to.LimitedTimeUsageChannel, expectedState raft_type.State) bool {
	return r.instance.SubscribeStateChange(subObj.(raft_talk_to.LimitedTimeUsageChannel), expectedState)
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

func (r *raft) Clock() talk_to.Clock {
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

func (r *raft) Log() talk_to.LogCollection {
	return r.log
}

func NewRaft(
	workerId int,
	currentTerm int,
	state raft_type.State,
	clock talk_to.Clock,
	peersSize int,
	peers []*labrpc.ClientEnd,
	log raft_talk_to.LogCollection,
	nextIndexSize int,
	matchIndexSize int,
	instance talk_to.Raft,
	voteFor *int,
) Raft {
	return &raft{
		workerId:       workerId,
		currentTerm:    currentTerm,
		state:          state,
		clock:          clock,
		peersSize:      peersSize,
		peers:          peers,
		log:            log.(talk_to.LogCollection),
		nextIndexSize:  nextIndexSize,
		matchIndexSize: matchIndexSize,
		instance:       instance,
		voteFor:        voteFor,
	}
}

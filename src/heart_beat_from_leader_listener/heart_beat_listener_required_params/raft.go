package heart_beat_listener_required_params

import (
	"raft/src/heart_beat_from_leader_listener/talk_to"
	"raft/src/raft_type"
)

type Raft interface {
	CurrentTerm() int
	WorkingId() int
	State() raft_type.State
	LogCollection() talk_to.LogCollection
	Clock() talk_to.Clock

	SubscribeStateChange(ch talk_to.LimitedTimeUsageChannel, expectedState raft_type.State) bool
	SubscribeCurrentTermChange(ch talk_to.LimitedTimeUsageChannel, expectedTerm int) bool

	SetState(newState raft_type.State)
}

type raft struct {
	currentTerm int
	workingId   int
	state       raft_type.State

	logCollection talk_to.LogCollection

	clock talk_to.Clock

	instance talk_to.Raft
}

func (r *raft) SetState(newState raft_type.State) {
	r.instance.SetState(newState)
}

func (r *raft) SubscribeCurrentTermChange(ch talk_to.LimitedTimeUsageChannel, expectedTerm int) bool {
	return r.instance.SubscribeCurrentTermChange(ch, expectedTerm)
}

func (r *raft) SubscribeStateChange(ch talk_to.LimitedTimeUsageChannel, expectedState raft_type.State) bool {
	return r.instance.SubscribeStateChange(ch, expectedState)
}

func (r *raft) Clock() talk_to.Clock {
	return r.clock
}

func (r *raft) LogCollection() talk_to.LogCollection {
	return r.logCollection
}

func (r *raft) CurrentTerm() int {
	return r.currentTerm
}

func (r *raft) WorkingId() int {
	return r.workingId
}

func (r *raft) State() raft_type.State {
	return r.state
}

func NewRaft(
	currentTerm int,
	workingId int,
	state raft_type.State,
	logCollection interface{},
	clock talk_to.Clock,
	instance talk_to.Raft,
) Raft {

	return &raft{
		currentTerm:   currentTerm,
		workingId:     workingId,
		state:         state,
		logCollection: logCollection.(talk_to.LogCollection),
		clock:         clock,
		instance:      instance,
	}
}

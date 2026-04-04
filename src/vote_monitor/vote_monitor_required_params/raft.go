package vote_monitor_required_params

import (
	"raft/src/raft_type"
	"raft/src/vote_monitor/talk_to"
)

type Raft interface {
	WorkerId() int
	CurrentTerm() int
	State() raft_type.State

	SetCurrentTermAndVoteFor(newTerm int)

	GetLastUpdateCurrentTerm() int
}

type raft struct {
	workerId    int
	currentTerm int
	state       raft_type.State

	instance talk_to.Raft
}

func (r *raft) WorkerId() int {
	return r.workerId
}

func (r *raft) CurrentTerm() int {
	return r.currentTerm
}

func (r *raft) GetLastUpdateCurrentTerm() int {
	return r.instance.CurrentTerm()
}

func (r *raft) State() raft_type.State {
	return r.state
}

func (r *raft) SetCurrentTermAndVoteFor(newTerm int) {
	r.instance.SetCurrentTermAndVoteFor(newTerm)
}

func NewRaft(
	workerId int,
	currentTerm int,
	state raft_type.State,
	instance talk_to.Raft,
) Raft {
	return &raft{workerId: workerId, currentTerm: currentTerm, state: state, instance: instance}
}

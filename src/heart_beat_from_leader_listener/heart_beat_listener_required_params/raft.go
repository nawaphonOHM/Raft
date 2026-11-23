package heart_beat_listener_required_params

import (
	"raft/src/clock/clock_api_for"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
	"raft/src/log_collection/log_collection_api_for"
	"raft/src/raft_api_for"
	"raft/src/raft_type"
)

type Raft interface {
	CurrentTerm() int
	WorkingId() int
	State() raft_type.State
	LogCollection() log_collection_api_for.HeartBeatListener
	Clock() clock_api_for.HeartBeatFromLeaderListener

	SubscribeStateChange(ch limited_time_usage_channel_api_for.HeartBeatFromLeaderListener, expectedState raft_type.State) bool
	SubscribeCurrentTermChange(ch limited_time_usage_channel_api_for.HeartBeatFromLeaderListener, expectedTerm int) bool

	SetState(newState raft_type.State)
}

type raft struct {
	currentTerm int
	workingId   int
	state       raft_type.State

	logCollection log_collection_api_for.HeartBeatListener

	clock clock_api_for.HeartBeatFromLeaderListener

	instance raft_api_for.HeartBeatFromLeaderListener
}

func (r *raft) SetState(newState raft_type.State) {
	r.instance.SetState(newState)
}

func (r *raft) SubscribeCurrentTermChange(ch limited_time_usage_channel_api_for.HeartBeatFromLeaderListener, expectedTerm int) bool {
	return r.instance.SubscribeCurrentTermChange(ch, expectedTerm)
}

func (r *raft) SubscribeStateChange(ch limited_time_usage_channel_api_for.HeartBeatFromLeaderListener, expectedState raft_type.State) bool {
	return r.instance.SubscribeStateChange(ch, expectedState)
}

func (r *raft) Clock() clock_api_for.HeartBeatFromLeaderListener {
	return r.clock
}

func (r *raft) LogCollection() log_collection_api_for.HeartBeatListener {
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
	logCollection any,
	clock clock_api_for.HeartBeatFromLeaderListener,
	instance raft_api_for.HeartBeatFromLeaderListener,
) Raft {

	return &raft{
		currentTerm:   currentTerm,
		workingId:     workingId,
		state:         state,
		logCollection: logCollection.(log_collection_api_for.HeartBeatListener),
		clock:         clock,
		instance:      instance,
	}
}

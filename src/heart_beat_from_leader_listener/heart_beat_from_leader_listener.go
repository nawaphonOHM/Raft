package heart_beat_from_leader_listener

import (
	"log"

	"raft/src/change_checker"
	"raft/src/heart_beat_from_leader_listener/heart_beat_listener_required_params"
	"raft/src/heart_beat_from_leader_listener/main_interface"
	"raft/src/limited_time_usage_channel"
	"raft/src/raft_state"
)

type heartbeatListener struct {
	raftParams heart_beat_listener_required_params.Raft
}

func (h *heartbeatListener) HearHeartbeat() bool {

	logChangeChannelObj := limited_time_usage_channel.NewLimitedTimeUsageChannel(1)

	if ok := h.raftParams.LogCollection().SubscribeLogChange(
		logChangeChannelObj,
		h.raftParams.LogCollection().Size(),
	); ok {
		log.Printf("[WorkerId %v][term %v][State: %v]: subscribe logChange success\n",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(h.raftParams.State()),
		)
	} else {
		log.Fatalf("[WorkerId %v][term %v][State: %v]: subscribe logChange fail\n",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(h.raftParams.State()),
		)
		return false
	}

	stateChangeChannelObj := limited_time_usage_channel.NewLimitedTimeUsageChannel(1)

	if ok := h.raftParams.SubscribeStateChange(stateChangeChannelObj, raft_state.FOLLOWER); ok {
		log.Printf("[WorkerId %v][term %v][State: %v]: subscribe stateChange success\n",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(h.raftParams.State()),
		)
	} else {
		log.Printf("[WorkerId %v][term %v][State: %v]: subscribe stateChange fail\n",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(h.raftParams.State()),
		)
		return false
	}

	currentTermChannelObj := limited_time_usage_channel.NewLimitedTimeUsageChannel(1)

	if ok := h.raftParams.SubscribeCurrentTermChange(currentTermChannelObj, h.raftParams.CurrentTerm()); ok {
		log.Printf("[WorkerId %v][term %v][State: %v]: subscribe currentTermChange success\n",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(h.raftParams.State()),
		)
	} else {
		log.Printf("[WorkerId %v][term %v][State: %v]: subscribe currentTermChange fail\n",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(h.raftParams.State()),
		)
		return false
	}

	timeOutSignal := make(chan bool, 1)

	h.raftParams.Clock().StartNewClockCycle(timeOutSignal)

	log.Printf("[WorkerId %v][term %v][State: %v]: start listening a heartbeat for term %v\n",
		h.raftParams.WorkingId(),
		h.raftParams.CurrentTerm(),
		raft_state.ConvertToStateString(h.raftParams.State()),
		h.raftParams.CurrentTerm(),
	)

	<-timeOutSignal

	log.Printf("[WorkerId %v][term %v][State: %v]: listening a heartbeat for term %v is over. Let's check for the result.\n",
		h.raftParams.WorkingId(),
		h.raftParams.CurrentTerm(),
		raft_state.ConvertToStateString(h.raftParams.State()),
		h.raftParams.CurrentTerm(),
	)

	if change := change_checker.IsChange(currentTermChannelObj.Channel()); change {

		log.Printf("[WorkerId %v][term %v][State: %v]: I'm out of sync, I think It was term %v but It has been changed",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(h.raftParams.State()),
			h.raftParams.CurrentTerm(),
		)

		return false
	}

	if change := change_checker.IsChange(stateChangeChannelObj.Channel()); change {
		log.Printf("[WorkerId %v][term %v][State: ???]: Maybe I'm out of sync, I thought It is still FOLLOWER but not",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
		)

		return false
	}

	if change := change_checker.IsChange(logChangeChannelObj.Channel()); change {

		log.Printf("[WorkerId %v][term %v][State: %v]: listening a heartbeat for term %v. result: did hear\n",
			h.raftParams.WorkingId(),
			h.raftParams.CurrentTerm(),
			raft_state.ConvertToStateString(h.raftParams.State()),
			h.raftParams.CurrentTerm(),
		)

		return true

	}

	log.Printf("[WorkerId %v][term %v][State: %v]: listening a heartbeat for term %v. result: did not hear\n",
		h.raftParams.WorkingId(),
		h.raftParams.CurrentTerm(),
		raft_state.ConvertToStateString(h.raftParams.State()),
		h.raftParams.CurrentTerm(),
	)

	h.raftParams.SetState(raft_state.CANDIDATE)

	return false

}

func NewHeartbeatListener(raftParams heart_beat_listener_required_params.Raft) main_interface.HeartbeatListener {
	return &heartbeatListener{raftParams: raftParams}
}

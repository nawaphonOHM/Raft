package vote_monitor_required_params

import (
	"log"
	"sync"

	"raft/src/channel_envelop"
	"raft/src/raft_type"
	"raft/src/unlimited_time_usage_channel"
	"raft/src/vote_monitor/talk_to"
)

type Raft interface {
	WorkerId() int
	CurrentTerm() int
	State() raft_type.State

	SetCurrentTermAndVoteFor(newTerm int)

	Close()
}

type raft struct {
	workerId    int
	currentTerm int
	state       raft_type.State

	instance talk_to.Raft

	channelEnvelop talk_to.ChannelEnvelop

	lock sync.Mutex
}

func (r *raft) Close() {

	r.channelEnvelop.Close()

}

func (r *raft) WorkerId() int {
	return r.workerId
}

func (r *raft) CurrentTerm() int {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.currentTerm
}

func (r *raft) State() raft_type.State {
	return r.state
}

func (r *raft) SetCurrentTermAndVoteFor(newTerm int) {
	r.instance.SetCurrentTermAndVoteFor(newTerm)
}

func (r *raft) subscribeCurrentTermChange() {

	go func(self *raft) {
		for {

			newCurrentTerm, open := <-r.channelEnvelop.Channel()

			if !open {
				break
			}

			self.lock.Lock()
			self.currentTerm = newCurrentTerm.(int)
			self.lock.Unlock()
		}
	}(r)
}

func NewRaft(
	workerId int,
	currentTerm int,
	state raft_type.State,
	instance talk_to.Raft,
) Raft {

	channelEnvelop := channel_envelop.NewChannelEnvelop(unlimited_time_usage_channel.NewUnlimitedTimeUsageChannel())

	ok := instance.SubscribeCurrentTermChange(channelEnvelop, currentTerm)

	if !ok {
		log.Fatal("Expected to be able to subscribe to currentTermChange")
	}

	obj := &raft{
		workerId:       workerId,
		currentTerm:    currentTerm,
		state:          state,
		instance:       instance,
		channelEnvelop: channelEnvelop,
	}

	obj.subscribeCurrentTermChange()

	return obj
}

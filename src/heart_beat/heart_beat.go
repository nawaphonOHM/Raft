package heart_beat

import (
	"errors"

	"raft/libs/labrpc"
	"raft/src/dto"
	customerror "raft/src/errors"
	"raft/src/heart_beat/heart_beat_required_params"
	"raft/src/heart_beat/main_interface"
)

type heartbeat struct {
	raftParams heart_beat_required_params.Raft
}

func (h *heartbeat) StartHeartbeat() {

	h.sendHeartbeatSignal()

}

func (h *heartbeat) signal(args *dto.AppendEntriesArgs, peer *labrpc.ClientEnd) {
	reply := &dto.AppendEntriesReply{}

	peer.Call("Raft.AppendEntries", args, reply)

}

func (h *heartbeat) buildArgs() *dto.AppendEntriesArgs {

	var noLogError *customerror.ItHasNoLogError
	if errors.As(h.raftParams.LogInformation().Err, &noLogError) {
		return &dto.AppendEntriesArgs{
			Term:         h.raftParams.CurrentTerm(),
			LeaderId:     h.raftParams.WorkerId(),
			PrevLogIndex: -1,
			PrevLogTerm:  1,
			Entries:      nil,
			LeaderCommit: h.raftParams.CommitIndex(),
		}
	}

	var onlyOneLogError *customerror.ItHasOnlyOneLogError
	if errors.As(h.raftParams.LogInformation().Err, &onlyOneLogError) {
		return &dto.AppendEntriesArgs{
			Term:         h.raftParams.CurrentTerm(),
			LeaderId:     h.raftParams.WorkerId(),
			PrevLogIndex: h.raftParams.LogInformation().LogIndex,
			PrevLogTerm:  h.raftParams.LogInformation().TermIndex,
			Entries:      nil,
			LeaderCommit: h.raftParams.CommitIndex(),
		}
	}

	return &dto.AppendEntriesArgs{
		Term:         h.raftParams.CurrentTerm(),
		LeaderId:     h.raftParams.WorkerId(),
		PrevLogIndex: h.raftParams.LogInformation().LogIndex,
		PrevLogTerm:  h.raftParams.LogInformation().TermIndex,
		Entries:      nil,
		LeaderCommit: h.raftParams.CommitIndex(),
	}

}

func (h *heartbeat) sendHeartbeatSignal() {

	args := h.buildArgs()

	for index, peer := range h.raftParams.Peers() {
		if index == h.raftParams.WorkerId() {
			continue
		}
		go h.signal(args, peer)
	}

}

func NewHeartbeat(raftParams heart_beat_required_params.Raft) main_interface.Heartbeat {
	return &heartbeat{raftParams: raftParams}
}

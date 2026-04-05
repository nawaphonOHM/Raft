package vote_monitor

import (
	"context"
	"fmt"
	"log"

	"raft/libs/labrpc"
	"raft/src/dto"
	"raft/src/raft_state"
	"raft/src/vote_monitor/internal_struct/peer_information"
	"raft/src/vote_monitor/main_interface"
	"raft/src/vote_monitor/talk_to"
	"raft/src/vote_monitor/vote_monitor_required_params"
)

type voteMonitor struct {
	peerInformation        talk_to.PeerInformation
	requestParam           *dto.RequestVoteArgs
	theContext             context.Context
	approveSignal          talk_to.LimitedTimeUsageChannel
	requiredParamsFromRaft vote_monitor_required_params.Raft

	cancelFunction context.CancelFunc
}

func (v *voteMonitor) SetApproveChannel(channel talk_to.LimitedTimeUsageChannel) {

	v.approveSignal = channel

	log.Printf(
		"[WorkerId %v][term %v][State: %v][vote monitor id %p]: Approve channel is set. channel ref: %p\n",
		v.requiredParamsFromRaft.WorkerId(),
		v.requiredParamsFromRaft.CurrentTerm(),
		raft_state.ConvertToStateString(v.requiredParamsFromRaft.State()),
		v,
		v.approveSignal,
	)
}

func (v *voteMonitor) requestVote() {

	requestVoteReply := &dto.RequestVoteReply{}

	v.peerInformation.Instance().Call("Raft.RequestVote", v.requestParam, requestVoteReply)

	v.validate(requestVoteReply)
}

func (v *voteMonitor) validate(result *dto.RequestVoteReply) {

	select {
	case <-v.theContext.Done():
		{
			v.approveSignal.Notify(&dto.VoteCommand{
				Point:            0,
				ChannelOperation: raft_state.Close,
				Sender: fmt.Sprintf(
					"VOTE_MONITOR_FOR_%v",
					v.peerInformation.InstanceId(),
				),
			})
			v.cancelFunction()
			v.requiredParamsFromRaft.Close()

			log.Printf("[WorkerId %v][term %v][State: %v][vote monitor id %p]: Received cancel ongoing task...LEAVE and kill approve channel ref %p\n",
				v.requiredParamsFromRaft.WorkerId(),
				v.requiredParamsFromRaft.CurrentTerm(),
				raft_state.ConvertToStateString(v.requiredParamsFromRaft.State()),
				v,
				v.approveSignal,
			)

			return
		}
	default:
		{

			log.Printf("[WorkerId %v][term %v][State: %v][vote monitor id %p]: peer %v sends back about the vote for term %v\n",
				v.requiredParamsFromRaft.WorkerId(),
				v.requiredParamsFromRaft.CurrentTerm(),
				raft_state.ConvertToStateString(v.requiredParamsFromRaft.State()),
				v,
				v.peerInformation.InstanceId(),
				v.requestParam.Term,
			)

			if result.VoteGranted {
				log.Printf("[WorkerId %v][term %v][State: %v][vote monitor id %p]: peer %v accepts me to be a leader. I will signal via approve channel ref %p\n",
					v.requiredParamsFromRaft.WorkerId(),
					v.requiredParamsFromRaft.CurrentTerm(),
					raft_state.ConvertToStateString(v.requiredParamsFromRaft.State()),
					v,
					v.peerInformation.InstanceId(),
					v.approveSignal,
				)

				v.approveSignal.Notify(&dto.VoteCommand{
					Point:            1,
					ChannelOperation: raft_state.Close,
					Sender: fmt.Sprintf(
						"VOTE_MONITOR_FOR_%v",
						v.peerInformation.InstanceId(),
					),
				})

				v.cancelFunction()
				v.requiredParamsFromRaft.Close()

				return
			}

			receivedTerm := result.Term

			if receivedTerm < 0 {
				v.cancelFunction()
				v.requiredParamsFromRaft.Close()

				log.Printf("[WorkerId %v][term %v][State: %v][vote monitor id %p]: peer %v denies me to be a leader for a reason a, Stop My task and kill approve channel ref %p\n",
					v.requiredParamsFromRaft.WorkerId(),
					v.requiredParamsFromRaft.CurrentTerm(),
					raft_state.ConvertToStateString(v.requiredParamsFromRaft.State()),
					v,
					v.peerInformation.InstanceId(),
					v.approveSignal,
				)

				return
			}

			if v.requiredParamsFromRaft.CurrentTerm() < receivedTerm {

				log.Printf("[WorkerId %v][term %v][State: %v][vote monitor id %p]: peer %v denies me to be a leader because I has outdated term update term %v to term %v and request vote again.\n",
					v.requiredParamsFromRaft.WorkerId(),
					v.requiredParamsFromRaft.CurrentTerm(),
					raft_state.ConvertToStateString(v.requiredParamsFromRaft.State()),
					v,
					v.peerInformation.InstanceId(),
					v.requiredParamsFromRaft.CurrentTerm(),
					receivedTerm,
				)

				v.requiredParamsFromRaft.SetCurrentTermAndVoteFor(receivedTerm)
				v.requestParam.Term = receivedTerm
			}

			v.Start()

		}
	}
}

func (v *voteMonitor) Start() {

	log.Printf(
		"[WorkerId %v][term %v][State: %v][vote monitor id %p]: I will monitor about sending a vote request in term %v to peer: %v\n",
		v.requiredParamsFromRaft.WorkerId(),
		v.requiredParamsFromRaft.CurrentTerm(),
		raft_state.ConvertToStateString(v.requiredParamsFromRaft.State()),
		v,
		v.requestParam.Term,
		v.peerInformation.InstanceId(),
	)

	go v.requestVote()
}

func (v *voteMonitor) SetTheContext(theContext context.Context) {
	v.theContext, v.cancelFunction = context.WithCancel(theContext)
}

func (v *voteMonitor) SetRequestVoteArgs(requestParam *dto.RequestVoteArgs) {
	v.requestParam = requestParam
}

func NewVoteMonitor(
	peer *labrpc.ClientEnd,
	peerId int,
	raft vote_monitor_required_params.Raft,
) main_interface.VoteMonitor {
	return &voteMonitor{
		peerInformation:        peer_information.NewPeerInformation(peer, peerId),
		requiredParamsFromRaft: raft,
	}
}

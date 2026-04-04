package peer_information

import (
	"raft/libs/labrpc"
	"raft/src/vote_monitor/internal_struct/peer_information/main_interface"
)

type peerInformation struct {
	instance   *labrpc.ClientEnd
	instanceId int
}

func (p *peerInformation) Instance() *labrpc.ClientEnd {
	return p.instance
}

func (p *peerInformation) InstanceId() int {
	return p.instanceId
}

func NewPeerInformation(instance *labrpc.ClientEnd, instanceId int) main_interface.PeerInformation {
	return &peerInformation{instance: instance, instanceId: instanceId}
}

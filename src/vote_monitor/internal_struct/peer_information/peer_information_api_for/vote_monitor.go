package peer_information_api_for

import (
	"raft/libs/labrpc"
)

type VoteMonitor interface {
	Instance() *labrpc.ClientEnd
	InstanceId() int
}

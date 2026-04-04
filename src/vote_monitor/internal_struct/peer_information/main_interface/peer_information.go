package main_interface

import (
	"raft/libs/labrpc"
)

type PeerInformation interface {
	Instance() *labrpc.ClientEnd
	InstanceId() int
}

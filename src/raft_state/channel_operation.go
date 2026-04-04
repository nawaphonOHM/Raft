package raft_state

import "raft/src/raft_type"

const (
	Close raft_type.ChannelOperation = iota
	NoClose
)

func ConvertToChannelOperationString(state raft_type.ChannelOperation) string {
	switch state {
	case Close:
		{
			return "Close"
		}
	case NoClose:
		{
			return "NoClose"
		}
	}

	panic("Invalid state")
}

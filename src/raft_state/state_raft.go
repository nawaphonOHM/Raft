package raft_state

import "raft/src/raft_type"

const (
	LEADER raft_type.State = iota
	FOLLOWER
	CANDIDATE
)

func ConvertToStateString(state raft_type.State) string {
	switch state {
	case LEADER:
		{
			return "LEADER"
		}
	case FOLLOWER:
		{
			return "FOLLOWER"
		}
	case CANDIDATE:
		{
			return "CANDIDATE"
		}
	}

	panic("Invalid state")
}

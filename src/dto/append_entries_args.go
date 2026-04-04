package dto

import (
	"raft/src/log_collection/log/main_interface"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []main_interface.Log

	LeaderCommit int
}

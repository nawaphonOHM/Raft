package heart_beat_required_params

import (
	"raft/libs/labrpc"
	"raft/src/dto"
)

type Raft interface {
	WorkerId() int
	Peers() []*labrpc.ClientEnd
	LogInformation() *dto.LatestLogIndexMinusOneAndLatestLogMinusOne
	CommitIndex() int
	CurrentTerm() int
}

type raft struct {
	logInformation *dto.LatestLogIndexMinusOneAndLatestLogMinusOne

	commitIndex int

	workerId int

	peers []*labrpc.ClientEnd

	currentTerm int
}

func (r *raft) CommitIndex() int {
	return r.commitIndex
}

func (r *raft) CurrentTerm() int {
	return r.currentTerm
}

func (r *raft) LogInformation() *dto.LatestLogIndexMinusOneAndLatestLogMinusOne {
	return r.logInformation
}

func (r *raft) Peers() []*labrpc.ClientEnd {

	return r.peers
}

func (r *raft) WorkerId() int {
	return r.workerId
}

func NewRaft(
	workerId int,
	peers []*labrpc.ClientEnd,
	logInformation *dto.LatestLogIndexMinusOneAndLatestLogMinusOne,
	commitIndex int,
	currentTerm int,
) Raft {
	return &raft{
		workerId:       workerId,
		peers:          peers,
		commitIndex:    commitIndex,
		logInformation: logInformation,
		currentTerm:    currentTerm,
	}
}

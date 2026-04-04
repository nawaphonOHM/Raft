package raft

// The file raftapi/log_collection.go defines the election_consideration_builder_main_interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft election_consideration_builder_main_interface.

import (
	"fmt"
	"log"
	"math/rand"
	"slices"
	"time"

	"sync"
	"sync/atomic"
	//"raft/libs/labgob"
	"raft/libs/labrpc"
	"raft/libs/raftapi"
	"raft/libs/tester1"
	"raft/src/clock"
	"raft/src/dto"
	"raft/src/election_consideration/builder"
	"raft/src/election_process"
	"raft/src/election_process/election_process_required_params"
	"raft/src/election_result"
	"raft/src/heart_beat"
	"raft/src/heart_beat/heart_beat_required_params"
	"raft/src/heart_beat_from_leader_listener"
	"raft/src/heart_beat_from_leader_listener/heart_beat_listener_required_params"
	"raft/src/heart_beat_from_leader_listener/talk_to"
	"raft/src/log_collection"
	definedLog "raft/src/log_collection/log"
	"raft/src/raft_state"
	"raft/src/raft_talk_to"
	"raft/src/raft_type"
	"raft/src/unlimited_time_usage_channel"
	"raft/src/vote_current_result"
	"raft/src/vote_monitor"
	"raft/src/vote_monitor/vote_monitor_required_params"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    *int
	log         raft_talk_to.LogCollection

	commitIndex int
	lastApplied int

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	nextIndex []int

	matchIndex []int

	state raft_type.State

	stateChangeSubscribers []raft_talk_to.LimitedTimeUsageChannel
	currentTermSubscribers []raft_talk_to.LimitedTimeUsageChannel

	clock talk_to.Clock

	leaderTerm []int
}

func (rf *Raft) CurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) logChangesSubscribe() {

	channelObj := unlimited_time_usage_channel.NewUnlimitedTimeUsageChannel()
	ok := rf.log.SubscribeLogChange(channelObj, 0)

	if !ok {
		log.Fatal("Expected to be able to subscribe to log changes")
	}

	go func(self *Raft, channel <-chan interface{}) {

		for {
			theMostIndex := <-channel

			rf.mu.Lock()

			if newsCommitIndex, pass := theMostIndex.(int); pass {
				rf.commitIndex = newsCommitIndex
			} else {
				log.Fatal("Expected to receive an int")
			}

			rf.mu.Unlock()
		}

	}(rf, channelObj.Channel())

}

func (rf *Raft) SetNextIndexAndMatchIndex(nextIndex []int, matchIndex []int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.nextIndex = nextIndex
	rf.matchIndex = matchIndex
}

func (rf *Raft) signalStateChange() {

	for _, subscriber := range rf.stateChangeSubscribers {
		subscriber.Notify()
	}

}

func (rf *Raft) signalCurrentTermChange() {

	for _, subscriber := range rf.currentTermSubscribers {
		subscriber.Notify()
	}
}

func (rf *Raft) clearDeadSignal() {

	var newArray []raft_talk_to.LimitedTimeUsageChannel

	newArray = make([]raft_talk_to.LimitedTimeUsageChannel, 0)

	for _, subscriber := range rf.stateChangeSubscribers {
		if !subscriber.IsBroken() {
			newArray = append(newArray, subscriber)
		}
	}

	rf.stateChangeSubscribers = newArray

	newArray = make([]raft_talk_to.LimitedTimeUsageChannel, 0)

	for _, subscriber := range rf.currentTermSubscribers {
		if !subscriber.IsBroken() {
			newArray = append(newArray, subscriber)
		}
	}

	rf.currentTermSubscribers = newArray
}

func (rf *Raft) SetState(newState raft_type.State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = newState

	rf.signalStateChange()
	rf.clearDeadSignal()
}

func (rf *Raft) SetCurrentTermAndVoteFor(newCurrentTerm int, voteFor ...int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(voteFor) > 1 {
		log.Fatal("Expected only one voteFor")
	}

	if len(voteFor) == 0 {
		rf.votedFor = nil
	} else {
		rf.votedFor = &voteFor[0]
	}

	rf.currentTerm = newCurrentTerm

	rf.signalCurrentTermChange()
	rf.clearDeadSignal()
}

func (rf *Raft) SubscribeStateChange(subObj raft_talk_to.LimitedTimeUsageChannel, expectedState raft_type.State) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if expectedState != rf.state {
		return false
	}

	rf.stateChangeSubscribers = append(rf.stateChangeSubscribers, subObj)

	return true
}

func (rf *Raft) SubscribeCurrentTermChange(subObj raft_talk_to.LimitedTimeUsageChannel, expectedTermNumber int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if expectedTermNumber != rf.currentTerm {
		return false
	}

	rf.currentTermSubscribers = append(rf.currentTermSubscribers, subObj)

	return true
}

func (rf *Raft) setCurrentTermMonotonically(newTerm int, voteFor ...int) bool {

	if len(voteFor) > 1 {
		log.Fatal("Expected only one voteFor")
	}

	if diff := newTerm - rf.currentTerm; diff != 1 {
		return false
	}

	if len(voteFor) == 0 {
		rf.setCurrentTermAndVoteFor(newTerm)
		return true
	}

	if len(voteFor) == 1 {
		rf.setCurrentTermAndVoteFor(newTerm, voteFor[0])
		return true
	}

	panic("Should not reach here.")

}

func (rf *Raft) setCurrentTermAndVoteFor(newCurrentTerm int, voteFor ...int) {

	if len(voteFor) > 1 {
		log.Fatal("Expected to receive at most one voteFor")
	}

	if len(voteFor) == 0 {
		rf.currentTerm = newCurrentTerm
		rf.votedFor = nil
	}

	if len(voteFor) == 1 {
		rf.currentTerm = newCurrentTerm
		rf.votedFor = &voteFor[0]
	}

	rf.signalCurrentTermChange()
	rf.clearDeadSignal()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	state := rf.state
	term = int(rf.currentTerm)
	workerId := rf.me
	rf.mu.Unlock()

	log.Printf("[WorkerId %v][term %v][State: %v]: Someone needs me to tell leaderState and CurrentTerm\n",
		workerId,
		term,
		raft_state.ConvertToStateString(state),
	)

	isleader = state == raft_state.LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log_collection_type?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log_collection_type through (and including)
// that index. Raft should now trim its log_collection_type as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).

	Term        int
	VoteGranted bool
}

type RequestVoteEnvelopReply struct {
	reply       *RequestVoteReply
	replyWorker *labrpc.ClientEnd
	who         int
}

func (rf *Raft) appendEntriesHeartbeatMode(args *dto.AppendEntriesArgs, reply *dto.AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == raft_state.LEADER {

		if rf.currentTerm > args.Term {
			log.Printf(
				"[WorkerId %v][term %v][State: %v]: I've received an outdated heartbeat from %v, but I'm a leader with updated term. so reject to listening a heartbeat it\n",
				rf.me,
				rf.currentTerm,
				raft_state.ConvertToStateString(rf.state),
				args.LeaderId,
			)

			reply.Term = -1
			reply.Success = false

		} else {

			log.Printf(
				"[WorkerId %v][term %v][State: %v]: I've somehow lost my leader. change me to be a follower\n",
				rf.me,
				rf.currentTerm,
				raft_state.ConvertToStateString(rf.state),
			)

			rf.setCurrentTermAndVoteFor(args.Term, args.LeaderId)
			rf.setState(raft_state.FOLLOWER)

			log.Printf(
				"[WorkerId %v][term %v][State: %v]: changing from LEADER to FOLLOWER is Done!!!\n",
				rf.me,
				rf.currentTerm,
				raft_state.ConvertToStateString(rf.state),
			)

			rf.log.Add(definedLog.NewLog("heartbeat", args.Term))

			reply.Term = rf.currentTerm
			reply.Success = true

		}

		return

	}

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.Success = false

		log.Printf("[WorkerId %v][term %v][State: %v]: received an outdated AppendEntries RPC (heartbeat) from %v with term %v but I'm now on term is %v, so reject it\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
			args.LeaderId,
			args.Term,
			rf.currentTerm,
		)

		return
	}

	if rf.state == raft_state.CANDIDATE && args.Term > rf.currentTerm {

		log.Printf(
			"[WorkerId %v][term %v][State: %v]: I think we have a leader by now. stop to be a CANDIDATE and change to a FOLLOWER\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
		)

		rf.setState(raft_state.FOLLOWER)

		log.Printf(
			"[WorkerId %v][term %v][State: %v]: change state from a CANDIDATE to a FOLLOWER is done!!!\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
		)

		rf.setCurrentTermAndVoteFor(args.Term, args.LeaderId)

		rf.log.Add(definedLog.NewLog("heartbeat", args.Term))

		reply.Term = rf.currentTerm
		reply.Success = true

		return
	}

	if rf.state == raft_state.CANDIDATE {
		log.Printf(
			"[WorkerId %v][term %v][State: %v]: I think we have a leader by now. stop to be a CANDIDATE change to a FOLLOWER\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
		)

		rf.setState(raft_state.FOLLOWER)

		log.Printf(
			"[WorkerId %v][term %v][State: %v]: change state from a CANDIDATE to a FOLLOWER is done!!!\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
		)
	}

	if args.Term > rf.currentTerm {
		log.Printf("[WorkerId %v][term %v][State: %v]: received AppendEntries RPC (heartbeat) from %v with term %v but I think current term is %v, so update it\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
			args.LeaderId,
			args.Term,
			rf.currentTerm,
		)

		rf.setCurrentTermAndVoteFor(args.Term, args.LeaderId)

		rf.log.Add(definedLog.NewLog("heartbeat", args.Term))

		reply.Term = rf.currentTerm
		reply.Success = true

		return

	}

	rf.log.Add(definedLog.NewLog("heartbeat", args.Term))

	reply.Term = rf.currentTerm
	reply.Success = true

	log.Printf("[WorkerId %v][term %v][State: %v]: received a valid AppendEntries RPC with no entries (heartbeat) from %v\n",
		rf.me,
		rf.currentTerm,
		raft_state.ConvertToStateString(rf.state),
		args.LeaderId,
	)

	return
}

func (rf *Raft) appendEntriesNotHeartbeatMode(args *dto.AppendEntriesArgs, reply *dto.AppendEntriesReply) {

	panic("implement me")

	// TODO: implantation is not complete

	//log_collection_type.Printf("[WorkerId %v][term %v][State: %v]: received AppendEntries RPC from peer %v\n",
	//	rf.me,
	//	rf.currentTerm,
	//	args.LeaderId,
	//	convertToStateString(rf.state),
	//)
	//
	//if args.Term < rf.currentTerm {
	//	reply.Term = rf.currentTerm
	//	reply.Success = false
	//
	//	log_collection_type.Printf("[WorkerId %v][term %v][State: %v]: received AppendEntries RPC with term %v but I think current term is %v, so reject it\n",
	//		rf.me,
	//		rf.currentTerm,
	//		args.Term,
	//		rf.currentTerm,
	//		convertToStateString(rf.state),
	//	)
	//
	//	return
	//}
	//
	//pass, err := rf.log_collection_type.validate(args.PrevLogTerm, args.PrevLogIndex)
	//
	//{
	//	cannotAccessError := &CannotAccessError{}
	//
	//	isCannotAccessError := errors.As(err, &cannotAccessError)
	//
	//	if !pass && isCannotAccessError {
	//		reply.Term = rf.currentTerm
	//		reply.Success = false
	//
	//		log_collection_type.Printf("[WorkerId %v][term %v][State: %v]: received AppendEntries RPC with prevLogIndex %v but I cannot access it, so reject it\n",
	//			rf.me,
	//			rf.currentTerm,
	//			args.PrevLogIndex,
	//			convertToStateString(rf.state),
	//		)
	//
	//		return
	//	}
	//
	//}
	//
	//{
	//	candidateTermMismatchError := &CandidateTermMismatch{}
	//
	//	isCandidateTermMismatchError := errors.As(err, &candidateTermMismatchError)
	//
	//	if !pass && isCandidateTermMismatchError {
	//
	//		log_collection_type.Printf("[WorkerId %v][term %v][State: %v]: received AppendEntries RPC with prevLogIndex %v but my term in log_collection_type index %v is not a term %v \n",
	//			rf.me,
	//			rf.currentTerm,
	//			args.PrevLogIndex,
	//			args.PrevLogIndex,
	//			args.PrevLogTerm,
	//			convertToStateString(rf.state),
	//		)
	//
	//		for index := args.PrevLogIndex; index < rf.log_collection_type.size(); index++ {
	//			rf.log_collection_type.remove(index)
	//		}
	//
	//	}
	//}
	//
	//if args.Term > rf.currentTerm {
	//	log_collection_type.Printf("[WorkerId %v][term %v][State: %v]: received AppendEntries RPC with term %v but I think current term is %v, so update it\n",
	//		rf.me,
	//		rf.currentTerm,
	//		args.Term,
	//		rf.currentTerm,
	//		convertToStateString(rf.state),
	//	)
	//	rf.currentTerm = args.Term
	//}
	//
	//reply.Term = rf.currentTerm
	//reply.Success = true
	//
	//if args.LeaderCommit > rf.commitIndex {
	//	rf.commitIndex = min(args.LeaderCommit, rf.log_collection_type.size()-1)
	//}
}

func (rf *Raft) AppendEntries(args *dto.AppendEntriesArgs, reply *dto.AppendEntriesReply) {

	if args.Entries == nil {
		rf.appendEntriesHeartbeatMode(args, reply)
	} else {
		rf.appendEntriesNotHeartbeatMode(args, reply)
	}

}

func (rf *Raft) setState(newState raft_type.State) {
	rf.state = newState

	rf.signalStateChange()
	rf.clearDeadSignal()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *dto.RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[WorkerId %v][term %v][State: %v]: received RequestVote RPC from %v for term %v\n",
		rf.me,
		rf.currentTerm,
		raft_state.ConvertToStateString(rf.state),
		args.CandidateId,
		args.Term,
	)

	if rf.state == raft_state.LEADER {

		reply.Term = -1
		reply.VoteGranted = false

		log.Printf("[WorkerId %v][term %v][State: %v]: I'm a leader. so vote denied\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
		)

		return
	}

	if rf.state != raft_state.CANDIDATE {

		log.Printf("[WorkerId %v][term %v][State: %v]: I'm out of sync, it's now an election_process turn, change it to candidate\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
		)

		rf.setState(raft_state.CANDIDATE)

		log.Printf("[WorkerId %v][term %v][State: %v]: Update current state to be a CANDIDATE is done\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
		)

	}

	if args.Term < rf.currentTerm {

		updatedCurrentTerm := rf.currentTerm

		reply.Term = updatedCurrentTerm
		reply.VoteGranted = false

		log.Printf("[WorkerId %v][term %v][State: %v]: received vote request from term %v but I think current term is %v, so vote denied\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
			args.Term,
			rf.currentTerm,
		)

		return
	}

	if args.Term > rf.currentTerm {

		log.Printf("[WorkerId %v][term %v][State: %v]: received vote request from term %v but I think current term is %v, so update it\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
			args.Term,
			rf.currentTerm,
		)

		rf.setCurrentTermAndVoteFor(args.Term)

		log.Printf("[WorkerId %v][term %v][State: %v]: update my current term is Done\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
		)

	}

	if rf.votedFor != nil && *rf.votedFor != args.CandidateId {

		reply.Term = -1
		reply.VoteGranted = false

		log.Printf("[WorkerId %v][term %v][State: %v]: already voted for %v, so vote denied\n",
			rf.me,
			rf.currentTerm,
			raft_state.ConvertToStateString(rf.state),
			*rf.votedFor,
		)

		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	log.Printf("[WorkerId %v][term %v][State: %v]: vote for candidate %v\n",
		rf.me,
		rf.currentTerm,
		raft_state.ConvertToStateString(rf.state),
		args.CandidateId,
	)

	rf.setCurrentTermAndVoteFor(rf.currentTerm, args.CandidateId)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *dto.RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log_collection_type. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log_collection_type, since the leader
// may fail or lose an election_process. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) tryLog(message string) {

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	leaderTerm := slices.Clone(rf.leaderTerm)
	rf.mu.Unlock()

	setLeaderTerm := false

	defer func(setLeaderTerm *bool, leaderTerm *[]int, self *Raft) {

		self.mu.Lock()

		if *setLeaderTerm {
			rf.leaderTerm = *leaderTerm
		}

		self.mu.Unlock()

	}(&setLeaderTerm, &leaderTerm, rf)

	if slices.Contains(rf.leaderTerm, currentTerm) {
		return
	}

	setLeaderTerm = true
	leaderTerm = append(leaderTerm, currentTerm)

	log.Printf(message)

}

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {

	currentState := raft_state.FOLLOWER

	var lastTerm int

	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election_process should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.lock()
		state := rf.state
		currentTerm := rf.currentTerm
		workerId := rf.me
		logInformation := rf.log.GetLatestLogIndexMinusOneAndLatestTermMinusOne()
		commitIndex := rf.commitIndex
		theLog := rf.log
		peers := rf.peers
		peersSize := len(rf.peers)
		clock_ := rf.clock
		nextIndexSize := len(rf.nextIndex)
		matchIndexSize := len(rf.matchIndex)
		voteFor := rf.votedFor
		rf.unlock()

		if state == raft_state.LEADER {
			currentState = raft_state.LEADER

			rf.tryLog(
				fmt.Sprintf("[WorkerId %v][term %v][State: %v]: I'm a LEADER and I will send a heartbeat\n",
					workerId,
					currentTerm,
					raft_state.ConvertToStateString(state),
				),
			)

			lastTerm = currentTerm

			heart_beat.NewHeartbeat(
				heart_beat_required_params.NewRaft(
					workerId,
					peers,
					logInformation,
					commitIndex,
					currentTerm,
				),
			).StartHeartbeat()

			continue
		}

		if currentState == raft_state.LEADER {
			log.Printf("[WorkerId %v][Term %v][State: %v]: I was a LEADER but not now.\n",
				workerId,
				currentTerm,
				raft_state.ConvertToStateString(state))
		}

		if rf.state == raft_state.CANDIDATE {
			currentState = raft_state.CANDIDATE

			newCurrentTerm := lastTerm + 1

			log.Printf("[WorkerId %v][Term %v][State: %v]: I will increase current term by 1\n",
				workerId,
				currentTerm,
				raft_state.ConvertToStateString(state))

			rf.mu.Lock()
			ok := rf.setCurrentTermMonotonically(newCurrentTerm)
			rf.unlock()

			if ok {

				lastTerm = newCurrentTerm

				log.Printf("[WorkerId %v][Term %v][State: %v]: I will increase current term by 1...Success\n",
					workerId,
					lastTerm,
					raft_state.ConvertToStateString(state))

			} else {

				rf.mu.Lock()
				rf.setState(raft_state.FOLLOWER)
				lastTerm = rf.currentTerm
				rf.mu.Unlock()

				log.Printf("[WorkerId %v][Term %v][State: %v]: I will increate current term by 1...Fail...as a result I will change to FOLLOWER\n",
					workerId,
					lastTerm,
					raft_state.ConvertToStateString(state))

				continue
			}

			voteCurrentResult := vote_current_result.NewVoteCurrentResult(
				peersSize,
				election_result.NewElectionResult(),
				lastTerm,
				workerId,
				state,
			)

			voteMonitors := make([]interface{}, 0)

			for peerId, peer := range rf.peers {
				voteMonitors = append(
					voteMonitors,
					vote_monitor.NewVoteMonitor(peer, peerId, vote_monitor_required_params.NewRaft(
						workerId,
						lastTerm,
						state,
						rf,
					)),
				)
			}

			election_process.NewElection(
				election_process_required_params.NewRaft(
					workerId,
					lastTerm,
					state,
					clock_,
					peersSize,
					peers,
					theLog,
					nextIndexSize,
					matchIndexSize,
					rf,
					voteFor,
				),
				voteMonitors,
				voteCurrentResult,
				election_consideration_builder_api_for.NewBuilder(voteCurrentResult),
			).StartElection()

			if voteCurrentResult.ElectionResult().(raft_talk_to.ElectionResult).Accept() {
				rf.mu.Lock()
				rf.setState(raft_state.LEADER)
				rf.mu.Unlock()
			}

			continue
		}

		currentState = raft_state.FOLLOWER

		//log.Printf(
		//	"[WorkerId %v][Term %v][State %v]: I'm a FOLLOWER\n",
		//	workerId,
		//	currentTerm,
		//	raft_state.ConvertToStateString(state),
		//)

		lastTerm = currentTerm

		heart_beat_from_leader_listener.NewHeartbeatListener(
			heart_beat_listener_required_params.NewRaft(
				lastTerm,
				workerId,
				state,
				theLog,
				clock_,
				rf,
			),
		).HearHeartbeat()

		continue
	}

	rf.lock()
	workerId := rf.me
	currentTerm := rf.currentTerm
	state := rf.state
	rf.unlock()

	log.Printf("[WorkerId %v][Term %v][State: %v]: I was killed by my boss\n",
		workerId,
		currentTerm,
		raft_state.ConvertToStateString(state),
	)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// disable log_collection_type output
	//log.SetOutput(io.Discard)

	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.log = log_collection.NewLogCollection()
	rf.clock = clock.NewClock(rf.me)
	rf.leaderTerm = make([]int, 0)
	rf.stateChangeSubscribers = make([]raft_talk_to.LimitedTimeUsageChannel, 0)
	rf.currentTermSubscribers = make([]raft_talk_to.LimitedTimeUsageChannel, 0)

	rf.state = raft_state.FOLLOWER

	log.Printf("[WorkerId %v][Term %v][State: %v]: start a new raft server. I will be a FOLLOWER\n",
		rf.me,
		rf.currentTerm,
		raft_state.ConvertToStateString(rf.state),
	)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.logChangesSubscribe()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

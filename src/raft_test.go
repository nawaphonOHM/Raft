package raft

//
// Raft tests.
//
// we will use the original raft_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	tester "raft/libs/tester1"

	// "log"
	"math/rand"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection3A(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	tester.AnnotateTest("TestInitialElection3A", servers)
	ts.Begin("Test (3A): initial election_process")

	// is a leader elected?
	ts.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election_process, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := ts.checkTerms()
	if term1 < 1 {
		ts.t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := ts.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	ts.checkOneLeader()
}

func TestReElection3A(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	tester.AnnotateTest("TestReElection3A", servers)
	ts.Begin("Test (3A): election_process after network failure")

	leader1 := ts.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	ts.g.DisconnectAll(leader1)
	tester.AnnotateConnection(ts.g.GetConnected())
	ts.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	ts.g.ConnectOne(leader1)
	tester.AnnotateConnection(ts.g.GetConnected())
	leader2 := ts.checkOneLeader()

	// if there's no quorum, no new leader should
	// be elected.
	ts.g.DisconnectAll(leader2)
	ts.g.DisconnectAll((leader2 + 1) % servers)
	tester.AnnotateConnection(ts.g.GetConnected())
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	ts.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	ts.g.ConnectOne((leader2 + 1) % servers)
	tester.AnnotateConnection(ts.g.GetConnected())
	ts.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	ts.g.ConnectOne(leader2)
	tester.AnnotateConnection(ts.g.GetConnected())
	ts.checkOneLeader()
}

func TestManyElections3A(t *testing.T) {
	servers := 7
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	tester.AnnotateTest("TestManyElection3A", servers)
	ts.Begin("Test (3A): multiple elections")

	ts.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// disconnect three nodes
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		ts.g.DisconnectAll(i1)
		ts.g.DisconnectAll(i2)
		ts.g.DisconnectAll(i3)
		tester.AnnotateConnection(ts.g.GetConnected())

		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		ts.checkOneLeader()

		ts.g.ConnectOne(i1)
		ts.g.ConnectOne(i2)
		ts.g.ConnectOne(i3)
		tester.AnnotateConnection(ts.g.GetConnected())
	}
	ts.checkOneLeader()
}

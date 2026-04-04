package clock

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"raft/src/clock/clock_state"
	"raft/src/clock/clock_type"
	"raft/src/clock/main_interface"
)

type clock struct {
	electionTime int
	cycle        int

	state clock_type.State

	lock sync.Mutex

	machine interface{}

	timeOutSub map[int][]chan<- bool

	sequenceNumber int
}

func (c *clock) StartNewClockCycle(timeoutChannel chan<- bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cycle = c.cycle + 1
	c.state = clock_state.READY

	if cap(timeoutChannel) != 1 {
		log.Fatalf("[WorkerId %v]: Expected 1-buffered channel for timeout subscription.", c.machine)
	}

	if _, ok := c.timeOutSub[c.cycle]; !ok {
		c.timeOutSub[c.cycle] = make([]chan<- bool, 0)
	}

	c.timeOutSub[c.cycle] = append(c.timeOutSub[c.cycle], timeoutChannel)

	c.countDown()
}

func (c *clock) State() clock_type.State {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.state
}

func genElectionTime() int {
	return 150 + rand.Intn(150+1)
}

func (c *clock) countDown() {

	c.electionTime = genElectionTime()

	go func(self *clock, myId int) {
		self.lock.Lock()

		self.state = clock_state.CountingDown

		cycle := self.cycle

		timeout := time.Duration(self.electionTime) * time.Millisecond

		log.Printf("[WorkerId %v][Clock thread Id %v][TimeOut %v]: state transition to COUNTING_DOWN",
			self.machine,
			myId,
			timeout,
		)

		self.lock.Unlock()

		time.Sleep(timeout)

		self.lock.Lock()

		if cycle == self.cycle {
			c.state = clock_state.TIMEOUT
			log.Printf("[WorkerId %v][Clock thread Id %v][TimeOut %v]: state transition to TIMEOUT",
				self.machine,
				myId,
				timeout,
			)
		} else {
			log.Printf("[WorkerId %v][Clock thread Id %v][TimeOut %v]: I notice that clock cycles have changed while I did countdown. Don't change state",
				self.machine,
				myId,
				timeout,
			)
		}

		if subscribers, hasKey := c.timeOutSub[cycle]; hasKey {

			for _, subscriber := range subscribers {
				subscriber <- true
				close(subscriber)
			}

			delete(c.timeOutSub, cycle)
		}

		c.lock.Unlock()

	}(c, c.sequenceNumber)

	c.sequenceNumber = c.sequenceNumber + 1
}

func NewClock(machine int) main_interface.Clock {

	return &clock{
		state:          clock_state.NOP,
		cycle:          0,
		machine:        machine,
		timeOutSub:     make(map[int][]chan<- bool),
		electionTime:   genElectionTime(),
		sequenceNumber: 0,
	}
}

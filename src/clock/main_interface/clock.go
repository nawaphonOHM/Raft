package main_interface

import (
	"raft/src/clock/clock_type"
)

type Clock interface {
	State() clock_type.State
	StartNewClockCycle(timeoutChannel chan<- bool)
}

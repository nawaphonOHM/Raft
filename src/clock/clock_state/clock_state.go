package clock_state

import (
	"raft/src/clock/clock_type"
)

const (
	READY clock_type.State = iota
	CountingDown
	TIMEOUT
	NOP
)

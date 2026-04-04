package limited_time_usage_channel

import (
	"log"

	"raft/src/limited_time_usage_channel/main_interface"
)

type limitedTimeUsageChannel struct {
	live int

	ch chan interface{}
}

func (l *limitedTimeUsageChannel) Notify(data ...interface{}) {

	if len(data) > 1 {
		log.Fatalf("[Notify]: only one data is allowed")
	}

	if l.live == 0 {
		log.Fatalf("[Notify]: the channel is broken")
	}

	l.live--

	if len(data) == 1 {
		l.ch <- data[0]

	} else {
		l.ch <- true
	}

	l.considerToCloseChannel()
}

func (l *limitedTimeUsageChannel) Channel() <-chan interface{} {

	return l.ch

}

func (l *limitedTimeUsageChannel) considerToCloseChannel() {

	if l.live == 0 {
		close(l.ch)
	}

}

func (l *limitedTimeUsageChannel) IsBroken() bool {

	return l.live == 0
}

func NewLimitedTimeUsageChannel(live int) main_interface.LimitedTimeUsageChannel {

	if live <= 0 {
		log.Fatalf("live must be greater than 0")
	}

	return &limitedTimeUsageChannel{live: live, ch: make(chan interface{}, live)}
}

package unlimited_time_usage_channel

import (
	"log"

	"raft/src/unlimited_time_usage_channel/main_interface"
)

type unlimitedTimeUsageChannel struct {
	ch chan interface{}
}

func (c *unlimitedTimeUsageChannel) Notify(data ...interface{}) {

	if len(data) > 1 {
		log.Fatalf("only one data is allowed")
	}

	if len(data) == 1 {
		c.ch <- data[0]
	} else {
		c.ch <- true
	}
}

func (c *unlimitedTimeUsageChannel) Close() {
	close(c.ch)
}

func (c *unlimitedTimeUsageChannel) Channel() <-chan interface{} {
	return c.ch
}

func NewUnlimitedTimeUsageChannel() main_interface.UnlimitedTimeUsageChannel {
	return &unlimitedTimeUsageChannel{ch: make(chan interface{}, 1)}
}

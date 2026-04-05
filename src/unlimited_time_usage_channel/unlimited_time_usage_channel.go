package unlimited_time_usage_channel

import (
	"log"
	"sync"

	"raft/src/unlimited_time_usage_channel/main_interface"
)

type unlimitedTimeUsageChannel struct {
	ch chan interface{}

	isBroken bool

	lock sync.Mutex
}

func (c *unlimitedTimeUsageChannel) Notify(data ...interface{}) {

	if len(data) > 1 {
		log.Fatalf("only one data is allowed")
	}

	c.lock.Lock()
	if len(data) == 1 {
		c.ch <- data[0]
	} else {
		c.ch <- true
	}
	c.lock.Unlock()
}

func (c *unlimitedTimeUsageChannel) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()

	close(c.ch)
	c.isBroken = true
}

func (c *unlimitedTimeUsageChannel) Channel() <-chan interface{} {
	return c.ch
}

func (c *unlimitedTimeUsageChannel) IsBroken() bool {
	return c.isBroken
}

func NewUnlimitedTimeUsageChannel() main_interface.UnlimitedTimeUsageChannel {
	return &unlimitedTimeUsageChannel{ch: make(chan interface{}, 1), isBroken: false}
}

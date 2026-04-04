package main_interface

type LimitedTimeUsageChannel interface {
	Notify(data ...interface{})
	Channel() <-chan interface{}
	IsBroken() bool
}

package main_interface

type UnlimitedTimeUsageChannel interface {
	Notify(data ...interface{})
	Close()
	Channel() <-chan interface{}
	IsBroken() bool
}

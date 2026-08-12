package main_interface

type UnlimitedTimeUsageChannel interface {
	Notify(data ...any)
	Close()
	Channel() <-chan any
	IsBroken() bool
}

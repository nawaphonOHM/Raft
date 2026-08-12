package main_interface

type LimitedTimeUsageChannel interface {
	Notify(data ...any)
	Channel() <-chan any
	IsBroken() bool
}

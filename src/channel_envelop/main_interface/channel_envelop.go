package main_interface

type ChannelInterface interface {
	IsBroken() bool
	Notify(data ...any)
	Close()
	Channel() <-chan any
}

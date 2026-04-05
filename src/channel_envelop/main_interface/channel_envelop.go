package main_interface

type ChannelInterface interface {
	IsBroken() bool
	Notify(data ...interface{})
	Close()
	Channel() <-chan interface{}
}

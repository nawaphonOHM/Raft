package main_interface

type Log interface {
	Term() int
	Command() string
}

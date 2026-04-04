package log

import (
	"raft/src/log_collection/log/main_interface"
)

type log struct {
	command string
	term    int
}

func (l *log) Command() string {
	return l.command
}

func (l *log) Term() int {

	return l.term
}

func NewLog(command string, term int) main_interface.Log {
	return &log{command: command, term: term}
}

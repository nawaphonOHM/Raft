package log_api_for

type LogCollection interface {
	Term() int
	Command() string
}

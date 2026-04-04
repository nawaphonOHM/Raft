package limited_time_usage_channel_api_for

type ElectionProcess interface {
	Notify(data ...interface{})
	Channel() <-chan interface{}
}

package limited_time_usage_channel_api_for

type ElectionConsideration interface {
	Channel() <-chan interface{}
}

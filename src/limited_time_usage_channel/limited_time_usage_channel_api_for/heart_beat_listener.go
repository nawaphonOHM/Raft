package limited_time_usage_channel_api_for

type LimitedTimeUsageChannel interface {
	Channel() <-chan interface{}
}

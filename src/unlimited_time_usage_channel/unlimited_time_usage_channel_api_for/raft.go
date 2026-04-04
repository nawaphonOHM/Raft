package unlimited_time_usage_channel_api_for

type Raft interface {
	Channel() <-chan interface{}
}

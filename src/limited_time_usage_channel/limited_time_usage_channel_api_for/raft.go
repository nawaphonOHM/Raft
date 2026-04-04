package limited_time_usage_channel_api_for

type Raft interface {
	Notify(data ...interface{})
	IsBroken() bool
}

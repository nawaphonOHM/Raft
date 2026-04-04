package limited_time_usage_channel_api_for

type VoteMonitor interface {
	Notify(data ...interface{})
}

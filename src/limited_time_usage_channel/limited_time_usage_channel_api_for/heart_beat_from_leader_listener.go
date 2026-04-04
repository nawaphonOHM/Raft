package limited_time_usage_channel_api_for

type HeartBeatFromLeaderListener interface {
	Notify(data ...interface{})
	IsBroken() bool
}

package limited_time_usage_channel_api_for

type LogCollection interface {
	Notify(data ...interface{})
	IsBroken() bool
}

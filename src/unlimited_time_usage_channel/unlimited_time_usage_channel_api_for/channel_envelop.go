package unlimited_time_usage_channel_api_for

type ChannelEnvelop interface {
	Notify(data ...interface{})
	Close()
}

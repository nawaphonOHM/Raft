package channel_envelop_api_for

type LogCollection interface {
	Notify(data ...interface{})
	IsBroken() bool
}

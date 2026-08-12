package channel_envelop_api_for

type LogCollection interface {
	Notify(data ...any)
	IsBroken() bool
}

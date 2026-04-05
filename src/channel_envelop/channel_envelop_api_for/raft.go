package channel_envelop_api_for

type Raft interface {
	IsBroken() bool
	Notify(data ...interface{})
}

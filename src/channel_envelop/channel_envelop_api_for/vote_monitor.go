package channel_envelop_api_for

type VoteMonitor interface {
	Close()
	Channel() <-chan interface{}
}

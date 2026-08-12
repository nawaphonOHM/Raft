package log_collection_api_for

type HeartBeatListener interface {
	SubscribeLogChange(channel any, logSize int) bool
	Size() int
}

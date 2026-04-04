package log_collection_api_for

type HeartBeatListener interface {
	SubscribeLogChange(channel interface{}, logSize int) bool
	Size() int
}

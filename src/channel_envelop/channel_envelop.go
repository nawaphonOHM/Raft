package channel_envelop

import (
	"fmt"
	"log"

	"raft/src/channel_envelop/internal_used_interfaace"
	"raft/src/channel_envelop/main_interface"
	"raft/src/limited_time_usage_channel/limited_time_usage_channel_api_for"
	"raft/src/unlimited_time_usage_channel/unlimited_time_usage_channel_api_for"
)

type channelEnvelop struct {
	instance any
}

func (e *channelEnvelop) Notify(data ...any) {

	e.instance.(internal_used_interfaace.UnionBehavioral).Notify(data...)

}

func (e *channelEnvelop) IsBroken() bool {
	return e.instance.(internal_used_interfaace.UnionBehavioral).IsBroken()
}

func (e *channelEnvelop) Close() {
	e.instance.(unlimited_time_usage_channel_api_for.ChannelEnvelop).Close()
}

func (e *channelEnvelop) Channel() <-chan any {
	return e.instance.(internal_used_interfaace.UnionBehavioral).Channel()
}

func NewChannelEnvelop(instance any) main_interface.ChannelInterface {

	var type__ any

	switch type_ := instance.(type) {
	case nil:
		{
			log.Fatalf("instance must not be a nil")
		}
	case limited_time_usage_channel_api_for.ChannelEnvelop:
		{
			return &channelEnvelop{instance: instance.(limited_time_usage_channel_api_for.ChannelEnvelop)}
		}
	case unlimited_time_usage_channel_api_for.ChannelEnvelop:
		{
			return &channelEnvelop{instance: instance.(unlimited_time_usage_channel_api_for.ChannelEnvelop)}
		}

	default:
		{
			type__ = type_
		}

	}

	panic(fmt.Sprintf("expected type: LimitedTimeUsageChannel or UnlimitedTimeUsageChannel, but got: %T", type__))

}

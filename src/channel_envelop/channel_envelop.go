package channel_envelop

import (
	"fmt"
	"log"

	"raft/src/channel_envelop/internal_used_interfaace"
	"raft/src/channel_envelop/main_interface"
	"raft/src/channel_envelop/talk_to"
)

type channelEnvelop struct {
	instance interface{}
}

func (e *channelEnvelop) Notify(data ...interface{}) {

	e.instance.(internal_used_interfaace.UnionBehavioral).Notify(data...)

}

func (e *channelEnvelop) IsBroken() bool {
	if instance_, ok := e.instance.(talk_to.LimitedTimeUsageChannel); ok {
		return instance_.IsBroken()
	}
	return false
}

func NewChannelEnvelop(instance interface{}) main_interface.ChannelInterface {

	var type__ interface{}

	switch type_ := instance.(type) {
	case nil:
		{
			log.Fatalf("instance must not be a nil")
		}
	case talk_to.LimitedTimeUsageChannel:
		{
			return &channelEnvelop{instance: instance.(talk_to.LimitedTimeUsageChannel)}
		}
	case talk_to.UnlimitedTimeUsageChannel:
		{
			return &channelEnvelop{instance: instance.(talk_to.UnlimitedTimeUsageChannel)}
		}

	default:
		{
			type__ = type_
		}

	}

	panic(fmt.Sprintf("expected type: LimitedTimeUsageChannel or UnlimitedTimeUsageChannel, but got: %T", type__))

}

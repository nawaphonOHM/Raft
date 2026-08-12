package channel_envelop

import (
	"fmt"
	"log"

	"raft/src/channel_envelop/internal_used_interfaace"
	"raft/src/channel_envelop/main_interface"
	"raft/src/channel_envelop/talk_to"
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
	e.instance.(talk_to.UnlimitedTimeUsageChannel).Close()
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

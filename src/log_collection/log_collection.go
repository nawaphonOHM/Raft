package log_collection

import (
	"log"
	"sync"

	"raft/src/channel_envelop"
	"raft/src/dto"
	"raft/src/errors"
	main_interface3 "raft/src/limited_time_usage_channel/main_interface"
	"raft/src/log_collection/main_interface"
	"raft/src/log_collection/talk_to"
	main_interface2 "raft/src/unlimited_time_usage_channel/main_interface"
)

type logCollection struct {
	log []talk_to.Log

	logChangeSubscribers []talk_to.ChannelEnvelop

	mutex sync.Mutex
}

func (l *logCollection) GetLatestLogIndexMinusOneAndLatestTermMinusOne() *dto.LatestLogIndexMinusOneAndLatestLogMinusOne {

	l.mutex.Lock()
	defer l.mutex.Unlock()

	size := l.size()

	if size == 0 {
		return dto.NewLatestLogIndexMinusOneAndLatestLogMinusOne(-1, -1, errors.NewItHasNoLogError())
	}

	if size == 1 {
		return dto.NewLatestLogIndexMinusOneAndLatestLogMinusOne(
			0,
			l.log[0].Term(),
			errors.NewItHasOnlyOneLogError(),
		)
	}

	correctIndex := l.size() - 1 - 1

	correctTermNumber := l.logAt(correctIndex).Term()

	return dto.NewLatestLogIndexMinusOneAndLatestLogMinusOne(correctIndex, correctTermNumber)
}

func (l *logCollection) logAt(index int) talk_to.Log {
	return l.log[index]
}

func (l *logCollection) LogAt(index int) talk_to.Log {

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index >= len(l.log) {
		return nil
	}

	return l.log[index]
}

func (l *logCollection) Validate(candidateTerm int, index int) (bool, error) {

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index < 0 {
		return true, nil
	}

	if index >= l.size() {
		return false, errors.NewCannotAccessError()
	}

	pass := candidateTerm == l.log[index].Term()

	if pass {
		return true, nil
	}

	return false, errors.NewCandidateTermMismatch(candidateTerm, l.log[index].Term())

}

func (l *logCollection) Add(newLog talk_to.Log) {

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.log = append(l.log, newLog)

	l.publishChangesToSubscribers(l.size() - 1)
	l.decideToRemoveBrokenSubscribers()
}

func (l *logCollection) decideToRemoveBrokenSubscribers() {
	newSubscribersArea := make([]talk_to.ChannelEnvelop, 0)

	for _, subscriber := range l.logChangeSubscribers {
		if !subscriber.IsBroken() {
			newSubscribersArea = append(newSubscribersArea, subscriber)
		}
	}

	l.logChangeSubscribers = newSubscribersArea
}

func (l *logCollection) publishChangesToSubscribers(latestIndex int) {

	for _, subscriber := range l.logChangeSubscribers {
		if !subscriber.IsBroken() {
			subscriber.Notify(latestIndex)
		}
	}

}

func (l *logCollection) SubscribeLogChange(
	channel interface{},
	logSize int,
) bool {

	switch channel.(type) {
	case nil:
		{
			log.Fatalf("channel must not be nil")
		}
	case main_interface3.LimitedTimeUsageChannel:
	case main_interface2.UnlimitedTimeUsageChannel:
	default:
		{
			log.Fatalf("channel must be LimitedTimeUsageChannel or UnlimitedTimeUsageChannel")
		}
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if logSize != l.size() {
		return false
	}

	l.logChangeSubscribers = append(l.logChangeSubscribers, channel_envelop.NewChannelEnvelop(channel))

	return true
}

func (l *logCollection) size() int {
	return len(l.log)
}

func (l *logCollection) Size() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return len(l.log)
}

func NewLogCollection() main_interface.LogCollection {
	return &logCollection{
		log:                  make([]talk_to.Log, 0),
		logChangeSubscribers: make([]talk_to.ChannelEnvelop, 0),
	}
}

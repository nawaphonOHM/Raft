package clock_api_for

type HeartBeatFromLeaderListener interface {
	StartNewClockCycle(timeoutChannel chan<- bool)
}

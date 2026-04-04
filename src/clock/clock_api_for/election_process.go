package clock_api_for

type ElectionProcess interface {
	StartNewClockCycle(timeoutChannel chan<- bool)
}

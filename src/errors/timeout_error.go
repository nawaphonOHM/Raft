package errors

type TimeOutError struct {
}

func (t *TimeOutError) Error() string {
	return "Timeout"
}

func NewTimeOutError() error {
	return &TimeOutError{}
}

func NewTimeOutErrorImplementationForComparingType() **TimeOutError {
	result := &TimeOutError{}

	return &result
}

package errors

type ItHasNoLogError struct {
}

func (i *ItHasNoLogError) Error() string {
	return "It has no log error"
}

func NewItHasNoLogError() error {
	return &ItHasNoLogError{}
}

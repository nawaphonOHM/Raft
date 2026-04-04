package errors

type ItHasOnlyOneLogError struct {
}

func (i *ItHasOnlyOneLogError) Error() string {
	return "It has only one log error"
}

func NewItHasOnlyOneLogError() error {
	return &ItHasOnlyOneLogError{}
}

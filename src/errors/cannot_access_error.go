package errors

type CannotAccessError struct {
}

func (c *CannotAccessError) Error() string {

	return "Cannot access log_collection_type"
}

func NewCannotAccessError() error {
	return &CannotAccessError{}
}

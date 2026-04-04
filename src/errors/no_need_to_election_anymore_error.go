package errors

type NoNeedToElectionAnymoreError struct {
}

func (n *NoNeedToElectionAnymoreError) Error() string {
	return "No need to election_process anymore"
}

func NewNoNeedToElectionAnymoreError() error {
	return &NoNeedToElectionAnymoreError{}
}

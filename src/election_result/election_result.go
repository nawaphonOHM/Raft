package election_result

import "raft/src/election_result/main_interface"

type electionResult struct {
	accept bool
	why    error
}

func (e *electionResult) SetAccept(accept bool) {
	e.accept = accept
}

func (e *electionResult) SetWhy(why error) {
	e.why = why
}

func (e *electionResult) Accept() bool {
	return e.accept
}

func (e *electionResult) Why() error {
	return e.why
}

func NewElectionResult() main_interface.ElectionResult {
	return &electionResult{}
}

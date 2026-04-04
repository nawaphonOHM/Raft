package election_result_api_for

type Raft interface {
	Accept() bool
}

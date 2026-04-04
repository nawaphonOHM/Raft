package election_result_api_for

type ElectionProcess interface {
	Accept() bool
	Why() error
}

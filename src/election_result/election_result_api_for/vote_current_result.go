package election_result_api_for

type VoteCurrentResult interface {
	SetAccept(accept bool)
	SetWhy(why error)
}

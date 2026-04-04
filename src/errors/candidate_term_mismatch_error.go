package errors

import (
	"fmt"
)

type CandidateTermMismatchError struct {
	gotTerm     int
	currentTerm int
}

func (c *CandidateTermMismatchError) Error() string {

	return fmt.Sprintf(
		"Candidate's term is not the same as the current term got %d, current term is %d",
		c.gotTerm,
		c.currentTerm,
	)
}

func NewCandidateTermMismatch(gotTerm int, currenTerm int) error {
	return &CandidateTermMismatchError{gotTerm: gotTerm, currentTerm: currenTerm}
}

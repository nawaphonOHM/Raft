package dto

import (
	"log"
)

type LatestLogIndexMinusOneAndLatestLogMinusOne struct {
	LogIndex  int
	TermIndex int

	Err error
}

func NewLatestLogIndexMinusOneAndLatestLogMinusOne(
	logIndex int,
	termIndex int,
	err ...error,
) *LatestLogIndexMinusOneAndLatestLogMinusOne {

	if len(err) > 1 {
		log.Fatalf("only one error is allowed")
	}

	if len(err) == 0 {
		return &LatestLogIndexMinusOneAndLatestLogMinusOne{LogIndex: logIndex, TermIndex: termIndex, Err: nil}
	}

	return &LatestLogIndexMinusOneAndLatestLogMinusOne{LogIndex: logIndex, TermIndex: termIndex, Err: err[0]}
}

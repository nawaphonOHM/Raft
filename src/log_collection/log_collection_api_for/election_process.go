package log_collection_api_for

import (
	"raft/src/log_collection/talk_to"
)

type ElectionProcess interface {
	Size() int
	LogAt(index int) talk_to.Log
}

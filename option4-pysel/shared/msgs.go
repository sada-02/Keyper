package shared

import (
	"github.com/pysel/dkvs/prototypes"
	pbpartition "github.com/pysel/dkvs/prototypes/partition"
)

func NewPrepareCommitMessage_Set(key, value []byte, lamport uint64) *pbpartition.PrepareCommitRequest {
	return &pbpartition.PrepareCommitRequest{
		Message: &pbpartition.PrepareCommitRequest_Set{
			Set: &prototypes.SetRequest{
				Key:     key,
				Value:   value,
				Lamport: lamport,
			},
		},
	}
}

func NewPrepareCommitMessage_Delete(key []byte, lamport uint64) *pbpartition.PrepareCommitRequest {
	return &pbpartition.PrepareCommitRequest{
		Message: &pbpartition.PrepareCommitRequest_Delete{
			Delete: &prototypes.DeleteRequest{
				Key:     key,
				Lamport: lamport,
			},
		},
	}
}

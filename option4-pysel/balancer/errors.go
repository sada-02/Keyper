package balancer

import (
	"errors"
	"fmt"

	"github.com/pysel/dkvs/shared"
)

var (
	// General Balancer errors
	ErrPartitionOverflow        = errors.New("enough partitions are already registered")
	ErrCoverageNotProperlySetUp = errors.New("coverage is not properly set up")
	ErrDigestNotCovered         = errors.New("digest is not covered by any range")
	ErrRangeNotYetCovered       = errors.New("range is not yet covered by any partition")
	ErrAllReplicasFailed        = errors.New("all replicas failed to process request")

	// 2PC
	ErrPrepareCommitAborted = errors.New("prepare commit aborted")
)

// ErrDecisionNotSavedToDisk is returned when a balancer's decision was not saved to disk during 2PC.
type ErrDecisionNotSavedToDisk struct {
	Reason   error
	Decision []byte
}

func (e ErrDecisionNotSavedToDisk) Error() string {
	return "decision not saved to disk: " + e.Reason.Error()
}

func (e ErrDecisionNotSavedToDisk) Unwrap() error {
	return e.Reason
}

// ErrDecisionWasNotCleared is returned when a balancer's decision was not cleared from disk after a two-phase commit has ended.
type ErrDecisionWasNotCleared struct {
	Reason error
}

func (e ErrDecisionWasNotCleared) Error() string {
	return "decision was not cleared from disk after a two-phase commit has ended: " + e.Reason.Error()
}

func (e ErrDecisionWasNotCleared) Unwrap() error {
	return e.Reason
}

type ErrPartitionsOffline struct {
	Addresses []string
	Errors    []error
}

func (e ErrPartitionsOffline) Error() string {
	return "partitions offline: " + e.Errors[0].Error()
}

// ErrOrNil returns nil if there were no partitions offline, self otherwise.
func (e ErrPartitionsOffline) ErrOrNil() error {
	if len(e.Errors) == 0 {
		return nil
	}

	return e
}

type ErrNotReadyForRequest struct {
	ClientId                uint64
	CurrentClientTimestamp  uint64
	ReceivedClientTimestamp uint64
}

func (e ErrNotReadyForRequest) Error() string {
	return fmt.Sprintf("client {%d}: received timestamp {%d} is not the next one, current timestamp: {%d}", e.ClientId, e.ReceivedClientTimestamp, e.CurrentClientTimestamp)
}

func (e ErrNotReadyForRequest) WarningErrorToEvent(req string) shared.Event {
	return &RequestWithUnexpectedTimestampEvent{
		ClientId: e.ClientId,
		Expected: e.CurrentClientTimestamp + 1,
		Received: e.ReceivedClientTimestamp,
	}
}

type ErrCommitAborted struct {
	Err error
}

func (e ErrCommitAborted) Error() string {
	return shared.RedWrap("commit aborted: " + e.Err.Error())
}

func (e ErrCommitAborted) Unwrap() error {
	return e.Err
}

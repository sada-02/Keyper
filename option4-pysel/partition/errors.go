package partition

import (
	"errors"
	"fmt"

	"github.com/pysel/dkvs/shared"
)

var (
	ErrNotThisPartitionKey = errors.New("a key provided is not in this partition's range")
	ErrInvalidKeySize      = errors.New("key size should be 32 bytes")
	ErrUnsupported2PCMsg   = errors.New("unsupported 2PC message")
	ErrNoLockedMessage     = errors.New("no locked message")
)

type (
	// ErrTimestampNotNext is returned when a timestamp of a received request is not the current timestamp + 1
	ErrTimestampNotNext struct {
		CurrentTimestamp  uint64
		ReceivedTimestamp uint64
	}

	// ErrInternal is returned when an internal error occurs during attempt to serve a request
	ErrInternal struct {
		Reason error
	}

	ErrTimestampIsStale struct {
		CurrentTimestamp uint64
		StaleTimestamp   uint64
	}
)

func (e ErrTimestampNotNext) Error() string {
	return fmt.Sprintf("timestamp is not the next one, current timestamp: %d", e.CurrentTimestamp)
}

func (e ErrTimestampNotNext) WarningErrorToEvent(req string) shared.Event {
	return NotNextRequestEvent{
		req:               req,
		currentTimestamp:  e.CurrentTimestamp,
		receivedTimestamp: e.ReceivedTimestamp,
	}
}

func (e ErrInternal) Error() string {
	return fmt.Sprintf("internal error: %s", e.Reason.Error())
}

func (e ErrInternal) Unwrap() error {
	return e.Reason
}

func (e ErrTimestampIsStale) Error() string {
	return fmt.Sprintf("timestamp is stale, current timestamp: %d, received timestamp: %d", e.CurrentTimestamp, e.StaleTimestamp)
}

func (e ErrTimestampIsStale) WarningErrorToEvent(req string) shared.Event {
	return StaleRequestEvent{
		req:               req,
		currentTimestamp:  e.CurrentTimestamp,
		receivedTimestamp: e.StaleTimestamp,
	}
}

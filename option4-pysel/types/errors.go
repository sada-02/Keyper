package types

import "errors"

var (
	ErrNilKey            = errors.New("key is nil")
	ErrNilValue          = errors.New("value is nil")
	ErrNilRequest        = errors.New("request is nil")
	ErrFailedToSetString = errors.New("failed to set string")
)

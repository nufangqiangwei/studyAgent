package persistence

import "errors"

var (
	ErrSequenceConflict = errors.New("stream sequence conflict")
	ErrStaleActivation  = errors.New("stale activation")
	ErrLeaseLost        = errors.New("lease lost")
	ErrDuplicateID      = errors.New("durable id conflict")
	ErrClosed           = errors.New("runtime storage is closed")
)

package fault

import (
	"agent/serviceruntime/contract"
	"errors"
	"fmt"
)

type Kind string

const (
	Validation      Kind = "validation"
	NotFound        Kind = "not_found"
	Conflict        Kind = "conflict"
	StaleActivation Kind = "stale_activation"
	LeaseLost       Kind = "lease_lost"
	Deferred        Kind = "deferred"
	Retryable       Kind = "retryable"
	Permanent       Kind = "permanent"
	CorruptState    Kind = "corrupt_state"
	ReconcileNeeded Kind = "reconciliation_required"
)

type RuntimeError struct {
	Kind       Kind
	Operation  string
	RuntimeID  contract.RuntimeID
	InstanceID contract.ServiceInstanceID
	MessageID  string
	EffectID   string
	Retryable  bool
	Cause      error
}

func (e *RuntimeError) Error() string {
	if e == nil {
		return "runtime error"
	}
	message := string(e.Kind)
	if e.Operation != "" {
		message = e.Operation + ": " + message
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *RuntimeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func New(kind Kind, operation string, cause error) *RuntimeError {
	return &RuntimeError{
		Kind:      kind,
		Operation: operation,
		Retryable: kind == Retryable || kind == Deferred || kind == Conflict || kind == StaleActivation || kind == LeaseLost,
		Cause:     cause,
	}
}

func Wrap(kind Kind, operation string, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("%s", kind)
	}
	var existing *RuntimeError
	if errors.As(cause, &existing) && existing.Kind == kind && existing.Operation == operation {
		return cause
	}
	return New(kind, operation, cause)
}

func KindOf(err error) Kind {
	var runtimeErr *RuntimeError
	if errors.As(err, &runtimeErr) {
		return runtimeErr.Kind
	}
	return Retryable
}

func IsKind(err error, kind Kind) bool {
	return KindOf(err) == kind
}

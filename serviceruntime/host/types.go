package host

import (
	"agent/serviceruntime/contract"
	"context"
)

type HandleStatus string

const (
	HandleIdle       HandleStatus = "idle"
	HandleCommitted  HandleStatus = "committed"
	HandleDuplicate  HandleStatus = "duplicate"
	HandleRetry      HandleStatus = "retry"
	HandleDeadLetter HandleStatus = "dead_letter"
	HandleStale      HandleStatus = "stale_activation"
	HandleCorrupt    HandleStatus = "corrupt_state"
)

type HandleResult struct {
	Status       HandleStatus
	InstanceID   contract.ServiceInstanceID
	MessageID    string
	StreamID     contract.StreamID
	LastSequence uint64
	EventIDs     []string
	OutboxIDs    []string
	EffectIDs    []string
}

type Host interface {
	Start(ctx context.Context) error
	HandleNext(ctx context.Context, instanceID contract.ServiceInstanceID) (HandleResult, error)
	Drain(ctx context.Context) error
	Stop(ctx context.Context) error
}

type SnapshotDecision struct {
	InstanceID       contract.ServiceInstanceID
	StreamID         contract.StreamID
	PreviousSequence uint64
	CurrentSequence  uint64
	EventsApplied    int
	StateBytes       int
}

type SnapshotPolicy interface {
	ShouldSnapshot(input SnapshotDecision) bool
}

type EveryNEvents struct {
	N uint64
}

func (p EveryNEvents) ShouldSnapshot(input SnapshotDecision) bool {
	return p.N > 0 && input.CurrentSequence > 0 && input.CurrentSequence/p.N > input.PreviousSequence/p.N
}

package state

import (
	"context"

	runtimeevent "agent/internal/event"
)

type StateStore interface {
	Load(ctx context.Context, runID string) (RunState, error)
	Save(ctx context.Context, state RunState) error
}

type EventStore interface {
	Append(ctx context.Context, event runtimeevent.Event) error
	List(ctx context.Context, runID string) ([]runtimeevent.Event, error)
}

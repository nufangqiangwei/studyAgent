package persistence

import (
	"agent/internal/runtime/eventbus"
	"context"
	"fmt"
)

type EventRecorder struct {
	store EventStore
}

func NewEventRecorder(store EventStore) (*EventRecorder, error) {
	if store == nil {
		return nil, fmt.Errorf("event recorder: store is required")
	}
	return &EventRecorder{store: store}, nil
}

func (r *EventRecorder) HandleEvent(ctx context.Context, event eventbus.Event) error {
	if r == nil || r.store == nil {
		return fmt.Errorf("event recorder is nil")
	}
	_, err := r.store.Append(ctx, event)
	return err
}

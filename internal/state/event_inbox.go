package state

import (
	"context"
	"fmt"
	"time"

	runtimeevent "agent/internal/event"
)

type EventInboxStatus string

const (
	EventInboxStatusPending   EventInboxStatus = "pending"
	EventInboxStatusClaimed   EventInboxStatus = "claimed"
	EventInboxStatusProcessed EventInboxStatus = "processed"
	EventInboxStatusFailed    EventInboxStatus = "failed"
)

type StoredEvent struct {
	Event       runtimeevent.Event `json:"event"`
	Status      EventInboxStatus   `json:"status"`
	Error       string             `json:"error,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	ClaimedAt   *time.Time         `json:"claimed_at,omitempty"`
	ProcessedAt *time.Time         `json:"processed_at,omitempty"`
	FailedAt    *time.Time         `json:"failed_at,omitempty"`
}

type EventInboxStore interface {
	Append(ctx context.Context, event runtimeevent.Event) (StoredEvent, bool, error)
	ListPending(ctx context.Context, runID string) ([]StoredEvent, error)
	Claim(ctx context.Context, runID string) (StoredEvent, bool, error)
	MarkProcessed(ctx context.Context, eventID string) error
	MarkFailed(ctx context.Context, eventID string, cause error) error
}

func (e StoredEvent) Clone() StoredEvent {
	cloned := e
	cloned.Event = e.Event.Clone()
	cloned.ClaimedAt = cloneTimePtr(e.ClaimedAt)
	cloned.ProcessedAt = cloneTimePtr(e.ProcessedAt)
	cloned.FailedAt = cloneTimePtr(e.FailedAt)
	return cloned
}

func normalizeStoredEvent(event runtimeevent.Event, now time.Time) (StoredEvent, error) {
	if event.ID == "" {
		return StoredEvent{}, fmt.Errorf("event id is required")
	}
	if event.RunID == "" {
		return StoredEvent{}, fmt.Errorf("event run_id is required")
	}
	if event.Type == "" {
		return StoredEvent{}, fmt.Errorf("event type is required")
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = now
	}
	return StoredEvent{
		Event:     event.Clone(),
		Status:    EventInboxStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func (s EventInboxStatus) Claimable() bool {
	return s == EventInboxStatusPending || s == EventInboxStatusClaimed
}

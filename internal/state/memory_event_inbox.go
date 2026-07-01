package state

import (
	"context"
	"fmt"
	"sync"
	"time"

	runtimeevent "agent/internal/event"
)

type MemoryEventInbox struct {
	mu     sync.Mutex
	events map[string]StoredEvent
	byRun  map[string][]string
	order  []string
}

func NewMemoryEventInbox() *MemoryEventInbox {
	return &MemoryEventInbox{
		events: make(map[string]StoredEvent),
		byRun:  make(map[string][]string),
	}
}

func (s *MemoryEventInbox) Append(ctx context.Context, event runtimeevent.Event) (StoredEvent, bool, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.events[event.ID]; ok {
		return existing.Clone(), false, nil
	}

	stored, err := normalizeStoredEvent(event, time.Now().UTC())
	if err != nil {
		return StoredEvent{}, false, err
	}

	s.events[stored.Event.ID] = stored.Clone()
	s.byRun[stored.Event.RunID] = append(s.byRun[stored.Event.RunID], stored.Event.ID)
	s.order = append(s.order, stored.Event.ID)
	return stored.Clone(), true, nil
}

func (s *MemoryEventInbox) ListPending(ctx context.Context, runID string) ([]StoredEvent, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := s.eventIDs(runID)
	out := make([]StoredEvent, 0, len(ids))
	for _, id := range ids {
		stored, ok := s.events[id]
		if !ok || !stored.Status.Claimable() {
			continue
		}
		out = append(out, stored.Clone())
	}
	return out, nil
}

func (s *MemoryEventInbox) Claim(ctx context.Context, runID string) (StoredEvent, bool, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := s.eventIDs(runID)
	for _, id := range ids {
		stored, ok := s.events[id]
		if !ok || !stored.Status.Claimable() {
			continue
		}
		if stored.Status == EventInboxStatusPending {
			markEventClaimed(&stored, time.Now().UTC())
			s.events[id] = stored.Clone()
		}
		return stored.Clone(), true, nil
	}
	return StoredEvent{}, false, nil
}

func (s *MemoryEventInbox) MarkProcessed(ctx context.Context, eventID string) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("event not found: %s", eventID)
	}
	now := time.Now().UTC()
	stored.Status = EventInboxStatusProcessed
	stored.Error = ""
	stored.UpdatedAt = now
	stored.ProcessedAt = &now
	s.events[eventID] = stored.Clone()
	return nil
}

func (s *MemoryEventInbox) MarkFailed(ctx context.Context, eventID string, cause error) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("event not found: %s", eventID)
	}
	now := time.Now().UTC()
	stored.Status = EventInboxStatusFailed
	if cause != nil {
		stored.Error = cause.Error()
	}
	stored.UpdatedAt = now
	stored.FailedAt = &now
	s.events[eventID] = stored.Clone()
	return nil
}

func (s *MemoryEventInbox) eventIDs(runID string) []string {
	if runID == "" {
		return append([]string(nil), s.order...)
	}
	return append([]string(nil), s.byRun[runID]...)
}

func markEventClaimed(stored *StoredEvent, now time.Time) {
	stored.Status = EventInboxStatusClaimed
	stored.UpdatedAt = now
	if stored.ClaimedAt == nil {
		stored.ClaimedAt = &now
	}
}

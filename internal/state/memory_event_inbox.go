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
	now    func() time.Time
}

func NewMemoryEventInbox() *MemoryEventInbox {
	return &MemoryEventInbox{
		events: make(map[string]StoredEvent),
		byRun:  make(map[string][]string),
		now:    time.Now,
	}
}

func (s *MemoryEventInbox) Append(ctx context.Context, event runtimeevent.Event) (StoredEvent, bool, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.events[event.ID]; ok {
		return existing.Clone(), false, nil
	}

	stored, err := normalizeStoredEvent(event, currentStoreTime(s.now))
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

func (s *MemoryEventInbox) Claim(ctx context.Context, runID string, owner string, leaseDuration time.Duration) (StoredEvent, bool, error) {
	_ = ctx
	owner, err := normalizeLease(owner, leaseDuration)
	if err != nil {
		return StoredEvent{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := currentStoreTime(s.now)
	ids := s.eventIDs(runID)
	for _, id := range ids {
		stored, ok := s.events[id]
		if !ok || !stored.EventClaimable(now) {
			continue
		}
		markEventClaimed(&stored, owner, leaseDuration, now)
		s.events[id] = stored.Clone()
		return stored.Clone(), true, nil
	}
	return StoredEvent{}, false, nil
}

func (s *MemoryEventInbox) MarkProcessed(ctx context.Context, eventID string, owner string) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("event not found: %s", eventID)
	}
	if stored.Status == EventInboxStatusProcessed {
		return validateTaskOwner(stored.Owner, owner)
	}
	if stored.Status == EventInboxStatusFailed {
		return validateTaskOwner(stored.Owner, owner)
	}
	now := currentStoreTime(s.now)
	if err := validateLeaseOwner(stored.Owner, stored.LeaseDeadline, owner, now); err != nil {
		return err
	}
	stored.Status = EventInboxStatusProcessed
	stored.Error = ""
	stored.UpdatedAt = now
	stored.ProcessedAt = &now
	s.events[eventID] = stored.Clone()
	return nil
}

func (s *MemoryEventInbox) MarkFailed(ctx context.Context, eventID string, owner string, cause error) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("event not found: %s", eventID)
	}
	if stored.Status == EventInboxStatusProcessed {
		return validateTaskOwner(stored.Owner, owner)
	}
	if stored.Status == EventInboxStatusFailed {
		return validateTaskOwner(stored.Owner, owner)
	}
	now := currentStoreTime(s.now)
	if err := validateLeaseOwner(stored.Owner, stored.LeaseDeadline, owner, now); err != nil {
		return err
	}
	stored.Status = EventInboxStatusFailed
	stored.Error = ""
	if cause != nil {
		stored.Error = cause.Error()
	}
	stored.UpdatedAt = now
	stored.FailedAt = &now
	s.events[eventID] = stored.Clone()
	return nil
}

func (s *MemoryEventInbox) RenewLease(ctx context.Context, eventID string, owner string, leaseDuration time.Duration) (StoredEvent, error) {
	_ = ctx
	owner, err := normalizeLease(owner, leaseDuration)
	if err != nil {
		return StoredEvent{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.events[eventID]
	if !ok {
		return StoredEvent{}, fmt.Errorf("event not found: %s", eventID)
	}
	now := currentStoreTime(s.now)
	if err := validateLeaseOwner(stored.Owner, stored.LeaseDeadline, owner, now); err != nil {
		return StoredEvent{}, err
	}
	deadline := leaseDeadline(now, leaseDuration)
	stored.LeaseDeadline = &deadline
	stored.UpdatedAt = now
	s.events[eventID] = stored.Clone()
	return stored.Clone(), nil
}

func (s *MemoryEventInbox) eventIDs(runID string) []string {
	if runID == "" {
		return append([]string(nil), s.order...)
	}
	return append([]string(nil), s.byRun[runID]...)
}

func (e StoredEvent) EventClaimable(now time.Time) bool {
	if e.Status == EventInboxStatusPending {
		return true
	}
	if e.Status != EventInboxStatusClaimed {
		return false
	}
	if e.Owner == "" {
		return true
	}
	return !leaseActive(e.LeaseDeadline, now)
}

func markEventClaimed(stored *StoredEvent, owner string, leaseDuration time.Duration, now time.Time) {
	stored.Status = EventInboxStatusClaimed
	stored.UpdatedAt = now
	if stored.ClaimedAt == nil {
		stored.ClaimedAt = &now
	}
	deadline := leaseDeadline(now, leaseDuration)
	stored.Owner = owner
	stored.LeaseDeadline = &deadline
	stored.ClaimCount++
	stored.Error = ""
}

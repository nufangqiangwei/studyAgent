package state

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	runtimeevent "agent/internal/event"
)

type MemoryStateStore struct {
	mu     sync.Mutex
	states map[string]RunState
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		states: make(map[string]RunState),
	}
}

func (s *MemoryStateStore) Load(ctx context.Context, runID string) (RunState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	st, ok := s.states[runID]
	if !ok {
		return RunState{}, fmt.Errorf("state not found: %s", runID)
	}

	return cloneRunState(st), nil
}

func (s *MemoryStateStore) List(ctx context.Context) ([]RunState, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	runIDs := make([]string, 0, len(s.states))
	for runID := range s.states {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)

	out := make([]RunState, 0, len(runIDs))
	for _, runID := range runIDs {
		out = append(out, cloneRunState(s.states[runID]))
	}
	return out, nil
}

func (s *MemoryStateStore) Save(ctx context.Context, st RunState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if st.RunID == "" {
		return fmt.Errorf("run_id is required")
	}

	s.states[st.RunID] = cloneRunState(st)
	return nil
}

type MemoryEventStore struct {
	mu     sync.Mutex
	events map[string][]runtimeevent.Event
	seen   map[string]struct{}
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		events: make(map[string][]runtimeevent.Event),
		seen:   make(map[string]struct{}),
	}
}

func (s *MemoryEventStore) Append(ctx context.Context, event runtimeevent.Event) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if event.ID == "" {
		return false, fmt.Errorf("event id is required")
	}
	if event.RunID == "" {
		return false, fmt.Errorf("run_id is required")
	}

	if _, ok := s.seen[event.ID]; ok {
		return false, nil
	}

	s.seen[event.ID] = struct{}{}
	s.events[event.RunID] = append(s.events[event.RunID], event.Clone())
	return true, nil
}

func (s *MemoryEventStore) List(ctx context.Context, runID string) ([]runtimeevent.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored := s.events[runID]
	out := make([]runtimeevent.Event, 0, len(stored))
	for _, event := range stored {
		out = append(out, event.Clone())
	}
	return out, nil
}

type MemoryEffectStore struct {
	mu      sync.Mutex
	effects map[string]StoredEffect
	byRun   map[string][]string
	order   []string
	now     func() time.Time
}

func NewMemoryEffectStore() *MemoryEffectStore {
	return &MemoryEffectStore{
		effects: make(map[string]StoredEffect),
		byRun:   make(map[string][]string),
		now:     time.Now,
	}
}

func (s *MemoryEffectStore) Append(ctx context.Context, effect Effect) (StoredEffect, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.effects[effect.ID]; ok {
		return existing.Clone(), nil
	}

	stored, err := normalizeStoredEffect(effect, currentStoreTime(s.now))
	if err != nil {
		return StoredEffect{}, err
	}

	s.effects[stored.Effect.ID] = stored.Clone()
	s.byRun[stored.Effect.RunID] = append(s.byRun[stored.Effect.RunID], stored.Effect.ID)
	s.order = append(s.order, stored.Effect.ID)
	return stored.Clone(), nil
}

func (s *MemoryEffectStore) ListPending(ctx context.Context, runID string) ([]StoredEffect, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := s.effectIDs(runID)
	out := make([]StoredEffect, 0, len(ids))
	for _, id := range ids {
		stored, ok := s.effects[id]
		if !ok || !stored.Status.PendingWork() {
			continue
		}
		out = append(out, stored.Clone())
	}
	return out, nil
}

func (s *MemoryEffectStore) Claim(ctx context.Context, runID string, owner string, leaseDuration time.Duration) (StoredEffect, bool, error) {
	_ = ctx
	owner, err := normalizeLease(owner, leaseDuration)
	if err != nil {
		return StoredEffect{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := currentStoreTime(s.now)
	ids := s.effectIDs(runID)
	for _, id := range ids {
		stored, ok := s.effects[id]
		if !ok || !stored.EffectClaimable(now) {
			continue
		}
		markEffectClaimed(&stored, owner, leaseDuration, now)
		s.effects[id] = stored.Clone()
		return stored.Clone(), true, nil
	}
	return StoredEffect{}, false, nil
}

func (s *MemoryEffectStore) MarkDispatched(ctx context.Context, effectID string) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.effects[effectID]
	if !ok {
		return fmt.Errorf("effect not found: %s", effectID)
	}
	if stored.Status == EffectStatusCompleted || stored.Status == EffectStatusFailed {
		return nil
	}
	markEffectDispatched(&stored, currentStoreTime(s.now))
	s.effects[effectID] = stored.Clone()
	return nil
}

func (s *MemoryEffectStore) MarkCompleted(ctx context.Context, effectID string, owner string) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.effects[effectID]
	if !ok {
		return fmt.Errorf("effect not found: %s", effectID)
	}
	if stored.Status == EffectStatusCompleted {
		return validateTaskOwner(stored.Owner, owner)
	}
	if stored.Status == EffectStatusFailed {
		return validateTaskOwner(stored.Owner, owner)
	}
	now := currentStoreTime(s.now)
	if err := validateLeaseOwner(stored.Owner, stored.LeaseDeadline, owner, now); err != nil {
		return err
	}
	stored.Status = EffectStatusCompleted
	stored.Error = ""
	stored.UpdatedAt = now
	stored.CompletedAt = &now
	s.effects[effectID] = stored.Clone()
	return nil
}

func (s *MemoryEffectStore) MarkFailed(ctx context.Context, effectID string, owner string, cause error) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.effects[effectID]
	if !ok {
		return fmt.Errorf("effect not found: %s", effectID)
	}
	if stored.Status == EffectStatusCompleted {
		return validateTaskOwner(stored.Owner, owner)
	}
	if stored.Status == EffectStatusFailed {
		return validateTaskOwner(stored.Owner, owner)
	}
	now := currentStoreTime(s.now)
	if err := validateLeaseOwner(stored.Owner, stored.LeaseDeadline, owner, now); err != nil {
		return err
	}
	stored.Status = EffectStatusFailed
	stored.Error = ""
	if cause != nil {
		stored.Error = cause.Error()
	}
	stored.UpdatedAt = now
	stored.FailedAt = &now
	s.effects[effectID] = stored.Clone()
	return nil
}

func (s *MemoryEffectStore) RenewLease(ctx context.Context, effectID string, owner string, leaseDuration time.Duration) (StoredEffect, error) {
	_ = ctx
	owner, err := normalizeLease(owner, leaseDuration)
	if err != nil {
		return StoredEffect{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.effects[effectID]
	if !ok {
		return StoredEffect{}, fmt.Errorf("effect not found: %s", effectID)
	}
	now := currentStoreTime(s.now)
	if err := validateLeaseOwner(stored.Owner, stored.LeaseDeadline, owner, now); err != nil {
		return StoredEffect{}, err
	}
	deadline := leaseDeadline(now, leaseDuration)
	stored.LeaseDeadline = &deadline
	stored.UpdatedAt = now
	s.effects[effectID] = stored.Clone()
	return stored.Clone(), nil
}

func (s *MemoryEffectStore) effectIDs(runID string) []string {
	if runID == "" {
		return append([]string(nil), s.order...)
	}
	return append([]string(nil), s.byRun[runID]...)
}

func (s EffectStatus) PendingWork() bool {
	return s == EffectStatusPending || s == EffectStatusDispatched
}

func (e StoredEffect) EffectClaimable(now time.Time) bool {
	if e.Status == EffectStatusPending {
		return true
	}
	if e.Status != EffectStatusDispatched {
		return false
	}
	if e.Owner == "" {
		return true
	}
	return !leaseActive(e.LeaseDeadline, now)
}

func markEffectDispatched(stored *StoredEffect, now time.Time) {
	stored.Status = EffectStatusDispatched
	stored.UpdatedAt = now
	if stored.DispatchedAt == nil {
		stored.DispatchedAt = &now
	}
}

func markEffectClaimed(stored *StoredEffect, owner string, leaseDuration time.Duration, now time.Time) {
	markEffectDispatched(stored, now)
	deadline := leaseDeadline(now, leaseDuration)
	stored.Owner = owner
	stored.LeaseDeadline = &deadline
	stored.ClaimCount++
	stored.Error = ""
}

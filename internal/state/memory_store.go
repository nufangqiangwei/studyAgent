package state

import (
	"context"
	"fmt"
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

	return st, nil
}

func (s *MemoryStateStore) Save(ctx context.Context, st RunState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if st.RunID == "" {
		return fmt.Errorf("run_id is required")
	}

	s.states[st.RunID] = st
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
}

func NewMemoryEffectStore() *MemoryEffectStore {
	return &MemoryEffectStore{
		effects: make(map[string]StoredEffect),
		byRun:   make(map[string][]string),
	}
}

func (s *MemoryEffectStore) Append(ctx context.Context, effect Effect) (StoredEffect, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.effects[effect.ID]; ok {
		return existing.Clone(), nil
	}

	stored, err := normalizeStoredEffect(effect, time.Now().UTC())
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

func (s *MemoryEffectStore) Claim(ctx context.Context, runID string) (StoredEffect, bool, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := s.effectIDs(runID)
	for _, id := range ids {
		stored, ok := s.effects[id]
		if !ok || !stored.Status.PendingWork() {
			continue
		}
		if stored.Status == EffectStatusPending {
			markEffectDispatched(&stored, time.Now().UTC())
			s.effects[id] = stored.Clone()
		}
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
	markEffectDispatched(&stored, time.Now().UTC())
	s.effects[effectID] = stored.Clone()
	return nil
}

func (s *MemoryEffectStore) MarkCompleted(ctx context.Context, effectID string) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.effects[effectID]
	if !ok {
		return fmt.Errorf("effect not found: %s", effectID)
	}
	now := time.Now().UTC()
	stored.Status = EffectStatusCompleted
	stored.Error = ""
	stored.UpdatedAt = now
	stored.CompletedAt = &now
	s.effects[effectID] = stored.Clone()
	return nil
}

func (s *MemoryEffectStore) MarkFailed(ctx context.Context, effectID string, cause error) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := s.effects[effectID]
	if !ok {
		return fmt.Errorf("effect not found: %s", effectID)
	}
	now := time.Now().UTC()
	stored.Status = EffectStatusFailed
	if cause != nil {
		stored.Error = cause.Error()
	}
	stored.UpdatedAt = now
	stored.FailedAt = &now
	s.effects[effectID] = stored.Clone()
	return nil
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

func markEffectDispatched(stored *StoredEffect, now time.Time) {
	stored.Status = EffectStatusDispatched
	stored.UpdatedAt = now
	if stored.DispatchedAt == nil {
		stored.DispatchedAt = &now
	}
}

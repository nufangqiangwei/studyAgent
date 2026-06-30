package state

import (
	"context"
	"fmt"
	"sync"

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

func (s *MemoryEventStore) Append(ctx context.Context, event runtimeevent.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if event.ID == "" {
		return fmt.Errorf("event id is required")
	}
	if event.RunID == "" {
		return fmt.Errorf("run_id is required")
	}

	if _, ok := s.seen[event.ID]; ok {
		return nil
	}

	s.seen[event.ID] = struct{}{}
	s.events[event.RunID] = append(s.events[event.RunID], event.Clone())
	return nil
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

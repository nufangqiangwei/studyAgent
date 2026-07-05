package statemachine

import (
	"context"
	"fmt"
	"sync"
)

type StateStore interface {
	Load(ctx context.Context, taskID string) (TaskState, bool, error)
	Save(ctx context.Context, state TaskState) error
}

type MemoryStateStore struct {
	mu     sync.RWMutex
	states map[string]TaskState
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{states: make(map[string]TaskState)}
}

func (s *MemoryStateStore) Load(_ context.Context, taskID string) (TaskState, bool, error) {
	if s == nil {
		return TaskState{}, false, fmt.Errorf("state store is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.states[taskID]
	if !ok {
		return TaskState{}, false, nil
	}
	return state.Clone(), true, nil
}

func (s *MemoryStateStore) Save(_ context.Context, state TaskState) error {
	if s == nil {
		return fmt.Errorf("state store is nil")
	}
	if state.TaskID == "" {
		return fmt.Errorf("task state id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[state.TaskID] = state.Clone()
	return nil
}

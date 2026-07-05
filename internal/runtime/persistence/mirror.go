package persistence

import (
	agents2 "agent/internal/runtime/agents"
	statemachine2 "agent/internal/runtime/statemachine"
	"context"
	"fmt"
)

var _ statemachine2.StateStore = (*MirroredTaskStateStore)(nil)
var _ agents2.SnapshotStore = (*MirroredSnapshotStore)(nil)

type MirroredTaskStateStore struct {
	memory *statemachine2.MemoryStateStore
	local  TaskStateStore
}

func NewMirroredTaskStateStore(memory *statemachine2.MemoryStateStore, local TaskStateStore) *MirroredTaskStateStore {
	return &MirroredTaskStateStore{memory: memory, local: local}
}

func (s *MirroredTaskStateStore) Load(ctx context.Context, taskID string) (statemachine2.TaskState, bool, error) {
	if s == nil {
		return statemachine2.TaskState{}, false, fmt.Errorf("mirrored task state store is nil")
	}
	if s.memory != nil {
		state, ok, err := s.memory.Load(ctx, taskID)
		if err != nil {
			return statemachine2.TaskState{}, false, err
		}
		if ok {
			return state, true, nil
		}
	}
	if s.local == nil {
		return statemachine2.TaskState{}, false, nil
	}
	state, ok, err := s.local.Load(ctx, taskID)
	if err != nil || !ok {
		return state, ok, err
	}
	if s.memory != nil {
		if err := s.memory.Save(ctx, state); err != nil {
			return statemachine2.TaskState{}, false, err
		}
	}
	return state, true, nil
}

func (s *MirroredTaskStateStore) Save(ctx context.Context, state statemachine2.TaskState) error {
	if s == nil {
		return fmt.Errorf("mirrored task state store is nil")
	}
	if s.local != nil {
		if err := s.local.Save(ctx, state); err != nil {
			return err
		}
	}
	if s.memory != nil {
		return s.memory.Save(ctx, state)
	}
	return nil
}

func (s *MirroredTaskStateStore) List(ctx context.Context) ([]statemachine2.TaskState, error) {
	if s == nil {
		return nil, fmt.Errorf("mirrored task state store is nil")
	}
	if s.local == nil {
		return nil, nil
	}
	return s.local.List(ctx)
}

type MirroredSnapshotStore struct {
	memory *agents2.MemorySnapshotStore
	local  SnapshotStore
}

func NewMirroredSnapshotStore(memory *agents2.MemorySnapshotStore, local SnapshotStore) *MirroredSnapshotStore {
	return &MirroredSnapshotStore{memory: memory, local: local}
}

func (s *MirroredSnapshotStore) Load(ctx context.Context, agentName string, taskID string) (agents2.AgentSnapshot, bool, error) {
	if s == nil {
		return agents2.AgentSnapshot{}, false, fmt.Errorf("mirrored snapshot store is nil")
	}
	if s.memory != nil {
		snapshot, ok, err := s.memory.Load(ctx, agentName, taskID)
		if err != nil {
			return agents2.AgentSnapshot{}, false, err
		}
		if ok {
			return snapshot, true, nil
		}
	}
	if s.local == nil {
		return agents2.AgentSnapshot{}, false, nil
	}
	snapshot, ok, err := s.local.Load(ctx, agentName, taskID)
	if err != nil || !ok {
		return snapshot, ok, err
	}
	if s.memory != nil {
		if err := s.memory.Save(ctx, snapshot); err != nil {
			return agents2.AgentSnapshot{}, false, err
		}
	}
	return snapshot, true, nil
}

func (s *MirroredSnapshotStore) Save(ctx context.Context, snapshot agents2.AgentSnapshot) error {
	if s == nil {
		return fmt.Errorf("mirrored snapshot store is nil")
	}
	if s.local != nil {
		if err := s.local.Save(ctx, snapshot); err != nil {
			return err
		}
	}
	if s.memory != nil {
		return s.memory.Save(ctx, snapshot)
	}
	return nil
}

func (s *MirroredSnapshotStore) List(ctx context.Context) ([]agents2.AgentSnapshot, error) {
	if s == nil {
		return nil, fmt.Errorf("mirrored snapshot store is nil")
	}
	if s.local == nil {
		return nil, nil
	}
	return s.local.List(ctx)
}

package agents

import (
	"context"
	"fmt"
	"sync"
)

type SnapshotStore interface {
	Load(ctx context.Context, agentName string, taskID string) (AgentSnapshot, bool, error)
	Save(ctx context.Context, snapshot AgentSnapshot) error
}

type MemorySnapshotStore struct {
	mu        sync.RWMutex
	snapshots map[string]AgentSnapshot
}

func NewMemorySnapshotStore() *MemorySnapshotStore {
	return &MemorySnapshotStore{snapshots: make(map[string]AgentSnapshot)}
}

func (s *MemorySnapshotStore) Load(_ context.Context, agentName string, taskID string) (AgentSnapshot, bool, error) {
	if s == nil {
		return AgentSnapshot{}, false, fmt.Errorf("snapshot store is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.snapshots[snapshotKey(agentName, taskID)]
	if !ok {
		return AgentSnapshot{}, false, nil
	}
	return snapshot.Clone(), true, nil
}

func (s *MemorySnapshotStore) Save(_ context.Context, snapshot AgentSnapshot) error {
	if s == nil {
		return fmt.Errorf("snapshot store is nil")
	}
	if snapshot.Agent == "" {
		return fmt.Errorf("agent snapshot agent is required")
	}
	if snapshot.TaskID == "" {
		return fmt.Errorf("agent snapshot task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[snapshotKey(snapshot.Agent, snapshot.TaskID)] = snapshot.Clone()
	return nil
}

func snapshotKey(agentName string, taskID string) string {
	return agentName + "\x00" + taskID
}

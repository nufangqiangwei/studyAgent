package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"fmt"
	"sort"
)

type connectionStore struct{ owner *Store }

func (s *connectionStore) Create(ctx context.Context, record persistence.ConnectionRecord) error {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if err := s.owner.check(ctx); err != nil {
		return err
	}
	if err := validateConnectionRecord(record); err != nil {
		return err
	}
	if _, exists := s.owner.connections[record.ConnectionID]; exists {
		return persistence.ErrDuplicateID
	}
	key := recordConnectionKey(record)
	if _, exists := s.owner.connectionKeys[key]; exists {
		return persistence.ErrDuplicateID
	}
	s.owner.connections[record.ConnectionID] = record.Clone()
	s.owner.connectionKeys[key] = record.ConnectionID
	return nil
}

func (s *connectionStore) Update(ctx context.Context, record persistence.ConnectionRecord) error {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if err := s.owner.check(ctx); err != nil {
		return err
	}
	if err := validateConnectionRecord(record); err != nil {
		return err
	}
	previous, exists := s.owner.connections[record.ConnectionID]
	if !exists {
		return fmt.Errorf("connection %q not found", record.ConnectionID)
	}
	if recordConnectionKey(previous) != recordConnectionKey(record) || previous.OwnerAddress != record.OwnerAddress || previous.Driver != record.Driver {
		return fmt.Errorf("connection %q identity cannot change", record.ConnectionID)
	}
	s.owner.connections[record.ConnectionID] = record.Clone()
	return nil
}

func (s *connectionStore) Get(ctx context.Context, runtimeID contract.RuntimeID, connectionID string) (persistence.ConnectionRecord, bool, error) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if err := s.owner.check(ctx); err != nil {
		return persistence.ConnectionRecord{}, false, err
	}
	record, found := s.owner.connections[connectionID]
	if !found || runtimeID != "" && record.RuntimeID != runtimeID {
		return persistence.ConnectionRecord{}, false, nil
	}
	return record.Clone(), true, nil
}

func (s *connectionStore) List(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.ConnectionRecord, error) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if err := s.owner.check(ctx); err != nil {
		return nil, err
	}
	result := make([]persistence.ConnectionRecord, 0, len(s.owner.connections))
	for _, record := range s.owner.connections {
		if runtimeID == "" || record.RuntimeID == runtimeID {
			result = append(result, record.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ConnectionID < result[j].ConnectionID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func recordConnectionKey(record persistence.ConnectionRecord) connectionKey {
	return connectionKey{runtime: record.RuntimeID, revision: record.PlanRevision, owner: record.OwnerInstanceID, key: record.Key}
}

func validateConnectionRecord(record persistence.ConnectionRecord) error {
	if record.ConnectionID == "" || record.RuntimeID == "" || record.PlanRevision == "" || record.OwnerInstanceID == "" || record.OwnerAddress == "" {
		return fmt.Errorf("connection id, runtime, plan, owner instance and owner address are required")
	}
	if record.Key == "" || record.Driver == "" {
		return fmt.Errorf("connection key and driver are required")
	}
	if !record.Status.Valid() {
		return fmt.Errorf("connection status %q is invalid", record.Status)
	}
	return nil
}

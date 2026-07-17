package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"fmt"
	"sort"
	"time"
)

func (s *Store) Create(ctx context.Context, record instance.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if _, exists := s.instances[record.InstanceID]; exists {
		return fmt.Errorf("instance %q already exists", record.InstanceID)
	}
	key := addressKey{runtime: record.RuntimeID, revision: record.PlanRevision, address: record.Address}
	if existing, exists := s.addresses[key]; exists {
		return fmt.Errorf("service address %q is already assigned to %q", record.Address, existing)
	}
	if record.RecordVersion == 0 {
		record.RecordVersion = 1
	}
	if record.Lifecycle == "" {
		record.Lifecycle = instance.Declared
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = s.now()
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = record.CreatedAt
	}
	s.instances[record.InstanceID] = record.Clone()
	s.addresses[key] = record.InstanceID
	return nil
}

func (s *Store) Get(ctx context.Context, instanceID contract.ServiceInstanceID) (instance.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return instance.Record{}, false, err
	}
	record, ok := s.instances[instanceID]
	return record.Clone(), ok, nil
}

func (s *Store) GetByAddress(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision, address contract.ServiceAddress) (instance.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return instance.Record{}, false, err
	}
	id, ok := s.addresses[addressKey{runtime: runtimeID, revision: revision, address: address}]
	if !ok {
		return instance.Record{}, false, nil
	}
	record, ok := s.instances[id]
	return record.Clone(), ok, nil
}

func (s *Store) CompareAndSwap(ctx context.Context, next instance.Record, expectedRecordVersion uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	current, ok := s.instances[next.InstanceID]
	if !ok {
		return fmt.Errorf("instance %q not found", next.InstanceID)
	}
	if current.RecordVersion != expectedRecordVersion {
		return persistence.ErrSequenceConflict
	}
	if current.Address != next.Address || current.RuntimeID != next.RuntimeID || current.PlanRevision != next.PlanRevision {
		return fmt.Errorf("instance identity fields cannot change")
	}
	if !instance.CanTransition(current.Lifecycle, next.Lifecycle) {
		return fmt.Errorf("instance lifecycle transition %q -> %q is not allowed", current.Lifecycle, next.Lifecycle)
	}
	next.RecordVersion = expectedRecordVersion + 1
	next.UpdatedAt = s.now()
	s.instances[next.InstanceID] = next.Clone()
	return nil
}

func (s *Store) List(ctx context.Context, query instance.Query) ([]instance.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	allowed := make(map[instance.Lifecycle]struct{}, len(query.Lifecycle))
	for _, lifecycle := range query.Lifecycle {
		allowed[lifecycle] = struct{}{}
	}
	ids := make([]string, 0, len(s.instances))
	for id := range s.instances {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	result := make([]instance.Record, 0, len(ids))
	for _, rawID := range ids {
		record := s.instances[contract.ServiceInstanceID(rawID)]
		if query.RuntimeID != "" && record.RuntimeID != query.RuntimeID {
			continue
		}
		if query.PlanRevision != "" && record.PlanRevision != query.PlanRevision {
			continue
		}
		if query.Kind != nil && record.Kind != *query.Kind {
			continue
		}
		if len(allowed) > 0 {
			if _, ok := allowed[record.Lifecycle]; !ok {
				continue
			}
		}
		result = append(result, record.Clone())
	}
	return result, nil
}

func (s *Store) Acquire(ctx context.Context, instanceID contract.ServiceInstanceID, ownerID string, duration time.Duration) (instance.ActivationLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return instance.ActivationLease{}, err
	}
	record, ok := s.instances[instanceID]
	if !ok {
		return instance.ActivationLease{}, fmt.Errorf("instance %q not found", instanceID)
	}
	now := s.now()
	if current, exists := s.leases[instanceID]; exists && current.LeaseUntil.After(now) && current.OwnerID != ownerID {
		return instance.ActivationLease{}, persistence.ErrLeaseLost
	}
	if duration <= 0 {
		duration = time.Minute
	}
	record.ActivationEpoch++
	record.RecordVersion++
	record.UpdatedAt = now
	s.instances[instanceID] = record
	lease := instance.ActivationLease{
		InstanceID: instanceID,
		Epoch:      record.ActivationEpoch,
		OwnerID:    ownerID,
		LeaseToken: s.token("activation"),
		AcquiredAt: now,
		LeaseUntil: now.Add(duration),
	}
	s.leases[instanceID] = lease
	return lease, nil
}

func (s *Store) Renew(ctx context.Context, lease instance.ActivationLease, duration time.Duration) (instance.ActivationLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return instance.ActivationLease{}, err
	}
	current, ok := s.leases[lease.InstanceID]
	if !ok || current.LeaseToken != lease.LeaseToken || current.Epoch != lease.Epoch {
		return instance.ActivationLease{}, persistence.ErrLeaseLost
	}
	if duration <= 0 {
		duration = time.Minute
	}
	current.LeaseUntil = s.now().Add(duration)
	s.leases[lease.InstanceID] = current
	return current, nil
}

func (s *Store) Release(ctx context.Context, lease instance.ActivationLease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	current, ok := s.leases[lease.InstanceID]
	if !ok {
		return nil
	}
	if current.LeaseToken != lease.LeaseToken || current.Epoch != lease.Epoch {
		return persistence.ErrLeaseLost
	}
	delete(s.leases, lease.InstanceID)
	return nil
}

func (s *Store) Current(ctx context.Context, instanceID contract.ServiceInstanceID) (instance.ActivationLease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return instance.ActivationLease{}, false, err
	}
	lease, ok := s.leases[instanceID]
	return lease, ok, nil
}

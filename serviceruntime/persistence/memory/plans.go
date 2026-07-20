package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"sort"
)

type planStore struct{ owner *Store }

func (s *planStore) Put(ctx context.Context, record persistence.PlanRecord) (bool, error) {
	return s.owner.putPlan(ctx, record)
}

func (s *planStore) Get(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision) (persistence.PlanRecord, bool, error) {
	return s.owner.getPlan(ctx, runtimeID, revision)
}

func (s *planStore) List(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.PlanRecord, error) {
	return s.owner.listPlans(ctx, runtimeID)
}

func (s *Store) putPlan(ctx context.Context, record persistence.PlanRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return false, err
	}
	key := planKey{runtime: record.RuntimeID, revision: record.PlanRevision}
	if existing, found := s.plans[key]; found {
		if existing.PlanHash != record.PlanHash {
			return false, persistence.ErrPlanConflict
		}
		return false, nil
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = s.now()
	}
	s.plans[key] = record.Clone()
	return true, nil
}

func (s *Store) getPlan(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision) (persistence.PlanRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return persistence.PlanRecord{}, false, err
	}
	record, found := s.plans[planKey{runtime: runtimeID, revision: revision}]
	return record.Clone(), found, nil
}

func (s *Store) listPlans(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.PlanRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	var result []persistence.PlanRecord
	for key, record := range s.plans {
		if runtimeID == "" || key.runtime == runtimeID {
			result = append(result, record.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PlanRevision < result[j].PlanRevision })
	return result, nil
}

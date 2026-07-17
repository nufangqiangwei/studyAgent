package memory

import (
	"agent/serviceruntime/contract"
	"context"
)

func (s *Store) LoadStream(ctx context.Context, streamID contract.StreamID, afterSequence uint64, limit int) ([]contract.StoredEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	stored := s.events[streamID]
	result := make([]contract.StoredEvent, 0)
	for _, event := range stored {
		if event.Sequence <= afterSequence {
			continue
		}
		result = append(result, event.Clone())
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (s *Store) Head(ctx context.Context, streamID contract.StreamID) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return 0, err
	}
	return uint64(len(s.events[streamID])), nil
}

func (s *Store) LoadLatest(ctx context.Context, streamID contract.StreamID) (contract.Snapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return contract.Snapshot{}, false, err
	}
	snapshot, ok := s.snapshots[streamID]
	return snapshot.Clone(), ok, nil
}

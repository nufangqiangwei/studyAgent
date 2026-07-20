package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"time"
)

type outboxStore struct{ owner *Store }

func (s *outboxStore) ClaimNext(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (persistence.OutboxClaim, bool, error) {
	return s.owner.claimNextOutbox(ctx, runtimeID, ownerID, lease)
}

func (s *outboxStore) RenewClaim(ctx context.Context, claim persistence.OutboxClaim, lease time.Duration) error {
	return s.owner.renewOutboxClaim(ctx, claim, lease)
}

func (s *outboxStore) MarkDelivered(ctx context.Context, claim persistence.OutboxClaim, result persistence.DeliverySummary) error {
	return s.owner.markOutboxDelivered(ctx, claim, result)
}

func (s *outboxStore) MarkRetry(ctx context.Context, claim persistence.OutboxClaim, retryAt time.Time, cause error) error {
	return s.owner.markOutboxRetry(ctx, claim, retryAt, cause)
}

func (s *outboxStore) MoveToDeadLetter(ctx context.Context, claim persistence.OutboxClaim, cause error) error {
	return s.owner.deadLetterOutbox(ctx, claim, cause)
}

func (s *outboxStore) CountPending(ctx context.Context, runtimeID contract.RuntimeID) (int, error) {
	return s.owner.countPendingOutbox(ctx, runtimeID)
}

func (s *Store) claimNextOutbox(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (persistence.OutboxClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return persistence.OutboxClaim{}, false, err
	}
	now := s.now()
	if lease <= 0 {
		lease = 30 * time.Second
	}
	for _, outboxID := range s.outboxOrder {
		record := s.outbox[outboxID]
		if runtimeID != "" && record.Message.RuntimeID != runtimeID {
			continue
		}
		claimable := (record.Status == persistence.OutboxPending || record.Status == persistence.OutboxRetry) && !record.AvailableAt.After(now)
		if record.Status == persistence.OutboxClaimed && record.LeaseUntil != nil && !record.LeaseUntil.After(now) {
			claimable = true
		}
		if !claimable {
			continue
		}
		record.Status = persistence.OutboxClaimed
		record.Attempt++
		record.LeaseOwner = ownerID
		record.LeaseToken = s.token("outbox")
		until := now.Add(lease)
		record.LeaseUntil = &until
		s.outbox[outboxID] = record
		return persistence.OutboxClaim{Record: record.Clone(), LeaseToken: record.LeaseToken}, true, nil
	}
	return persistence.OutboxClaim{}, false, nil
}

func (s *Store) markOutboxDelivered(ctx context.Context, claim persistence.OutboxClaim, _ persistence.DeliverySummary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, ok := s.outbox[claim.Record.OutboxID]
	if !ok || record.Status != persistence.OutboxClaimed || record.LeaseToken != claim.LeaseToken {
		return persistence.ErrLeaseLost
	}
	now := s.now()
	record.Status = persistence.OutboxDelivered
	record.DeliveredAt = &now
	record.LeaseOwner, record.LeaseToken, record.LeaseUntil = "", "", nil
	s.outbox[record.OutboxID] = record
	return nil
}

func (s *Store) renewOutboxClaim(ctx context.Context, claim persistence.OutboxClaim, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, ok := s.outbox[claim.Record.OutboxID]
	if !ok || record.Status != persistence.OutboxClaimed || record.LeaseToken != claim.LeaseToken {
		return persistence.ErrLeaseLost
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	until := s.now().Add(lease)
	record.LeaseUntil = &until
	s.outbox[record.OutboxID] = record
	return nil
}

func (s *Store) markOutboxRetry(ctx context.Context, claim persistence.OutboxClaim, retryAt time.Time, cause error) error {
	return s.finishOutbox(ctx, claim, persistence.OutboxRetry, retryAt, cause)
}

func (s *Store) deadLetterOutbox(ctx context.Context, claim persistence.OutboxClaim, cause error) error {
	return s.finishOutbox(ctx, claim, persistence.OutboxDead, time.Time{}, cause)
}

func (s *Store) finishOutbox(ctx context.Context, claim persistence.OutboxClaim, status persistence.OutboxStatus, retryAt time.Time, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, ok := s.outbox[claim.Record.OutboxID]
	if !ok || record.Status != persistence.OutboxClaimed || record.LeaseToken != claim.LeaseToken {
		return persistence.ErrLeaseLost
	}
	record.Status = status
	record.LeaseOwner, record.LeaseToken, record.LeaseUntil = "", "", nil
	if !retryAt.IsZero() {
		record.AvailableAt = retryAt
	}
	if cause != nil {
		record.LastError = cause.Error()
	}
	s.outbox[record.OutboxID] = record
	return nil
}

func (s *Store) countPendingOutbox(ctx context.Context, runtimeID contract.RuntimeID) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return 0, err
	}
	count := 0
	for _, outboxID := range s.outboxOrder {
		record := s.outbox[outboxID]
		if runtimeID != "" && record.Message.RuntimeID != runtimeID {
			continue
		}
		switch record.Status {
		case persistence.OutboxPending, persistence.OutboxRetry, persistence.OutboxClaimed:
			count++
		}
	}
	return count, nil
}

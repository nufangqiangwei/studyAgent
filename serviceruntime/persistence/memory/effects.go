package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"fmt"
	"time"
)

func (s *Store) ClaimNextEffect(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (persistence.EffectClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return persistence.EffectClaim{}, false, err
	}
	now := s.now()
	if lease <= 0 {
		lease = time.Minute
	}
	for _, effectID := range s.effectOrder {
		record := s.effects[effectID]
		if runtimeID != "" && record.RuntimeID != runtimeID {
			continue
		}
		claimable := (record.Status == persistence.EffectPlanned || record.Status == persistence.EffectFailed) && !record.AvailableAt.After(now)
		if record.Status == persistence.EffectStarted && record.LeaseUntil != nil && !record.LeaseUntil.After(now) {
			claimable = true
		}
		if !claimable {
			continue
		}
		record.Attempt++
		record.LeaseOwner = ownerID
		record.LeaseToken = s.token("effect")
		until := now.Add(lease)
		record.LeaseUntil = &until
		s.effects[effectID] = record
		return persistence.EffectClaim{Record: record.Clone(), LeaseToken: record.LeaseToken}, true, nil
	}
	return persistence.EffectClaim{}, false, nil
}

// ClaimNext satisfies EffectStore. Its distinct parameter list from the
// OutboxStore ClaimNext is represented by the adapter returned from Effects.
func (s *effectStore) ClaimNext(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (persistence.EffectClaim, bool, error) {
	return s.owner.ClaimNextEffect(ctx, runtimeID, ownerID, lease)
}

type effectStore struct{ owner *Store }

func (s *effectStore) RenewClaim(ctx context.Context, claim persistence.EffectClaim, lease time.Duration) error {
	return s.owner.renewEffectClaim(ctx, claim, lease)
}

func (s *effectStore) Claim(ctx context.Context, effectID string, ownerID string, lease time.Duration) (persistence.EffectClaim, error) {
	return s.owner.claimEffect(ctx, effectID, ownerID, lease)
}

func (s *effectStore) MarkStarted(ctx context.Context, claim persistence.EffectClaim) error {
	return s.owner.markEffectStarted(ctx, claim)
}

func (s *effectStore) MarkSucceeded(ctx context.Context, claim persistence.EffectClaim, result persistence.EffectResult) error {
	return s.owner.markEffectSucceeded(ctx, claim, result)
}

func (s *effectStore) MarkFailed(ctx context.Context, claim persistence.EffectClaim, cause error, retryAt *time.Time) error {
	return s.owner.markEffectFailed(ctx, claim, cause, retryAt)
}

func (s *effectStore) MarkTerminalFailed(ctx context.Context, claim persistence.EffectClaim, cause error) error {
	return s.owner.markEffectTerminalFailed(ctx, claim, cause)
}

func (s *effectStore) RequireReconciliation(ctx context.Context, claim persistence.EffectClaim, cause error) error {
	return s.owner.requireReconciliation(ctx, claim, cause)
}

func (s *effectStore) ListUnfinished(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.EffectRecord, error) {
	return s.owner.listUnfinishedEffects(ctx, runtimeID)
}

func (s *Store) markEffectStarted(ctx context.Context, claim persistence.EffectClaim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, err := s.claimedEffect(claim)
	if err != nil {
		return err
	}
	now := s.now()
	record.Status = persistence.EffectStarted
	record.StartedAt = &now
	s.effects[record.EffectID] = record
	return nil
}

func (s *Store) renewEffectClaim(ctx context.Context, claim persistence.EffectClaim, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, err := s.claimedEffect(claim)
	if err != nil {
		return err
	}
	if lease <= 0 {
		lease = time.Minute
	}
	until := s.now().Add(lease)
	record.LeaseUntil = &until
	s.effects[record.EffectID] = record
	return nil
}

func (s *Store) markEffectSucceeded(ctx context.Context, claim persistence.EffectClaim, result persistence.EffectResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, err := s.claimedEffect(claim)
	if err != nil {
		return err
	}
	now := s.now()
	record.Status = persistence.EffectSucceeded
	record.Result = contract.CloneRaw(result.Payload)
	record.ResultMetadata = contract.CloneStrings(result.Metadata)
	record.CompletedAt = &now
	record.LeaseOwner, record.LeaseToken, record.LeaseUntil = "", "", nil
	s.effects[record.EffectID] = record
	return nil
}

func (s *Store) markEffectFailed(ctx context.Context, claim persistence.EffectClaim, cause error, retryAt *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, err := s.claimedEffect(claim)
	if err != nil {
		return err
	}
	record.Status = persistence.EffectFailed
	if retryAt != nil {
		record.AvailableAt = *retryAt
	} else {
		record.AvailableAt = s.now()
	}
	if cause != nil {
		record.LastError = cause.Error()
	}
	record.LeaseOwner, record.LeaseToken, record.LeaseUntil = "", "", nil
	s.effects[record.EffectID] = record
	return nil
}

func (s *Store) markEffectTerminalFailed(ctx context.Context, claim persistence.EffectClaim, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, err := s.claimedEffect(claim)
	if err != nil {
		return err
	}
	now := s.now()
	record.Status = persistence.EffectTerminalFailed
	if cause != nil {
		record.LastError = cause.Error()
	}
	record.CompletedAt = &now
	record.LeaseOwner, record.LeaseToken, record.LeaseUntil = "", "", nil
	s.effects[record.EffectID] = record
	return nil
}

func (s *Store) requireReconciliation(ctx context.Context, claim persistence.EffectClaim, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, err := s.claimedEffect(claim)
	if err != nil {
		return err
	}
	record.Status = persistence.EffectReconciliationRequired
	if cause != nil {
		record.LastError = cause.Error()
	}
	record.LeaseOwner, record.LeaseToken, record.LeaseUntil = "", "", nil
	s.effects[record.EffectID] = record
	return nil
}

func (s *Store) listUnfinishedEffects(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.EffectRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	result := make([]persistence.EffectRecord, 0)
	for _, effectID := range s.effectOrder {
		record := s.effects[effectID]
		if runtimeID != "" && record.RuntimeID != runtimeID {
			continue
		}
		if record.Status != persistence.EffectSucceeded && record.Status != persistence.EffectTerminalFailed {
			result = append(result, record.Clone())
		}
	}
	return result, nil
}

func (s *Store) claimEffect(ctx context.Context, effectID string, ownerID string, lease time.Duration) (persistence.EffectClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return persistence.EffectClaim{}, err
	}
	record, ok := s.effects[effectID]
	if !ok {
		return persistence.EffectClaim{}, fmt.Errorf("effect %q not found", effectID)
	}
	now := s.now()
	if record.LeaseUntil != nil && record.LeaseUntil.After(now) && record.LeaseOwner != ownerID {
		return persistence.EffectClaim{}, persistence.ErrLeaseLost
	}
	if lease <= 0 {
		lease = time.Minute
	}
	record.Attempt++
	record.LeaseOwner = ownerID
	record.LeaseToken = s.token("effect")
	until := now.Add(lease)
	record.LeaseUntil = &until
	s.effects[effectID] = record
	return persistence.EffectClaim{Record: record.Clone(), LeaseToken: record.LeaseToken}, nil
}

func (s *Store) claimedEffect(claim persistence.EffectClaim) (persistence.EffectRecord, error) {
	record, ok := s.effects[claim.Record.EffectID]
	if !ok || record.LeaseToken != claim.LeaseToken {
		return persistence.EffectRecord{}, persistence.ErrLeaseLost
	}
	return record, nil
}

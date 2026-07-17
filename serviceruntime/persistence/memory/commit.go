package memory

import (
	"agent/serviceruntime/persistence"
	"context"
	"fmt"
)

func (s *Store) CommitMessage(ctx context.Context, commit persistence.MessageCommit) (persistence.CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return persistence.CommitResult{}, err
	}
	inbox, ok := s.inbox[commit.Ack.InboxID]
	if !ok || inbox.Message.ID != commit.Ack.MessageID {
		return persistence.CommitResult{}, fmt.Errorf("inbox acknowledgement does not match a stored message")
	}
	if inbox.Status == persistence.InboxAcked {
		return persistence.CommitResult{LastSequence: uint64(len(s.events[commit.StreamID])), Duplicate: true}, nil
	}
	if inbox.Status != persistence.InboxClaimed || inbox.LeaseToken != commit.Ack.LeaseToken {
		return persistence.CommitResult{}, persistence.ErrLeaseLost
	}
	lease, leased := s.leases[commit.InstanceID]
	if !leased || lease.Epoch != commit.ActivationEpoch || !lease.LeaseUntil.After(s.now()) {
		return persistence.CommitResult{}, persistence.ErrStaleActivation
	}
	currentSequence := uint64(len(s.events[commit.StreamID]))
	if currentSequence != commit.ExpectedSequence {
		return persistence.CommitResult{}, persistence.ErrSequenceConflict
	}
	for index, event := range commit.Events {
		expected := currentSequence + uint64(index) + 1
		if event.StreamID != commit.StreamID || event.Sequence != expected || event.EventID == "" {
			return persistence.CommitResult{}, fmt.Errorf("event sequence or identity is invalid")
		}
		if _, exists := s.eventIDs[event.EventID]; exists {
			return persistence.CommitResult{}, persistence.ErrDuplicateID
		}
	}
	lastSequence := currentSequence + uint64(len(commit.Events))
	if commit.Snapshot != nil {
		if commit.Snapshot.StreamID != commit.StreamID || commit.Snapshot.LastSequence != lastSequence {
			return persistence.CommitResult{}, fmt.Errorf("snapshot does not match the committed stream position")
		}
	}
	for _, record := range commit.Outbox {
		if record.OutboxID == "" || record.Message.ID == "" {
			return persistence.CommitResult{}, fmt.Errorf("outbox and message ids are required")
		}
		if _, exists := s.outbox[record.OutboxID]; exists {
			return persistence.CommitResult{}, persistence.ErrDuplicateID
		}
	}
	for _, record := range commit.Effects {
		if record.EffectID == "" || record.IdempotencyKey == "" {
			return persistence.CommitResult{}, fmt.Errorf("effect id and idempotency key are required")
		}
		if _, exists := s.effects[record.EffectID]; exists {
			return persistence.CommitResult{}, persistence.ErrDuplicateID
		}
	}

	result := persistence.CommitResult{LastSequence: lastSequence}
	for _, event := range commit.Events {
		cloned := event.Clone()
		s.events[commit.StreamID] = append(s.events[commit.StreamID], cloned)
		s.eventIDs[event.EventID] = commit.StreamID
		result.StoredEventIDs = append(result.StoredEventIDs, event.EventID)
	}
	if commit.Snapshot != nil {
		s.snapshots[commit.StreamID] = commit.Snapshot.Clone()
	}
	now := s.now()
	for _, record := range commit.Outbox {
		record = record.Clone()
		record.InstanceID = commit.InstanceID
		if record.Status == "" {
			record.Status = persistence.OutboxPending
		}
		if record.AvailableAt.IsZero() {
			record.AvailableAt = now
		}
		if record.CreatedAt.IsZero() {
			record.CreatedAt = now
		}
		s.outbox[record.OutboxID] = record
		s.outboxOrder = append(s.outboxOrder, record.OutboxID)
		result.StoredOutboxIDs = append(result.StoredOutboxIDs, record.OutboxID)
	}
	for _, record := range commit.Effects {
		record = record.Clone()
		record.RuntimeID = commit.RuntimeID
		record.PlanRevision = commit.PlanRevision
		record.InstanceID = commit.InstanceID
		if record.Status == "" {
			record.Status = persistence.EffectPlanned
		}
		if record.PlannedAt.IsZero() {
			record.PlannedAt = now
		}
		if record.AvailableAt.IsZero() {
			record.AvailableAt = now
		}
		s.effects[record.EffectID] = record
		s.effectOrder = append(s.effectOrder, record.EffectID)
		result.StoredEffectIDs = append(result.StoredEffectIDs, record.EffectID)
	}
	inbox.Status = persistence.InboxAcked
	inbox.AckedAt = &commit.Ack.AckedAt
	inbox.LeaseOwner, inbox.LeaseToken, inbox.LeaseUntil = "", "", nil
	s.inbox[inbox.InboxID] = inbox
	return result, nil
}

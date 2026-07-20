package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

type inboxStore struct{ owner *Store }

func (s *inboxStore) Enqueue(ctx context.Context, target instance.DeliveryTarget, message contract.Message) (persistence.InboxRecord, bool, error) {
	return s.owner.enqueueInbox(ctx, target, message)
}

func (s *inboxStore) ClaimNext(ctx context.Context, mailboxID contract.MailboxID, ownerID string, lease time.Duration) (persistence.InboxClaim, bool, error) {
	return s.owner.claimNextInbox(ctx, mailboxID, ownerID, lease)
}

func (s *inboxStore) RenewClaim(ctx context.Context, claim persistence.InboxClaim, lease time.Duration) error {
	return s.owner.renewInboxClaim(ctx, claim, lease)
}

func (s *inboxStore) ReleaseClaim(ctx context.Context, claim persistence.InboxClaim, retryAt time.Time, cause error) error {
	return s.owner.releaseInboxClaim(ctx, claim, retryAt, cause)
}

func (s *inboxStore) MoveToDeadLetter(ctx context.Context, claim persistence.InboxClaim, cause error) error {
	return s.owner.deadLetterInbox(ctx, claim, cause)
}

func (s *inboxStore) CountPending(ctx context.Context, mailboxID contract.MailboxID) (int, error) {
	return s.owner.countPendingInbox(ctx, mailboxID)
}

func (s *Store) enqueueInbox(ctx context.Context, target instance.DeliveryTarget, message contract.Message) (persistence.InboxRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return persistence.InboxRecord{}, false, err
	}
	if err := message.Validate(); err != nil {
		return persistence.InboxRecord{}, false, err
	}
	if message.StreamID != "" && message.Sequence == 0 {
		return persistence.InboxRecord{}, false, fmt.Errorf("ordered message stream %q requires a sequence", message.StreamID)
	}
	dedupeKey := string(target.MailboxID) + "\x00" + message.ID
	if inboxID, exists := s.inboxDedupe[dedupeKey]; exists {
		return s.inbox[inboxID].Clone(), true, nil
	}
	now := s.now()
	inboxID := stableID("inbox", dedupeKey)
	record := persistence.InboxRecord{
		InboxID: inboxID, MailboxID: target.MailboxID, InstanceID: target.InstanceID,
		Message: message.Clone(), Status: persistence.InboxPending,
		AvailableAt: now, ReceivedAt: now,
	}
	s.inbox[inboxID] = record
	s.inboxOrder[target.MailboxID] = append(s.inboxOrder[target.MailboxID], inboxID)
	s.inboxDedupe[dedupeKey] = inboxID
	return record.Clone(), false, nil
}

func (s *Store) claimNextInbox(ctx context.Context, mailboxID contract.MailboxID, ownerID string, lease time.Duration) (persistence.InboxClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return persistence.InboxClaim{}, false, err
	}
	now := s.now()
	if lease <= 0 {
		lease = 30 * time.Second
	}
	for _, inboxID := range s.inboxOrder[mailboxID] {
		record := s.inbox[inboxID]
		claimable := (record.Status == persistence.InboxPending || record.Status == persistence.InboxRetry) && !record.AvailableAt.After(now)
		if record.Status == persistence.InboxClaimed && record.LeaseUntil != nil && !record.LeaseUntil.After(now) {
			claimable = true
		}
		if !claimable {
			continue
		}
		if !s.canClaimInbox(record) {
			continue
		}
		record.Status = persistence.InboxClaimed
		record.Attempt++
		record.Message.Attempt = record.Attempt
		record.LeaseOwner = ownerID
		record.LeaseToken = s.token("inbox")
		until := now.Add(lease)
		record.LeaseUntil = &until
		s.inbox[inboxID] = record
		return persistence.InboxClaim{Record: record.Clone(), LeaseToken: record.LeaseToken}, true, nil
	}
	return persistence.InboxClaim{}, false, nil
}

func (s *Store) renewInboxClaim(ctx context.Context, claim persistence.InboxClaim, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, ok := s.inbox[claim.Record.InboxID]
	if !ok || record.Status != persistence.InboxClaimed || record.LeaseToken != claim.LeaseToken {
		return persistence.ErrLeaseLost
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	until := s.now().Add(lease)
	record.LeaseUntil = &until
	s.inbox[record.InboxID] = record
	return nil
}

func (s *Store) releaseInboxClaim(ctx context.Context, claim persistence.InboxClaim, retryAt time.Time, cause error) error {
	return s.finishInboxClaim(ctx, claim, persistence.InboxRetry, retryAt, cause)
}

func (s *Store) deadLetterInbox(ctx context.Context, claim persistence.InboxClaim, cause error) error {
	return s.finishInboxClaim(ctx, claim, persistence.InboxDeadLetter, time.Time{}, cause)
}

func (s *Store) finishInboxClaim(ctx context.Context, claim persistence.InboxClaim, status persistence.InboxStatus, retryAt time.Time, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	record, ok := s.inbox[claim.Record.InboxID]
	if !ok || record.Status != persistence.InboxClaimed || record.LeaseToken != claim.LeaseToken {
		return persistence.ErrLeaseLost
	}
	if status == persistence.InboxDeadLetter {
		if err := s.advanceInboxHead(record.Message, record.MailboxID, s.inboxHeads); err != nil {
			return err
		}
	}
	record.Status = status
	record.LeaseOwner = ""
	record.LeaseToken = ""
	record.LeaseUntil = nil
	if !retryAt.IsZero() {
		record.AvailableAt = retryAt
	}
	if cause != nil {
		record.LastError = cause.Error()
	}
	s.inbox[record.InboxID] = record
	return nil
}

func (s *Store) canClaimInbox(record persistence.InboxRecord) bool {
	message := record.Message
	if message.StreamID == "" || message.Sequence == 0 {
		return true
	}
	head, found := s.inboxHeads[inboxStreamKey{mailbox: record.MailboxID, stream: message.StreamID}]
	return !found && message.Sequence == 1 || found && message.Sequence == head+1
}

func (s *Store) advanceInboxHead(message contract.Message, mailboxID contract.MailboxID, heads map[inboxStreamKey]uint64) error {
	if message.StreamID == "" || message.Sequence == 0 {
		return nil
	}
	key := inboxStreamKey{mailbox: mailboxID, stream: message.StreamID}
	head, found := heads[key]
	if !found && message.Sequence != 1 {
		return fmt.Errorf("inbox stream %q starts with sequence %d instead of 1", message.StreamID, message.Sequence)
	}
	if found && message.Sequence != head+1 {
		return fmt.Errorf("inbox stream %q sequence %d is not next after %d", message.StreamID, message.Sequence, head)
	}
	heads[key] = message.Sequence
	return nil
}

func (s *Store) countPendingInbox(ctx context.Context, mailboxID contract.MailboxID) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return 0, err
	}
	count := 0
	for _, inboxID := range s.inboxOrder[mailboxID] {
		switch s.inbox[inboxID].Status {
		case persistence.InboxPending, persistence.InboxRetry, persistence.InboxClaimed:
			count++
		}
	}
	return count, nil
}

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(sum[:12]))
}

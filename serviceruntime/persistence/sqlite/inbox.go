package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

const inboxColumns = `inbox_id, mailbox_id, instance_id, message, status, attempt, available_at,
	lease_owner, lease_token, lease_until, received_at, acked_at, last_error`

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
	return s.owner.finishInboxClaim(ctx, claim, persistence.InboxRetry, retryAt, cause)
}

func (s *inboxStore) MoveToDeadLetter(ctx context.Context, claim persistence.InboxClaim, cause error) error {
	return s.owner.finishInboxClaim(ctx, claim, persistence.InboxDeadLetter, time.Time{}, cause)
}

func (s *inboxStore) CountPending(ctx context.Context, mailboxID contract.MailboxID) (int, error) {
	var count int
	err := s.owner.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbox WHERE mailbox_id = ? AND status IN (?, ?, ?)`,
		mailboxID, persistence.InboxPending, persistence.InboxRetry, persistence.InboxClaimed).Scan(&count)
	return count, err
}

func (s *Store) enqueueInbox(ctx context.Context, target instance.DeliveryTarget, message contract.Message) (persistence.InboxRecord, bool, error) {
	if target.MailboxID == "" || target.InstanceID == "" {
		return persistence.InboxRecord{}, false, fmt.Errorf("inbox delivery target mailbox and instance are required")
	}
	if err := message.Validate(); err != nil {
		return persistence.InboxRecord{}, false, err
	}
	if message.RuntimeID != target.RuntimeID || message.PlanRevision != target.PlanRevision {
		return persistence.InboxRecord{}, false, fmt.Errorf("message runtime or plan revision does not match inbox target")
	}
	encoded, err := encodeJSON(message.Clone())
	if err != nil {
		return persistence.InboxRecord{}, false, err
	}
	now := s.now()
	id := stableRecordID("inbox", string(target.MailboxID)+"\x00"+message.ID)
	result, err := s.db.ExecContext(ctx, `INSERT INTO inbox(inbox_id, mailbox_id, instance_id, message_id, message,
		status, attempt, available_at, received_at) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
		ON CONFLICT(mailbox_id, message_id) DO NOTHING`, id, target.MailboxID, target.InstanceID, message.ID,
		encoded, persistence.InboxPending, timeValue(now), timeValue(now))
	if err != nil {
		return persistence.InboxRecord{}, false, fmt.Errorf("enqueue inbox message %q: %w", message.ID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return persistence.InboxRecord{}, false, err
	}
	record, err := scanInbox(s.db.QueryRowContext(ctx, `SELECT `+inboxColumns+` FROM inbox WHERE mailbox_id = ? AND message_id = ?`, target.MailboxID, message.ID))
	if err != nil {
		return persistence.InboxRecord{}, false, fmt.Errorf("reload inbox message %q: %w", message.ID, err)
	}
	return record, changed == 0, nil
}

func (s *Store) claimNextInbox(ctx context.Context, mailboxID contract.MailboxID, ownerID string, lease time.Duration) (persistence.InboxClaim, bool, error) {
	if ownerID == "" {
		return persistence.InboxClaim{}, false, fmt.Errorf("inbox claim owner id is required")
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	token, err := newToken("inbox")
	if err != nil {
		return persistence.InboxClaim{}, false, err
	}
	now := s.now()
	row := s.db.QueryRowContext(ctx, `UPDATE inbox SET status = ?, attempt = attempt + 1,
		lease_owner = ?, lease_token = ?, lease_until = ?
		WHERE ordering_id = (
			SELECT ordering_id FROM inbox WHERE mailbox_id = ? AND (
				(status IN (?, ?) AND available_at <= ?) OR
				(status = ? AND lease_until > 0 AND lease_until <= ?)
			) ORDER BY ordering_id LIMIT 1
		) RETURNING `+inboxColumns,
		persistence.InboxClaimed, ownerID, token, timeValue(now.Add(lease)), mailboxID,
		persistence.InboxPending, persistence.InboxRetry, timeValue(now), persistence.InboxClaimed, timeValue(now))
	record, err := scanInbox(row)
	if err == sql.ErrNoRows {
		return persistence.InboxClaim{}, false, nil
	}
	if err != nil {
		return persistence.InboxClaim{}, false, fmt.Errorf("claim inbox %q: %w", mailboxID, err)
	}
	return persistence.InboxClaim{Record: record, LeaseToken: token}, true, nil
}

func (s *Store) renewInboxClaim(ctx context.Context, claim persistence.InboxClaim, lease time.Duration) error {
	if lease <= 0 {
		lease = 30 * time.Second
	}
	result, err := s.db.ExecContext(ctx, `UPDATE inbox SET lease_until = ? WHERE inbox_id = ? AND status = ? AND lease_token = ?`,
		timeValue(s.now().Add(lease)), claim.Record.InboxID, persistence.InboxClaimed, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

func (s *Store) finishInboxClaim(ctx context.Context, claim persistence.InboxClaim, status persistence.InboxStatus, retryAt time.Time, cause error) error {
	availableAt := claim.Record.AvailableAt
	if !retryAt.IsZero() {
		availableAt = retryAt
	}
	ackedAt := int64(0)
	if status == persistence.InboxAcked {
		ackedAt = timeValue(s.now())
	}
	result, err := s.db.ExecContext(ctx, `UPDATE inbox SET status = ?, available_at = ?, lease_owner = '',
		lease_token = '', lease_until = 0, acked_at = CASE WHEN ? > 0 THEN ? ELSE acked_at END, last_error = ?
		WHERE inbox_id = ? AND status = ? AND lease_token = ?`, status, timeValue(availableAt), ackedAt, ackedAt,
		errorText(cause), claim.Record.InboxID, persistence.InboxClaimed, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

type inboxScanner interface {
	Scan(dest ...interface{}) error
}

func scanInbox(scanner inboxScanner) (persistence.InboxRecord, error) {
	var record persistence.InboxRecord
	var message []byte
	var availableAt, leaseUntil, receivedAt, ackedAt int64
	err := scanner.Scan(&record.InboxID, &record.MailboxID, &record.InstanceID, &message, &record.Status,
		&record.Attempt, &availableAt, &record.LeaseOwner, &record.LeaseToken, &leaseUntil,
		&receivedAt, &ackedAt, &record.LastError)
	if err != nil {
		return persistence.InboxRecord{}, err
	}
	if err := decodeJSON(message, &record.Message); err != nil {
		return persistence.InboxRecord{}, err
	}
	record.Message.Attempt = record.Attempt
	record.AvailableAt, record.ReceivedAt = timeFromValue(availableAt), timeFromValue(receivedAt)
	record.LeaseUntil, record.AckedAt = timePointer(leaseUntil), timePointer(ackedAt)
	return record.Clone(), nil
}

func stableRecordID(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + "-" + hex.EncodeToString(sum[:16])
}

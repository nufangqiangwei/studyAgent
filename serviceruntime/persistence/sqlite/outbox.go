package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"database/sql"
	"fmt"
	"time"
)

const outboxColumns = `outbox_id, instance_id, message, status, attempt, available_at,
	lease_owner, lease_token, lease_until, created_at, delivered_at, last_error`

type outboxStore struct{ owner *Store }

func (s *outboxStore) ClaimNext(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (persistence.OutboxClaim, bool, error) {
	return s.owner.claimNextOutbox(ctx, runtimeID, ownerID, lease)
}

func (s *outboxStore) RenewClaim(ctx context.Context, claim persistence.OutboxClaim, lease time.Duration) error {
	if lease <= 0 {
		lease = 30 * time.Second
	}
	result, err := s.owner.db.ExecContext(ctx, `UPDATE outbox SET lease_until = ?
		WHERE outbox_id = ? AND status = ? AND lease_token = ?`, timeValue(s.owner.now().Add(lease)),
		claim.Record.OutboxID, persistence.OutboxClaimed, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

func (s *outboxStore) MarkDelivered(ctx context.Context, claim persistence.OutboxClaim, _ persistence.DeliverySummary) error {
	return s.owner.finishOutbox(ctx, claim, persistence.OutboxDelivered, time.Time{}, nil)
}

func (s *outboxStore) MarkRetry(ctx context.Context, claim persistence.OutboxClaim, retryAt time.Time, cause error) error {
	return s.owner.finishOutbox(ctx, claim, persistence.OutboxRetry, retryAt, cause)
}

func (s *outboxStore) MoveToDeadLetter(ctx context.Context, claim persistence.OutboxClaim, cause error) error {
	return s.owner.finishOutbox(ctx, claim, persistence.OutboxDead, time.Time{}, cause)
}

func (s *outboxStore) CountPending(ctx context.Context, runtimeID contract.RuntimeID) (int, error) {
	var count int
	err := s.owner.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE runtime_id = ? AND status IN (?, ?, ?)`,
		runtimeID, persistence.OutboxPending, persistence.OutboxRetry, persistence.OutboxClaimed).Scan(&count)
	return count, err
}

func (s *Store) claimNextOutbox(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (persistence.OutboxClaim, bool, error) {
	if ownerID == "" {
		return persistence.OutboxClaim{}, false, fmt.Errorf("outbox claim owner id is required")
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	token, err := newToken("outbox")
	if err != nil {
		return persistence.OutboxClaim{}, false, err
	}
	now := s.now()
	record, err := scanOutbox(s.db.QueryRowContext(ctx, `UPDATE outbox SET status = ?, attempt = attempt + 1,
		lease_owner = ?, lease_token = ?, lease_until = ? WHERE ordering_id = (
			SELECT ordering_id FROM outbox WHERE runtime_id = ? AND (
				(status IN (?, ?) AND available_at <= ?) OR
				(status = ? AND lease_until > 0 AND lease_until <= ?)
			) ORDER BY ordering_id LIMIT 1
		) RETURNING `+outboxColumns,
		persistence.OutboxClaimed, ownerID, token, timeValue(now.Add(lease)), runtimeID,
		persistence.OutboxPending, persistence.OutboxRetry, timeValue(now), persistence.OutboxClaimed, timeValue(now)))
	if err == sql.ErrNoRows {
		return persistence.OutboxClaim{}, false, nil
	}
	if err != nil {
		return persistence.OutboxClaim{}, false, fmt.Errorf("claim runtime outbox: %w", err)
	}
	return persistence.OutboxClaim{Record: record, LeaseToken: token}, true, nil
}

func (s *Store) finishOutbox(ctx context.Context, claim persistence.OutboxClaim, status persistence.OutboxStatus, retryAt time.Time, cause error) error {
	availableAt := claim.Record.AvailableAt
	if !retryAt.IsZero() {
		availableAt = retryAt
	}
	deliveredAt := int64(0)
	if status == persistence.OutboxDelivered {
		deliveredAt = timeValue(s.now())
	}
	result, err := s.db.ExecContext(ctx, `UPDATE outbox SET status = ?, available_at = ?, lease_owner = '',
		lease_token = '', lease_until = 0, delivered_at = CASE WHEN ? > 0 THEN ? ELSE delivered_at END,
		last_error = ? WHERE outbox_id = ? AND status = ? AND lease_token = ?`,
		status, timeValue(availableAt), deliveredAt, deliveredAt, errorText(cause), claim.Record.OutboxID,
		persistence.OutboxClaimed, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

type outboxScanner interface {
	Scan(dest ...interface{}) error
}

func scanOutbox(scanner outboxScanner) (persistence.OutboxRecord, error) {
	var record persistence.OutboxRecord
	var message []byte
	var availableAt, leaseUntil, createdAt, deliveredAt int64
	err := scanner.Scan(&record.OutboxID, &record.InstanceID, &message, &record.Status, &record.Attempt,
		&availableAt, &record.LeaseOwner, &record.LeaseToken, &leaseUntil, &createdAt, &deliveredAt, &record.LastError)
	if err != nil {
		return persistence.OutboxRecord{}, err
	}
	if err := decodeJSON(message, &record.Message); err != nil {
		return persistence.OutboxRecord{}, err
	}
	record.Message.Attempt = record.Attempt
	record.AvailableAt, record.CreatedAt = timeFromValue(availableAt), timeFromValue(createdAt)
	record.LeaseUntil, record.DeliveredAt = timePointer(leaseUntil), timePointer(deliveredAt)
	return record.Clone(), nil
}

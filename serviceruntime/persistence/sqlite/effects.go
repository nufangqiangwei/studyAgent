package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"database/sql"
	"fmt"
	"time"
)

const effectColumns = `effect_id, runtime_id, plan_revision, instance_id, source_message_id,
	effect_type, version, executor_ref, idempotency_key, status, attempt, available_at,
	payload, result, metadata, result_metadata, deadline, last_error, planned_at, started_at, completed_at,
	lease_owner, lease_token, lease_until`

type effectStore struct{ owner *Store }

func (s *effectStore) RenewClaim(ctx context.Context, claim persistence.EffectClaim, lease time.Duration) error {
	if lease <= 0 {
		lease = time.Minute
	}
	result, err := s.owner.db.ExecContext(ctx, `UPDATE effects SET lease_until = ?
		WHERE effect_id = ? AND lease_token = ?`, timeValue(s.owner.now().Add(lease)), claim.Record.EffectID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

func (s *effectStore) ClaimNext(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (persistence.EffectClaim, bool, error) {
	return s.owner.claimNextEffect(ctx, runtimeID, ownerID, lease)
}

func (s *effectStore) Claim(ctx context.Context, effectID string, ownerID string, lease time.Duration) (persistence.EffectClaim, error) {
	return s.owner.claimEffect(ctx, effectID, ownerID, lease)
}

func (s *effectStore) MarkStarted(ctx context.Context, claim persistence.EffectClaim) error {
	now := s.owner.now()
	result, err := s.owner.db.ExecContext(ctx, `UPDATE effects SET status = ?, started_at = ? WHERE effect_id = ? AND lease_token = ?`,
		persistence.EffectStarted, timeValue(now), claim.Record.EffectID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

func (s *effectStore) MarkSucceeded(ctx context.Context, claim persistence.EffectClaim, completion persistence.EffectResult) error {
	now := s.owner.now()
	metadata, err := encodeJSON(completion.Metadata)
	if err != nil {
		return err
	}
	result, err := s.owner.db.ExecContext(ctx, `UPDATE effects SET status = ?, result = ?, result_metadata = ?, completed_at = ?,
		lease_owner = '', lease_token = '', lease_until = 0 WHERE effect_id = ? AND lease_token = ?`,
		persistence.EffectSucceeded, []byte(completion.Payload), metadata, timeValue(now), claim.Record.EffectID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

func (s *effectStore) MarkFailed(ctx context.Context, claim persistence.EffectClaim, cause error, retryAt *time.Time) error {
	availableAt := s.owner.now()
	if retryAt != nil {
		availableAt = retryAt.UTC()
	}
	result, err := s.owner.db.ExecContext(ctx, `UPDATE effects SET status = ?, available_at = ?, last_error = ?,
		lease_owner = '', lease_token = '', lease_until = 0 WHERE effect_id = ? AND lease_token = ?`,
		persistence.EffectFailed, timeValue(availableAt), errorText(cause), claim.Record.EffectID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

func (s *effectStore) MarkTerminalFailed(ctx context.Context, claim persistence.EffectClaim, cause error) error {
	now := s.owner.now()
	result, err := s.owner.db.ExecContext(ctx, `UPDATE effects SET status = ?, completed_at = ?, last_error = ?,
		lease_owner = '', lease_token = '', lease_until = 0 WHERE effect_id = ? AND lease_token = ?`,
		persistence.EffectTerminalFailed, timeValue(now), errorText(cause), claim.Record.EffectID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

func (s *effectStore) RequireReconciliation(ctx context.Context, claim persistence.EffectClaim, cause error) error {
	result, err := s.owner.db.ExecContext(ctx, `UPDATE effects SET status = ?, available_at = ?, last_error = ?,
		lease_owner = '', lease_token = '', lease_until = 0 WHERE effect_id = ? AND lease_token = ?`,
		persistence.EffectReconciliationRequired, timeValue(s.owner.now().Add(time.Second)), errorText(cause), claim.Record.EffectID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return rowsChanged(result, persistence.ErrLeaseLost)
}

func (s *effectStore) ListUnfinished(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.EffectRecord, error) {
	rows, err := s.owner.db.QueryContext(ctx, `SELECT `+effectColumns+` FROM effects WHERE runtime_id = ?
		AND status NOT IN (?, ?) ORDER BY ordering_id`, runtimeID, persistence.EffectSucceeded, persistence.EffectTerminalFailed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []persistence.EffectRecord
	for rows.Next() {
		record, err := scanEffect(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) claimNextEffect(ctx context.Context, runtimeID contract.RuntimeID, ownerID string, lease time.Duration) (persistence.EffectClaim, bool, error) {
	if ownerID == "" {
		return persistence.EffectClaim{}, false, fmt.Errorf("effect claim owner id is required")
	}
	if lease <= 0 {
		lease = time.Minute
	}
	token, err := newToken("effect")
	if err != nil {
		return persistence.EffectClaim{}, false, err
	}
	now := s.now()
	record, err := scanEffect(s.db.QueryRowContext(ctx, `UPDATE effects SET attempt = attempt + 1,
		lease_owner = ?, lease_token = ?, lease_until = ? WHERE ordering_id = (
			SELECT ordering_id FROM effects WHERE runtime_id = ? AND (
				(status IN (?, ?) AND available_at <= ?) OR
				(status = ? AND lease_until > 0 AND lease_until <= ?) OR
				(status = ? AND available_at <= ? AND (lease_until = 0 OR lease_until <= ?))
			) ORDER BY ordering_id LIMIT 1
		) RETURNING `+effectColumns,
		ownerID, token, timeValue(now.Add(lease)), runtimeID,
		persistence.EffectPlanned, persistence.EffectFailed, timeValue(now),
		persistence.EffectStarted, timeValue(now), persistence.EffectReconciliationRequired, timeValue(now), timeValue(now)))
	if err == sql.ErrNoRows {
		return persistence.EffectClaim{}, false, nil
	}
	if err != nil {
		return persistence.EffectClaim{}, false, fmt.Errorf("claim runtime effect: %w", err)
	}
	return persistence.EffectClaim{Record: record, LeaseToken: token}, true, nil
}

func (s *Store) claimEffect(ctx context.Context, effectID, ownerID string, lease time.Duration) (persistence.EffectClaim, error) {
	if ownerID == "" {
		return persistence.EffectClaim{}, fmt.Errorf("effect claim owner id is required")
	}
	if lease <= 0 {
		lease = time.Minute
	}
	token, err := newToken("effect")
	if err != nil {
		return persistence.EffectClaim{}, err
	}
	now := s.now()
	record, err := scanEffect(s.db.QueryRowContext(ctx, `UPDATE effects SET attempt = attempt + 1,
		lease_owner = ?, lease_token = ?, lease_until = ? WHERE effect_id = ?
		AND status NOT IN (?, ?) AND (lease_until = 0 OR lease_until <= ? OR lease_owner = ?)
		RETURNING `+effectColumns,
		ownerID, token, timeValue(now.Add(lease)), effectID, persistence.EffectSucceeded,
		persistence.EffectTerminalFailed, timeValue(now), ownerID))
	if err == sql.ErrNoRows {
		return persistence.EffectClaim{}, persistence.ErrLeaseLost
	}
	if err != nil {
		return persistence.EffectClaim{}, err
	}
	return persistence.EffectClaim{Record: record, LeaseToken: token}, nil
}

type effectScanner interface {
	Scan(dest ...interface{}) error
}

func scanEffect(scanner effectScanner) (persistence.EffectRecord, error) {
	var record persistence.EffectRecord
	var availableAt, deadline, plannedAt, startedAt, completedAt, leaseUntil int64
	var payload, result, metadata, resultMetadata []byte
	err := scanner.Scan(&record.EffectID, &record.RuntimeID, &record.PlanRevision, &record.InstanceID,
		&record.SourceMessageID, &record.Type, &record.Version, &record.ExecutorRef, &record.IdempotencyKey,
		&record.Status, &record.Attempt, &availableAt, &payload, &result, &metadata, &resultMetadata, &deadline, &record.LastError,
		&plannedAt, &startedAt, &completedAt, &record.LeaseOwner, &record.LeaseToken, &leaseUntil)
	if err != nil {
		return persistence.EffectRecord{}, err
	}
	record.AvailableAt, record.PlannedAt = timeFromValue(availableAt), timeFromValue(plannedAt)
	record.StartedAt, record.CompletedAt, record.LeaseUntil = timePointer(startedAt), timePointer(completedAt), timePointer(leaseUntil)
	record.Deadline = timePointer(deadline)
	record.Payload, record.Result = contract.CloneRaw(payload), contract.CloneRaw(result)
	if err := decodeJSON(metadata, &record.Metadata); err != nil {
		return persistence.EffectRecord{}, err
	}
	if err := decodeJSON(resultMetadata, &record.ResultMetadata); err != nil {
		return persistence.EffectRecord{}, err
	}
	return record.Clone(), nil
}

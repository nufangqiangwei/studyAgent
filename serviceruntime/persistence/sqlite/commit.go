package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *Store) CommitMessage(ctx context.Context, commit persistence.MessageCommit) (persistence.CommitResult, error) {
	if commit.RuntimeID == "" || commit.PlanRevision == "" || commit.InstanceID == "" || commit.StreamID == "" {
		return persistence.CommitResult{}, fmt.Errorf("message commit runtime, plan, instance and stream are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return persistence.CommitResult{}, fmt.Errorf("begin message commit: %w", err)
	}
	defer rollback(tx)

	var storedMessageID string
	var storedInstanceID contract.ServiceInstanceID
	var inboxStatus persistence.InboxStatus
	var inboxLeaseToken string
	var storedMessage []byte
	err = tx.QueryRowContext(ctx, `SELECT instance_id, message_id, message, status, lease_token FROM inbox WHERE inbox_id = ?`, commit.Ack.InboxID).
		Scan(&storedInstanceID, &storedMessageID, &storedMessage, &inboxStatus, &inboxLeaseToken)
	if err == sql.ErrNoRows || storedMessageID != commit.Ack.MessageID || storedInstanceID != commit.InstanceID {
		return persistence.CommitResult{}, fmt.Errorf("inbox acknowledgement does not match a stored message")
	}
	if err != nil {
		return persistence.CommitResult{}, err
	}
	var inboxMessage contract.Message
	if err := decodeJSON(storedMessage, &inboxMessage); err != nil {
		return persistence.CommitResult{}, err
	}
	if inboxMessage.RuntimeID != commit.RuntimeID || inboxMessage.PlanRevision != commit.PlanRevision {
		return persistence.CommitResult{}, fmt.Errorf("inbox message does not belong to the committed runtime plan")
	}
	if inboxStatus == persistence.InboxAcked {
		head, err := journalHeadTx(ctx, tx, commit.StreamID)
		if err != nil {
			return persistence.CommitResult{}, err
		}
		return persistence.CommitResult{LastSequence: head, Duplicate: true}, nil
	}
	if inboxStatus != persistence.InboxClaimed || inboxLeaseToken != commit.Ack.LeaseToken {
		return persistence.CommitResult{}, persistence.ErrLeaseLost
	}

	var runtimeID contract.RuntimeID
	var revision contract.PlanRevision
	var streamID contract.StreamID
	var epoch uint64
	if err := tx.QueryRowContext(ctx, `SELECT runtime_id, plan_revision, state_stream_id, activation_epoch
		FROM service_instances WHERE instance_id = ?`, commit.InstanceID).Scan(&runtimeID, &revision, &streamID, &epoch); err != nil {
		return persistence.CommitResult{}, err
	}
	if runtimeID != commit.RuntimeID || revision != commit.PlanRevision || streamID != commit.StreamID || epoch != commit.ActivationEpoch {
		return persistence.CommitResult{}, persistence.ErrStaleActivation
	}
	var leaseEpoch uint64
	var leaseUntil int64
	if err := tx.QueryRowContext(ctx, `SELECT epoch, lease_until FROM activation_leases WHERE instance_id = ?`, commit.InstanceID).
		Scan(&leaseEpoch, &leaseUntil); err != nil {
		if err == sql.ErrNoRows {
			return persistence.CommitResult{}, persistence.ErrStaleActivation
		}
		return persistence.CommitResult{}, err
	}
	if leaseEpoch != commit.ActivationEpoch || !timeFromValue(leaseUntil).After(s.now()) {
		return persistence.CommitResult{}, persistence.ErrStaleActivation
	}

	currentSequence, err := journalHeadTx(ctx, tx, commit.StreamID)
	if err != nil {
		return persistence.CommitResult{}, err
	}
	if currentSequence != commit.ExpectedSequence {
		return persistence.CommitResult{}, persistence.ErrSequenceConflict
	}
	lastSequence := currentSequence + uint64(len(commit.Events))
	if err := validateCommit(commit, currentSequence, lastSequence); err != nil {
		return persistence.CommitResult{}, err
	}

	result := persistence.CommitResult{LastSequence: lastSequence}
	for _, event := range commit.Events {
		metadata, err := encodeJSON(event.Metadata)
		if err != nil {
			return persistence.CommitResult{}, err
		}
		if exists, err := durableIDExists(ctx, tx, "journal_events", "event_id", event.EventID); err != nil {
			return persistence.CommitResult{}, err
		} else if exists {
			return persistence.CommitResult{}, persistence.ErrDuplicateID
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_events(stream_id, sequence, event_id, stream_type,
			event_type, event_version, plan_revision, service_version, correlation_id, causation_id,
			payload, metadata, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			event.StreamID, event.Sequence, event.EventID, event.StreamType, event.EventType, event.EventVersion,
			event.PlanRevision, event.ServiceVersion, event.CorrelationID, event.CausationID, []byte(event.Payload),
			metadata, timeValue(event.OccurredAt)); err != nil {
			return persistence.CommitResult{}, fmt.Errorf("append journal event %q: %w", event.EventID, err)
		}
		result.StoredEventIDs = append(result.StoredEventIDs, event.EventID)
	}
	if commit.Snapshot != nil {
		snapshot := commit.Snapshot.Clone()
		if snapshot.CreatedAt.IsZero() {
			snapshot.CreatedAt = s.now()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(stream_id, aggregate_type, owner_service,
			plan_revision, schema_version, last_sequence, state, checksum, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(stream_id) DO UPDATE SET aggregate_type = excluded.aggregate_type,
			owner_service = excluded.owner_service, plan_revision = excluded.plan_revision,
			schema_version = excluded.schema_version, last_sequence = excluded.last_sequence,
			state = excluded.state, checksum = excluded.checksum, created_at = excluded.created_at`,
			snapshot.StreamID, snapshot.AggregateType, snapshot.OwnerService, snapshot.PlanRevision,
			snapshot.SchemaVersion, snapshot.LastSequence, []byte(snapshot.State), snapshot.Checksum,
			timeValue(snapshot.CreatedAt)); err != nil {
			return persistence.CommitResult{}, fmt.Errorf("save snapshot %q: %w", snapshot.StreamID, err)
		}
	}

	now := s.now()
	for _, record := range commit.Outbox {
		if exists, err := durableIDExists(ctx, tx, "outbox", "outbox_id", record.OutboxID); err != nil {
			return persistence.CommitResult{}, err
		} else if exists {
			return persistence.CommitResult{}, persistence.ErrDuplicateID
		}
		message, err := encodeJSON(record.Message.Clone())
		if err != nil {
			return persistence.CommitResult{}, err
		}
		if record.Status == "" {
			record.Status = persistence.OutboxPending
		}
		if record.AvailableAt.IsZero() {
			record.AvailableAt = now
		}
		if record.CreatedAt.IsZero() {
			record.CreatedAt = now
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO outbox(outbox_id, runtime_id, instance_id, message_id,
			message, status, attempt, available_at, lease_owner, lease_token, lease_until, created_at,
			delivered_at, last_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			record.OutboxID, commit.RuntimeID, commit.InstanceID, record.Message.ID, message, record.Status,
			record.Attempt, timeValue(record.AvailableAt), record.LeaseOwner, record.LeaseToken,
			timeValuePointer(record.LeaseUntil), timeValue(record.CreatedAt), timeValuePointer(record.DeliveredAt),
			record.LastError); err != nil {
			return persistence.CommitResult{}, fmt.Errorf("insert outbox %q: %w", record.OutboxID, err)
		}
		result.StoredOutboxIDs = append(result.StoredOutboxIDs, record.OutboxID)
	}
	for _, record := range commit.Effects {
		if exists, err := durableIDExists(ctx, tx, "effects", "effect_id", record.EffectID); err != nil {
			return persistence.CommitResult{}, err
		} else if exists {
			return persistence.CommitResult{}, persistence.ErrDuplicateID
		}
		if record.Status == "" {
			record.Status = persistence.EffectPlanned
		}
		if record.AvailableAt.IsZero() {
			record.AvailableAt = now
		}
		if record.PlannedAt.IsZero() {
			record.PlannedAt = now
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO effects(effect_id, runtime_id, plan_revision, instance_id,
			source_message_id, effect_type, version, executor_ref, idempotency_key, status, attempt,
			available_at, payload, result, last_error, planned_at, started_at, completed_at,
			lease_owner, lease_token, lease_until) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			record.EffectID, commit.RuntimeID, commit.PlanRevision, commit.InstanceID, record.SourceMessageID,
			record.Type, record.Version, record.ExecutorRef, record.IdempotencyKey, record.Status, record.Attempt,
			timeValue(record.AvailableAt), []byte(record.Payload), []byte(record.Result), record.LastError,
			timeValue(record.PlannedAt), timeValuePointer(record.StartedAt), timeValuePointer(record.CompletedAt),
			record.LeaseOwner, record.LeaseToken, timeValuePointer(record.LeaseUntil)); err != nil {
			return persistence.CommitResult{}, fmt.Errorf("insert effect %q: %w", record.EffectID, err)
		}
		result.StoredEffectIDs = append(result.StoredEffectIDs, record.EffectID)
	}

	ackedAt := commit.Ack.AckedAt
	if ackedAt.IsZero() {
		ackedAt = now
	}
	updated, err := tx.ExecContext(ctx, `UPDATE inbox SET status = ?, acked_at = ?, lease_owner = '',
		lease_token = '', lease_until = 0 WHERE inbox_id = ? AND message_id = ? AND status = ? AND lease_token = ?`,
		persistence.InboxAcked, timeValue(ackedAt), commit.Ack.InboxID, commit.Ack.MessageID,
		persistence.InboxClaimed, commit.Ack.LeaseToken)
	if err != nil {
		return persistence.CommitResult{}, err
	}
	if err := rowsChanged(updated, persistence.ErrLeaseLost); err != nil {
		return persistence.CommitResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return persistence.CommitResult{}, fmt.Errorf("commit durable message facts: %w", err)
	}
	return result, nil
}

func validateCommit(commit persistence.MessageCommit, currentSequence, lastSequence uint64) error {
	seenEvents := make(map[string]struct{}, len(commit.Events))
	for index, event := range commit.Events {
		expected := currentSequence + uint64(index) + 1
		if event.StreamID != commit.StreamID || event.Sequence != expected || event.EventID == "" ||
			event.PlanRevision != commit.PlanRevision {
			return fmt.Errorf("event sequence, identity or plan revision is invalid")
		}
		if _, exists := seenEvents[event.EventID]; exists {
			return persistence.ErrDuplicateID
		}
		seenEvents[event.EventID] = struct{}{}
	}
	if commit.Snapshot != nil && (commit.Snapshot.StreamID != commit.StreamID ||
		commit.Snapshot.LastSequence != lastSequence || commit.Snapshot.PlanRevision != commit.PlanRevision) {
		return fmt.Errorf("snapshot does not match the committed stream position")
	}
	seenOutbox := make(map[string]struct{}, len(commit.Outbox))
	for _, record := range commit.Outbox {
		if record.OutboxID == "" || record.Message.ID == "" || record.Message.RuntimeID != commit.RuntimeID ||
			record.Message.PlanRevision != commit.PlanRevision {
			return fmt.Errorf("outbox identity, message runtime and plan revision are required")
		}
		if err := record.Message.Validate(); err != nil {
			return fmt.Errorf("outbox message %q: %w", record.Message.ID, err)
		}
		if _, exists := seenOutbox[record.OutboxID]; exists {
			return persistence.ErrDuplicateID
		}
		seenOutbox[record.OutboxID] = struct{}{}
	}
	seenEffects := make(map[string]struct{}, len(commit.Effects))
	for _, record := range commit.Effects {
		if record.EffectID == "" || record.IdempotencyKey == "" {
			return fmt.Errorf("effect id and idempotency key are required")
		}
		if _, exists := seenEffects[record.EffectID]; exists {
			return persistence.ErrDuplicateID
		}
		seenEffects[record.EffectID] = struct{}{}
	}
	return nil
}

func journalHeadTx(ctx context.Context, tx *sql.Tx, streamID contract.StreamID) (uint64, error) {
	var head uint64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM journal_events WHERE stream_id = ?`, streamID).Scan(&head)
	return head, err
}

func durableIDExists(ctx context.Context, tx *sql.Tx, table, column, value string) (bool, error) {
	var exists int
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = ?)", table, column)
	if err := tx.QueryRowContext(ctx, query, value).Scan(&exists); err != nil {
		return false, err
	}
	return exists != 0, nil
}

func timeValuePointer(value *time.Time) int64 {
	if value == nil {
		return 0
	}
	return timeValue(*value)
}

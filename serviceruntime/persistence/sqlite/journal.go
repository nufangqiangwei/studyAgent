package sqlite

import (
	"agent/serviceruntime/contract"
	"context"
	"database/sql"
	"fmt"
)

func (s *Store) LoadStream(ctx context.Context, streamID contract.StreamID, afterSequence uint64, limit int) ([]contract.StoredEvent, error) {
	query := `SELECT event_id, stream_id, stream_type, sequence, event_type, event_version,
		plan_revision, service_version, correlation_id, causation_id, payload, metadata, occurred_at
		FROM journal_events WHERE stream_id = ? AND sequence > ? ORDER BY sequence`
	args := []interface{}{streamID, afterSequence}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("load journal stream %q: %w", streamID, err)
	}
	defer rows.Close()
	var events []contract.StoredEvent
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate journal stream %q: %w", streamID, err)
	}
	return events, nil
}

func (s *Store) Head(ctx context.Context, streamID contract.StreamID) (uint64, error) {
	var value uint64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM journal_events WHERE stream_id = ?`, streamID).Scan(&value); err != nil {
		return 0, fmt.Errorf("load journal head %q: %w", streamID, err)
	}
	return value, nil
}

func (s *Store) LoadLatest(ctx context.Context, streamID contract.StreamID) (contract.Snapshot, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT stream_id, aggregate_type, owner_service, plan_revision,
		schema_version, last_sequence, state, checksum, created_at FROM snapshots WHERE stream_id = ?`, streamID)
	var snapshot contract.Snapshot
	var state []byte
	var createdAt int64
	if err := row.Scan(&snapshot.StreamID, &snapshot.AggregateType, &snapshot.OwnerService, &snapshot.PlanRevision,
		&snapshot.SchemaVersion, &snapshot.LastSequence, &state, &snapshot.Checksum, &createdAt); err != nil {
		if err == sql.ErrNoRows {
			return contract.Snapshot{}, false, nil
		}
		return contract.Snapshot{}, false, fmt.Errorf("load snapshot %q: %w", streamID, err)
	}
	snapshot.State = contract.CloneRaw(state)
	snapshot.CreatedAt = timeFromValue(createdAt)
	return snapshot.Clone(), true, nil
}

type eventScanner interface {
	Scan(dest ...interface{}) error
}

func scanEvent(scanner eventScanner) (contract.StoredEvent, error) {
	var event contract.StoredEvent
	var payload []byte
	var metadata []byte
	var occurredAt int64
	if err := scanner.Scan(&event.EventID, &event.StreamID, &event.StreamType, &event.Sequence,
		&event.EventType, &event.EventVersion, &event.PlanRevision, &event.ServiceVersion,
		&event.CorrelationID, &event.CausationID, &payload, &metadata, &occurredAt); err != nil {
		return contract.StoredEvent{}, fmt.Errorf("scan journal event: %w", err)
	}
	event.Payload = contract.CloneRaw(payload)
	if err := decodeJSON(metadata, &event.Metadata); err != nil {
		return contract.StoredEvent{}, err
	}
	event.OccurredAt = timeFromValue(occurredAt)
	return event.Clone(), nil
}

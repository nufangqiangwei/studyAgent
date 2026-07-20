package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const connectionColumns = `connection_id, runtime_id, plan_revision, owner_instance_id, owner_address,
	connection_key, driver, config, metadata, desired_open, status, last_error,
	created_at, updated_at, opened_at, closed_at`

type connectionStore struct{ owner *Store }

func (s *connectionStore) Create(ctx context.Context, record persistence.ConnectionRecord) error {
	if err := validateConnectionRecord(record); err != nil {
		return err
	}
	metadata, err := encodeJSON(record.Metadata)
	if err != nil {
		return err
	}
	_, err = s.owner.db.ExecContext(ctx, `INSERT INTO connections (`+connectionColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ConnectionID, record.RuntimeID, record.PlanRevision, record.OwnerInstanceID, record.OwnerAddress,
		record.Key, record.Driver, []byte(record.Config), metadata, record.DesiredOpen, record.Status, record.LastError,
		timeValue(record.CreatedAt), timeValue(record.UpdatedAt), nullableTimeValue(record.OpenedAt), nullableTimeValue(record.ClosedAt))
	if err != nil {
		if isConstraintError(err) {
			return persistence.ErrDuplicateID
		}
		return fmt.Errorf("create connection record: %w", err)
	}
	return nil
}

func (s *connectionStore) Update(ctx context.Context, record persistence.ConnectionRecord) error {
	if err := validateConnectionRecord(record); err != nil {
		return err
	}
	metadata, err := encodeJSON(record.Metadata)
	if err != nil {
		return err
	}
	result, err := s.owner.db.ExecContext(ctx, `UPDATE connections SET config = ?, metadata = ?, desired_open = ?,
		status = ?, last_error = ?, updated_at = ?, opened_at = ?, closed_at = ?
		WHERE connection_id = ? AND runtime_id = ? AND plan_revision = ? AND owner_instance_id = ?
		AND owner_address = ? AND connection_key = ? AND driver = ?`,
		[]byte(record.Config), metadata, record.DesiredOpen, record.Status, record.LastError,
		timeValue(record.UpdatedAt), nullableTimeValue(record.OpenedAt), nullableTimeValue(record.ClosedAt),
		record.ConnectionID, record.RuntimeID, record.PlanRevision, record.OwnerInstanceID,
		record.OwnerAddress, record.Key, record.Driver)
	if err != nil {
		return fmt.Errorf("update connection record: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("connection %q not found or identity changed", record.ConnectionID)
	}
	return nil
}

func (s *connectionStore) Get(ctx context.Context, runtimeID contract.RuntimeID, connectionID string) (persistence.ConnectionRecord, bool, error) {
	record, err := scanConnection(s.owner.db.QueryRowContext(ctx, `SELECT `+connectionColumns+`
		FROM connections WHERE runtime_id = ? AND connection_id = ?`, runtimeID, connectionID))
	if errors.Is(err, sql.ErrNoRows) {
		return persistence.ConnectionRecord{}, false, nil
	}
	if err != nil {
		return persistence.ConnectionRecord{}, false, fmt.Errorf("get connection record: %w", err)
	}
	return record, true, nil
}

func (s *connectionStore) List(ctx context.Context, runtimeID contract.RuntimeID) ([]persistence.ConnectionRecord, error) {
	rows, err := s.owner.db.QueryContext(ctx, `SELECT `+connectionColumns+`
		FROM connections WHERE runtime_id = ? ORDER BY created_at, connection_id`, runtimeID)
	if err != nil {
		return nil, fmt.Errorf("list connection records: %w", err)
	}
	defer rows.Close()
	var result []persistence.ConnectionRecord
	for rows.Next() {
		record, scanErr := scanConnection(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

type connectionScanner interface {
	Scan(dest ...interface{}) error
}

func scanConnection(scanner connectionScanner) (persistence.ConnectionRecord, error) {
	var record persistence.ConnectionRecord
	var config, metadata []byte
	var createdAt, updatedAt, openedAt, closedAt int64
	err := scanner.Scan(&record.ConnectionID, &record.RuntimeID, &record.PlanRevision,
		&record.OwnerInstanceID, &record.OwnerAddress, &record.Key, &record.Driver,
		&config, &metadata, &record.DesiredOpen, &record.Status, &record.LastError,
		&createdAt, &updatedAt, &openedAt, &closedAt)
	if err != nil {
		return persistence.ConnectionRecord{}, err
	}
	record.Config = contract.CloneRaw(config)
	if err := decodeJSON(metadata, &record.Metadata); err != nil {
		return persistence.ConnectionRecord{}, err
	}
	record.CreatedAt, record.UpdatedAt = timeFromValue(createdAt), timeFromValue(updatedAt)
	record.OpenedAt, record.ClosedAt = timePointer(openedAt), timePointer(closedAt)
	return record.Clone(), nil
}

func validateConnectionRecord(record persistence.ConnectionRecord) error {
	if record.ConnectionID == "" || record.RuntimeID == "" || record.PlanRevision == "" || record.OwnerInstanceID == "" || record.OwnerAddress == "" {
		return fmt.Errorf("connection id, runtime, plan, owner instance and owner address are required")
	}
	if record.Key == "" || record.Driver == "" {
		return fmt.Errorf("connection key and driver are required")
	}
	if !record.Status.Valid() {
		return fmt.Errorf("connection status %q is invalid", record.Status)
	}
	return nil
}

func nullableTimeValue(value *time.Time) int64 {
	if value == nil {
		return 0
	}
	return timeValue(*value)
}

func isConstraintError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "constraint")
}

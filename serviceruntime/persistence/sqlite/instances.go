package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const instanceColumns = `instance_id, address, kind, component_type, component_version,
	runtime_id, plan_revision, parent_id, root_id, depth, mailbox_id, state_stream_id,
	lifecycle, activation_epoch, record_version, created_at, updated_at, activated_at,
	passivated_at, terminated_at, last_error, metadata`

func (s *Store) Create(ctx context.Context, record instance.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	now := s.now()
	if record.RecordVersion == 0 {
		record.RecordVersion = 1
	}
	if record.Lifecycle == "" {
		record.Lifecycle = instance.Declared
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = record.CreatedAt
	}
	metadata, err := encodeJSON(record.Metadata)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO service_instances (`+instanceColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		record.InstanceID, record.Address, record.Kind, record.DefinitionRef.Type, record.DefinitionRef.Version,
		record.RuntimeID, record.PlanRevision, record.ParentID, record.RootID, record.Depth, record.MailboxID,
		record.StateStreamID, record.Lifecycle, record.ActivationEpoch, record.RecordVersion,
		timeValue(record.CreatedAt), timeValue(record.UpdatedAt), timeValuePointer(record.ActivatedAt),
		timeValuePointer(record.PassivatedAt), timeValuePointer(record.TerminatedAt), record.LastError, metadata)
	if err != nil {
		return fmt.Errorf("create service instance %q: %w", record.InstanceID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("instance %q or its durable address already exists", record.InstanceID)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, instanceID contract.ServiceInstanceID) (instance.Record, bool, error) {
	record, err := scanInstance(s.db.QueryRowContext(ctx, `SELECT `+instanceColumns+` FROM service_instances WHERE instance_id = ?`, instanceID))
	if err == sql.ErrNoRows {
		return instance.Record{}, false, nil
	}
	if err != nil {
		return instance.Record{}, false, fmt.Errorf("get service instance %q: %w", instanceID, err)
	}
	return record, true, nil
}

func (s *Store) GetByAddress(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision, address contract.ServiceAddress) (instance.Record, bool, error) {
	record, err := scanInstance(s.db.QueryRowContext(ctx, `SELECT `+instanceColumns+` FROM service_instances
		WHERE runtime_id = ? AND plan_revision = ? AND address = ?`, runtimeID, revision, address))
	if err == sql.ErrNoRows {
		return instance.Record{}, false, nil
	}
	if err != nil {
		return instance.Record{}, false, fmt.Errorf("get service address %q: %w", address, err)
	}
	return record, true, nil
}

func (s *Store) CompareAndSwap(ctx context.Context, next instance.Record, expectedRecordVersion uint64) error {
	current, found, err := s.Get(ctx, next.InstanceID)
	if err != nil {
		return err
	}
	if !found || current.RecordVersion != expectedRecordVersion {
		return persistence.ErrSequenceConflict
	}
	if current.Address != next.Address || current.RuntimeID != next.RuntimeID || current.PlanRevision != next.PlanRevision ||
		current.MailboxID != next.MailboxID || current.StateStreamID != next.StateStreamID {
		return fmt.Errorf("instance identity fields cannot change")
	}
	if !instance.CanTransition(current.Lifecycle, next.Lifecycle) {
		return fmt.Errorf("instance lifecycle transition %q -> %q is not allowed", current.Lifecycle, next.Lifecycle)
	}
	metadata, err := encodeJSON(next.Metadata)
	if err != nil {
		return err
	}
	next.RecordVersion = expectedRecordVersion + 1
	result, err := s.db.ExecContext(ctx, `UPDATE service_instances SET kind = ?, component_type = ?, component_version = ?,
		parent_id = ?, root_id = ?, depth = ?, lifecycle = ?, activation_epoch = ?, record_version = ?, updated_at = ?,
		activated_at = ?, passivated_at = ?, terminated_at = ?, last_error = ?, metadata = ?
		WHERE instance_id = ? AND record_version = ?`,
		next.Kind, next.DefinitionRef.Type, next.DefinitionRef.Version, next.ParentID, next.RootID, next.Depth,
		next.Lifecycle, next.ActivationEpoch, next.RecordVersion, timeValue(s.now()), timeValuePointer(next.ActivatedAt),
		timeValuePointer(next.PassivatedAt), timeValuePointer(next.TerminatedAt), next.LastError, metadata,
		next.InstanceID, expectedRecordVersion)
	if err != nil {
		return fmt.Errorf("compare and swap service instance %q: %w", next.InstanceID, err)
	}
	return rowsChanged(result, persistence.ErrSequenceConflict)
}

func (s *Store) List(ctx context.Context, query instance.Query) ([]instance.Record, error) {
	statement := `SELECT ` + instanceColumns + ` FROM service_instances WHERE 1 = 1`
	var args []interface{}
	if query.RuntimeID != "" {
		statement += " AND runtime_id = ?"
		args = append(args, query.RuntimeID)
	}
	if query.PlanRevision != "" {
		statement += " AND plan_revision = ?"
		args = append(args, query.PlanRevision)
	}
	if query.Kind != nil {
		statement += " AND kind = ?"
		args = append(args, *query.Kind)
	}
	if len(query.Lifecycle) > 0 {
		statement += " AND lifecycle IN (" + strings.TrimRight(strings.Repeat("?,", len(query.Lifecycle)), ",") + ")"
		for _, lifecycle := range query.Lifecycle {
			args = append(args, lifecycle)
		}
	}
	statement += " ORDER BY instance_id"
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list service instances: %w", err)
	}
	defer rows.Close()
	var records []instance.Record
	for rows.Next() {
		record, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate service instances: %w", err)
	}
	return records, nil
}

func (s *Store) Acquire(ctx context.Context, instanceID contract.ServiceInstanceID, ownerID string, duration time.Duration) (instance.ActivationLease, error) {
	if ownerID == "" {
		return instance.ActivationLease{}, fmt.Errorf("activation lease owner id is required")
	}
	if duration <= 0 {
		duration = time.Minute
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return instance.ActivationLease{}, err
	}
	defer rollback(tx)
	var epoch, recordVersion uint64
	if err := tx.QueryRowContext(ctx, `SELECT activation_epoch, record_version FROM service_instances WHERE instance_id = ?`, instanceID).Scan(&epoch, &recordVersion); err != nil {
		if err == sql.ErrNoRows {
			return instance.ActivationLease{}, fmt.Errorf("instance %q not found", instanceID)
		}
		return instance.ActivationLease{}, err
	}
	var currentOwner string
	var currentUntil int64
	err = tx.QueryRowContext(ctx, `SELECT owner_id, lease_until FROM activation_leases WHERE instance_id = ?`, instanceID).Scan(&currentOwner, &currentUntil)
	if err != nil && err != sql.ErrNoRows {
		return instance.ActivationLease{}, err
	}
	now := s.now()
	if err == nil && timeFromValue(currentUntil).After(now) && currentOwner != ownerID {
		return instance.ActivationLease{}, persistence.ErrLeaseLost
	}
	token, err := newToken("activation")
	if err != nil {
		return instance.ActivationLease{}, err
	}
	epoch++
	result, err := tx.ExecContext(ctx, `UPDATE service_instances SET activation_epoch = ?, record_version = record_version + 1,
		updated_at = ? WHERE instance_id = ? AND record_version = ?`, epoch, timeValue(now), instanceID, recordVersion)
	if err != nil {
		return instance.ActivationLease{}, err
	}
	if err := rowsChanged(result, persistence.ErrSequenceConflict); err != nil {
		return instance.ActivationLease{}, err
	}
	lease := instance.ActivationLease{InstanceID: instanceID, Epoch: epoch, OwnerID: ownerID, LeaseToken: token, AcquiredAt: now, LeaseUntil: now.Add(duration)}
	if _, err := tx.ExecContext(ctx, `INSERT INTO activation_leases(instance_id, epoch, owner_id, lease_token, acquired_at, lease_until)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(instance_id) DO UPDATE SET epoch = excluded.epoch, owner_id = excluded.owner_id,
		lease_token = excluded.lease_token, acquired_at = excluded.acquired_at, lease_until = excluded.lease_until`,
		lease.InstanceID, lease.Epoch, lease.OwnerID, lease.LeaseToken, timeValue(lease.AcquiredAt), timeValue(lease.LeaseUntil)); err != nil {
		return instance.ActivationLease{}, err
	}
	if err := tx.Commit(); err != nil {
		return instance.ActivationLease{}, err
	}
	return lease, nil
}

func (s *Store) Renew(ctx context.Context, lease instance.ActivationLease, duration time.Duration) (instance.ActivationLease, error) {
	if duration <= 0 {
		duration = time.Minute
	}
	lease.LeaseUntil = s.now().Add(duration)
	result, err := s.db.ExecContext(ctx, `UPDATE activation_leases SET lease_until = ?
		WHERE instance_id = ? AND epoch = ? AND lease_token = ?`, timeValue(lease.LeaseUntil), lease.InstanceID, lease.Epoch, lease.LeaseToken)
	if err != nil {
		return instance.ActivationLease{}, err
	}
	if err := rowsChanged(result, persistence.ErrLeaseLost); err != nil {
		return instance.ActivationLease{}, err
	}
	return lease, nil
}

func (s *Store) Release(ctx context.Context, lease instance.ActivationLease) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM activation_leases WHERE instance_id = ? AND epoch = ? AND lease_token = ?`,
		lease.InstanceID, lease.Epoch, lease.LeaseToken)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		var exists int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM activation_leases WHERE instance_id = ?`, lease.InstanceID).Scan(&exists); err != nil {
			return err
		}
		if exists > 0 {
			return persistence.ErrLeaseLost
		}
	}
	return nil
}

func (s *Store) Current(ctx context.Context, instanceID contract.ServiceInstanceID) (instance.ActivationLease, bool, error) {
	var lease instance.ActivationLease
	var acquiredAt, leaseUntil int64
	err := s.db.QueryRowContext(ctx, `SELECT instance_id, epoch, owner_id, lease_token, acquired_at, lease_until
		FROM activation_leases WHERE instance_id = ?`, instanceID).Scan(&lease.InstanceID, &lease.Epoch, &lease.OwnerID, &lease.LeaseToken, &acquiredAt, &leaseUntil)
	if err == sql.ErrNoRows {
		return instance.ActivationLease{}, false, nil
	}
	if err != nil {
		return instance.ActivationLease{}, false, err
	}
	lease.AcquiredAt, lease.LeaseUntil = timeFromValue(acquiredAt), timeFromValue(leaseUntil)
	return lease, true, nil
}

type instanceScanner interface {
	Scan(dest ...interface{}) error
}

func scanInstance(scanner instanceScanner) (instance.Record, error) {
	var record instance.Record
	var createdAt, updatedAt, activatedAt, passivatedAt, terminatedAt int64
	var metadata []byte
	err := scanner.Scan(&record.InstanceID, &record.Address, &record.Kind, &record.DefinitionRef.Type, &record.DefinitionRef.Version,
		&record.RuntimeID, &record.PlanRevision, &record.ParentID, &record.RootID, &record.Depth, &record.MailboxID,
		&record.StateStreamID, &record.Lifecycle, &record.ActivationEpoch, &record.RecordVersion,
		&createdAt, &updatedAt, &activatedAt, &passivatedAt, &terminatedAt, &record.LastError, &metadata)
	if err != nil {
		return instance.Record{}, err
	}
	if err := decodeJSON(metadata, &record.Metadata); err != nil {
		return instance.Record{}, err
	}
	record.CreatedAt, record.UpdatedAt = timeFromValue(createdAt), timeFromValue(updatedAt)
	record.ActivatedAt, record.PassivatedAt, record.TerminatedAt = timePointer(activatedAt), timePointer(passivatedAt), timePointer(terminatedAt)
	return record.Clone(), nil
}

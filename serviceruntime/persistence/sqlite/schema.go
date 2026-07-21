package sqlite

import (
	"context"
	"fmt"
)

func (s *Store) migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read sqlite runtime schema version: %w", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("sqlite runtime schema version %d is newer than supported version %d", version, schemaVersion)
	}
	if version == schemaVersion {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite runtime schema migration: %w", err)
	}
	defer rollback(tx)
	if version == 0 {
		for _, statement := range schemaStatements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply sqlite runtime schema: %w", err)
			}
		}
	} else {
		for target := version + 1; target <= schemaVersion; target++ {
			statements, ok := schemaMigrations[target]
			if !ok {
				return fmt.Errorf("sqlite runtime schema migration to version %d is not defined", target)
			}
			for _, statement := range statements {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return fmt.Errorf("apply sqlite runtime schema migration %d: %w", target, err)
				}
			}
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		return fmt.Errorf("write sqlite runtime schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite runtime schema migration: %w", err)
	}
	return nil
}

var schemaStatements = []string{
	`CREATE TABLE service_instances (
		instance_id TEXT PRIMARY KEY,
		address TEXT NOT NULL,
		kind TEXT NOT NULL,
		component_type TEXT NOT NULL,
		component_version TEXT NOT NULL,
		runtime_id TEXT NOT NULL,
		plan_revision TEXT NOT NULL,
		parent_id TEXT NOT NULL DEFAULT '',
		root_id TEXT NOT NULL DEFAULT '',
		depth INTEGER NOT NULL DEFAULT 0,
		mailbox_id TEXT NOT NULL UNIQUE,
		state_stream_id TEXT NOT NULL UNIQUE,
		lifecycle TEXT NOT NULL,
		activation_epoch INTEGER NOT NULL DEFAULT 0,
		record_version INTEGER NOT NULL,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		activated_at INTEGER NOT NULL DEFAULT 0,
		passivated_at INTEGER NOT NULL DEFAULT 0,
		terminated_at INTEGER NOT NULL DEFAULT 0,
		last_error TEXT NOT NULL DEFAULT '',
		metadata BLOB,
		UNIQUE(runtime_id, plan_revision, address)
	)`,
	`CREATE INDEX service_instances_runtime_idx ON service_instances(runtime_id, plan_revision, lifecycle, kind)`,
	`CREATE TABLE activation_leases (
		instance_id TEXT PRIMARY KEY,
		epoch INTEGER NOT NULL,
		owner_id TEXT NOT NULL,
		lease_token TEXT NOT NULL,
		acquired_at INTEGER NOT NULL,
		lease_until INTEGER NOT NULL
	)`,
	`CREATE TABLE journal_events (
		stream_id TEXT NOT NULL,
		sequence INTEGER NOT NULL,
		event_id TEXT NOT NULL UNIQUE,
		stream_type TEXT NOT NULL,
		event_type TEXT NOT NULL,
		event_version INTEGER NOT NULL,
		plan_revision TEXT NOT NULL,
		service_version TEXT NOT NULL,
		correlation_id TEXT NOT NULL DEFAULT '',
		causation_id TEXT NOT NULL DEFAULT '',
		payload BLOB,
		metadata BLOB,
		occurred_at INTEGER NOT NULL,
		PRIMARY KEY(stream_id, sequence)
	)`,
	`CREATE TABLE snapshots (
		stream_id TEXT PRIMARY KEY,
		aggregate_type TEXT NOT NULL,
		owner_service TEXT NOT NULL,
		plan_revision TEXT NOT NULL,
		schema_version INTEGER NOT NULL,
		last_sequence INTEGER NOT NULL,
		state BLOB,
		checksum TEXT NOT NULL,
		created_at INTEGER NOT NULL
	)`,
	`CREATE TABLE inbox (
		ordering_id INTEGER PRIMARY KEY AUTOINCREMENT,
		inbox_id TEXT NOT NULL UNIQUE,
		mailbox_id TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		message_id TEXT NOT NULL,
		message BLOB NOT NULL,
		stream_id TEXT NOT NULL DEFAULT '',
		stream_sequence INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL,
		attempt INTEGER NOT NULL DEFAULT 0,
		available_at INTEGER NOT NULL,
		lease_owner TEXT NOT NULL DEFAULT '',
		lease_token TEXT NOT NULL DEFAULT '',
		lease_until INTEGER NOT NULL DEFAULT 0,
		received_at INTEGER NOT NULL,
		acked_at INTEGER NOT NULL DEFAULT 0,
		last_error TEXT NOT NULL DEFAULT '',
		UNIQUE(mailbox_id, message_id)
	)`,
	`CREATE INDEX inbox_claim_idx ON inbox(mailbox_id, status, available_at, lease_until, ordering_id)`,
	`CREATE INDEX inbox_stream_idx ON inbox(mailbox_id, stream_id, stream_sequence)`,
	`CREATE TABLE outbox (
		ordering_id INTEGER PRIMARY KEY AUTOINCREMENT,
		outbox_id TEXT NOT NULL UNIQUE,
		runtime_id TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		message_id TEXT NOT NULL,
		message BLOB NOT NULL,
		status TEXT NOT NULL,
		attempt INTEGER NOT NULL DEFAULT 0,
		available_at INTEGER NOT NULL,
		lease_owner TEXT NOT NULL DEFAULT '',
		lease_token TEXT NOT NULL DEFAULT '',
		lease_until INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL,
		delivered_at INTEGER NOT NULL DEFAULT 0,
		last_error TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE INDEX outbox_claim_idx ON outbox(runtime_id, status, available_at, lease_until, ordering_id)`,
	`CREATE TABLE effects (
		ordering_id INTEGER PRIMARY KEY AUTOINCREMENT,
		effect_id TEXT NOT NULL UNIQUE,
		runtime_id TEXT NOT NULL,
		plan_revision TEXT NOT NULL,
		instance_id TEXT NOT NULL,
		source_message_id TEXT NOT NULL,
		effect_type TEXT NOT NULL,
		version INTEGER NOT NULL,
		executor_ref TEXT NOT NULL,
		idempotency_key TEXT NOT NULL,
		status TEXT NOT NULL,
		attempt INTEGER NOT NULL DEFAULT 0,
		available_at INTEGER NOT NULL,
		payload BLOB,
		result BLOB,
		metadata BLOB,
		result_metadata BLOB,
		deadline INTEGER NOT NULL DEFAULT 0,
		last_error TEXT NOT NULL DEFAULT '',
		planned_at INTEGER NOT NULL,
		started_at INTEGER NOT NULL DEFAULT 0,
		completed_at INTEGER NOT NULL DEFAULT 0,
		lease_owner TEXT NOT NULL DEFAULT '',
		lease_token TEXT NOT NULL DEFAULT '',
		lease_until INTEGER NOT NULL DEFAULT 0
	)`,
	`CREATE INDEX effects_claim_idx ON effects(runtime_id, status, available_at, lease_until, ordering_id)`,
	`CREATE TABLE runtime_plans (
		runtime_id TEXT NOT NULL,
		plan_revision TEXT NOT NULL,
		plan_hash TEXT NOT NULL,
		manifest BLOB NOT NULL,
		created_at INTEGER NOT NULL,
		PRIMARY KEY(runtime_id, plan_revision)
	)`,
	`CREATE TABLE message_sequences (
		scope TEXT NOT NULL,
		message_id TEXT NOT NULL,
		runtime_id TEXT NOT NULL,
		stream_id TEXT NOT NULL,
		sequence INTEGER NOT NULL,
		PRIMARY KEY(scope, message_id),
		UNIQUE(scope, runtime_id, stream_id, sequence)
	)`,
	`CREATE TABLE inbox_stream_heads (
		mailbox_id TEXT NOT NULL,
		stream_id TEXT NOT NULL,
		last_sequence INTEGER NOT NULL,
		PRIMARY KEY(mailbox_id, stream_id)
	)`,
}

var schemaMigrations = map[int][]string{
	2: {
		`ALTER TABLE effects ADD COLUMN metadata BLOB`,
		`ALTER TABLE effects ADD COLUMN result_metadata BLOB`,
		`ALTER TABLE effects ADD COLUMN deadline INTEGER NOT NULL DEFAULT 0`,
	},
	3: {
		`CREATE TABLE runtime_plans (
			runtime_id TEXT NOT NULL,
			plan_revision TEXT NOT NULL,
			plan_hash TEXT NOT NULL,
			manifest BLOB NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY(runtime_id, plan_revision)
		)`,
	},
	4: {
		`ALTER TABLE inbox ADD COLUMN stream_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE inbox ADD COLUMN stream_sequence INTEGER NOT NULL DEFAULT 0`,
		`CREATE INDEX inbox_stream_idx ON inbox(mailbox_id, stream_id, stream_sequence)`,
		`CREATE TABLE message_sequences (
			scope TEXT NOT NULL,
			message_id TEXT NOT NULL,
			runtime_id TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			sequence INTEGER NOT NULL,
			PRIMARY KEY(scope, message_id),
			UNIQUE(scope, runtime_id, stream_id, sequence)
		)`,
		`CREATE TABLE inbox_stream_heads (
			mailbox_id TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			last_sequence INTEGER NOT NULL,
			PRIMARY KEY(mailbox_id, stream_id)
		)`,
	},
	// Version 5 used to add a connection-specific table. The version remains a
	// tombstone so existing runtime databases stay readable after connection
	// persistence moved out of the generic RuntimeStorage.
	5: nil,
}

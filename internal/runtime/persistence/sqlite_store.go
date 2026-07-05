package persistence

import (
	"agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/statemachine"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

var _ TaskStateStore = (*sqliteTaskStateStore)(nil)
var _ SnapshotStore = (*sqliteSnapshotStore)(nil)
var _ RuntimeStore = (*sqliteRuntimeStore)(nil)
var _ EventStore = (*sqliteEventStore)(nil)

type SQLiteOptions struct {
	Path   string
	Driver string
}

type sqliteTaskStateStore struct {
	db  *sql.DB
	now func() time.Time
}

type sqliteSnapshotStore struct {
	db  *sql.DB
	now func() time.Time
}

type sqliteRuntimeStore struct {
	db  *sql.DB
	now func() time.Time
}

type sqliteEventStore struct {
	db  *sql.DB
	now func() time.Time
}

func NewSQLiteStore(ctx context.Context, options SQLiteOptions) (*LocalStore, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path := strings.TrimSpace(options.Path)
	if path == "" {
		return nil, fmt.Errorf("sqlite persistence: path is required")
	}
	driver, err := chooseSQLiteDriver(options.Driver)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(driver, path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite persistence %s: %w", path, err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite persistence %s: %w", path, err)
	}
	if err := initSQLiteSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	now := func() time.Time { return time.Now().UTC() }
	return &LocalStore{
		backend:   BackendSQLite,
		states:    &sqliteTaskStateStore{db: db, now: now},
		snapshots: &sqliteSnapshotStore{db: db, now: now},
		runtimes:  &sqliteRuntimeStore{db: db, now: now},
		events:    &sqliteEventStore{db: db, now: now},
		close:     db.Close,
	}, nil
}

func chooseSQLiteDriver(preferred string) (string, error) {
	preferred = strings.TrimSpace(preferred)
	drivers := sql.Drivers()
	if preferred != "" {
		for _, driver := range drivers {
			if driver == preferred {
				return preferred, nil
			}
		}
		return "", fmt.Errorf("sqlite driver %q is not registered", preferred)
	}
	for _, candidate := range []string{"sqlite", "sqlite3"} {
		for _, driver := range drivers {
			if driver == candidate {
				return candidate, nil
			}
		}
	}
	return "", fmt.Errorf("sqlite driver is not registered")
}

func initSQLiteSchema(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS task_states (
			task_id TEXT PRIMARY KEY,
			state_json BLOB NOT NULL,
			last_event_id TEXT,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS agent_snapshots (
			agent TEXT NOT NULL,
			task_id TEXT NOT NULL,
			snapshot_json BLOB NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(agent, task_id)
		)`,
		`CREATE TABLE IF NOT EXISTS task_runtimes (
			task_id TEXT PRIMARY KEY,
			runtime_json BLOB NOT NULL,
			agent TEXT,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL UNIQUE,
			task_id TEXT,
			topic TEXT,
			event_type TEXT,
			occurred_at TEXT NOT NULL,
			event_json BLOB NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_task_seq ON events(task_id, seq)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize sqlite persistence schema: %w", err)
		}
	}
	return nil
}

func (s *sqliteTaskStateStore) Load(ctx context.Context, taskID string) (statemachine.TaskState, bool, error) {
	if s == nil || s.db == nil {
		return statemachine.TaskState{}, false, fmt.Errorf("sqlite task state store is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return statemachine.TaskState{}, false, fmt.Errorf("task_id is required")
	}
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT state_json FROM task_states WHERE task_id = ?`, taskID).Scan(&raw)
	if err == sql.ErrNoRows {
		return statemachine.TaskState{}, false, nil
	}
	if err != nil {
		return statemachine.TaskState{}, false, err
	}
	var state statemachine.TaskState
	if err := json.Unmarshal(raw, &state); err != nil {
		return statemachine.TaskState{}, false, err
	}
	return state.Clone(), true, nil
}

func (s *sqliteTaskStateStore) Save(ctx context.Context, state statemachine.TaskState) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite task state store is nil")
	}
	if strings.TrimSpace(state.TaskID) == "" {
		return fmt.Errorf("task state task_id is required")
	}
	raw, err := json.Marshal(state.Clone())
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO task_states(task_id, state_json, last_event_id, updated_at)
		 VALUES(?, ?, ?, ?)
		 ON CONFLICT(task_id) DO UPDATE SET
			state_json = excluded.state_json,
			last_event_id = excluded.last_event_id,
			updated_at = excluded.updated_at`,
		state.TaskID, raw, state.LastEventID, currentTime(s.now).Format(time.RFC3339Nano),
	)
	return err
}

func (s *sqliteTaskStateStore) List(ctx context.Context) ([]statemachine.TaskState, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite task state store is nil")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT state_json FROM task_states ORDER BY task_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var states []statemachine.TaskState
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var state statemachine.TaskState
		if err := json.Unmarshal(raw, &state); err != nil {
			return nil, err
		}
		states = append(states, state.Clone())
	}
	return states, rows.Err()
}

func (s *sqliteSnapshotStore) Load(ctx context.Context, agentName string, taskID string) (agents.AgentSnapshot, bool, error) {
	if s == nil || s.db == nil {
		return agents.AgentSnapshot{}, false, fmt.Errorf("sqlite snapshot store is nil")
	}
	agentName = strings.TrimSpace(agentName)
	taskID = strings.TrimSpace(taskID)
	if agentName == "" {
		return agents.AgentSnapshot{}, false, fmt.Errorf("agent name is required")
	}
	if taskID == "" {
		return agents.AgentSnapshot{}, false, fmt.Errorf("task_id is required")
	}
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT snapshot_json FROM agent_snapshots WHERE agent = ? AND task_id = ?`, agentName, taskID).Scan(&raw)
	if err == sql.ErrNoRows {
		return agents.AgentSnapshot{}, false, nil
	}
	if err != nil {
		return agents.AgentSnapshot{}, false, err
	}
	var snapshot agents.AgentSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return agents.AgentSnapshot{}, false, err
	}
	return snapshot.Clone(), true, nil
}

func (s *sqliteSnapshotStore) Save(ctx context.Context, snapshot agents.AgentSnapshot) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite snapshot store is nil")
	}
	if strings.TrimSpace(snapshot.Agent) == "" {
		return fmt.Errorf("agent snapshot agent is required")
	}
	if strings.TrimSpace(snapshot.TaskID) == "" {
		return fmt.Errorf("agent snapshot task_id is required")
	}
	raw, err := json.Marshal(snapshot.Clone())
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO agent_snapshots(agent, task_id, snapshot_json, updated_at)
		 VALUES(?, ?, ?, ?)
		 ON CONFLICT(agent, task_id) DO UPDATE SET
			snapshot_json = excluded.snapshot_json,
			updated_at = excluded.updated_at`,
		snapshot.Agent, snapshot.TaskID, raw, currentTime(s.now).Format(time.RFC3339Nano),
	)
	return err
}

func (s *sqliteSnapshotStore) List(ctx context.Context) ([]agents.AgentSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite snapshot store is nil")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT snapshot_json FROM agent_snapshots ORDER BY agent, task_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []agents.AgentSnapshot
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var snapshot agents.AgentSnapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot.Clone())
	}
	return snapshots, rows.Err()
}

func (s *sqliteRuntimeStore) Load(ctx context.Context, taskID string) (RuntimeSnapshot, bool, error) {
	if s == nil || s.db == nil {
		return RuntimeSnapshot{}, false, fmt.Errorf("sqlite runtime store is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return RuntimeSnapshot{}, false, fmt.Errorf("task_id is required")
	}
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT runtime_json FROM task_runtimes WHERE task_id = ?`, taskID).Scan(&raw)
	if err == sql.ErrNoRows {
		return RuntimeSnapshot{}, false, nil
	}
	if err != nil {
		return RuntimeSnapshot{}, false, err
	}
	var snapshot RuntimeSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return RuntimeSnapshot{}, false, err
	}
	return snapshot.Clone(), true, nil
}

func (s *sqliteRuntimeStore) Save(ctx context.Context, snapshot RuntimeSnapshot) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite runtime store is nil")
	}
	if strings.TrimSpace(snapshot.TaskID) == "" {
		return fmt.Errorf("runtime snapshot task_id is required")
	}
	now := currentTime(s.now)
	snapshot = snapshot.Clone()
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = now
	}
	snapshot.UpdatedAt = now
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO task_runtimes(task_id, runtime_json, agent, updated_at)
		 VALUES(?, ?, ?, ?)
		 ON CONFLICT(task_id) DO UPDATE SET
			runtime_json = excluded.runtime_json,
			agent = excluded.agent,
			updated_at = excluded.updated_at`,
		snapshot.TaskID, raw, snapshot.Agent, now.Format(time.RFC3339Nano),
	)
	return err
}

func (s *sqliteRuntimeStore) Delete(ctx context.Context, taskID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite runtime store is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("task_id is required")
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM task_runtimes WHERE task_id = ?`, taskID)
	return err
}

func (s *sqliteRuntimeStore) List(ctx context.Context) ([]RuntimeSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite runtime store is nil")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT runtime_json FROM task_runtimes ORDER BY task_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []RuntimeSnapshot
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var snapshot RuntimeSnapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot.Clone())
	}
	return snapshots, rows.Err()
}

func (s *sqliteEventStore) Append(ctx context.Context, event eventbus.Event) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("sqlite event store is nil")
	}
	if strings.TrimSpace(event.ID) == "" {
		return false, fmt.Errorf("event id is required")
	}
	raw, err := json.Marshal(event.Clone())
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO events(event_id, task_id, topic, event_type, occurred_at, event_json)
		 VALUES(?, ?, ?, ?, ?, ?)`,
		event.ID, event.TaskID, event.Topic, string(event.Type), event.OccurredAt.UTC().Format(time.RFC3339Nano), raw,
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return true, nil
	}
	return affected > 0, nil
}

func (s *sqliteEventStore) List(ctx context.Context, taskID string) ([]eventbus.Event, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sqlite event store is nil")
	}
	query := `SELECT event_json FROM events`
	args := []any{}
	if strings.TrimSpace(taskID) != "" {
		query += ` WHERE task_id = ?`
		args = append(args, taskID)
	}
	query += ` ORDER BY seq`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []eventbus.Event
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var event eventbus.Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		events = append(events, event.Clone())
	}
	return events, rows.Err()
}

func (s *sqliteEventStore) Last(ctx context.Context, taskID string) (eventbus.Event, bool, error) {
	if s == nil || s.db == nil {
		return eventbus.Event{}, false, fmt.Errorf("sqlite event store is nil")
	}
	query := `SELECT event_json FROM events`
	args := []any{}
	if strings.TrimSpace(taskID) != "" {
		query += ` WHERE task_id = ?`
		args = append(args, taskID)
	}
	query += ` ORDER BY seq DESC LIMIT 1`
	var raw []byte
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&raw)
	if err == sql.ErrNoRows {
		return eventbus.Event{}, false, nil
	}
	if err != nil {
		return eventbus.Event{}, false, err
	}
	var event eventbus.Event
	if err := json.Unmarshal(raw, &event); err != nil {
		return eventbus.Event{}, false, err
	}
	return event.Clone(), true, nil
}

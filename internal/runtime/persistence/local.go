package persistence

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	BackendSQLite = "sqlite"
	BackendFile   = "file"
)

type OpenOptions struct {
	Path          string
	FileDir       string
	SQLiteDriver  string
	DisableSQLite bool
}

func Open(ctx context.Context, options OpenOptions) (*LocalStore, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(options.Path) == "" && strings.TrimSpace(options.FileDir) == "" {
		return nil, fmt.Errorf("persistence: sqlite path or file fallback directory is required")
	}

	var sqliteErr error
	if !options.DisableSQLite && strings.TrimSpace(options.Path) != "" {
		store, err := NewSQLiteStore(ctx, SQLiteOptions{
			Path:   options.Path,
			Driver: options.SQLiteDriver,
		})
		if err == nil {
			return store, nil
		}
		sqliteErr = err
	}

	fileDir := strings.TrimSpace(options.FileDir)
	if fileDir == "" {
		fileDir = options.Path + ".files"
	}
	fileDir = filepath.Clean(fileDir)
	store, err := NewFileStore(fileDir)
	if err != nil {
		if sqliteErr != nil {
			return nil, fmt.Errorf("open sqlite store failed: %v; open file fallback failed: %w", sqliteErr, err)
		}
		return nil, err
	}
	return store, nil
}

type LocalStore struct {
	backend   string
	states    TaskStateStore
	snapshots SnapshotStore
	runtimes  RuntimeStore
	events    EventStore
	close     func() error
}

func (s *LocalStore) TaskStates() TaskStateStore {
	if s == nil {
		return nil
	}
	return s.states
}

func (s *LocalStore) AgentSnapshots() SnapshotStore {
	if s == nil {
		return nil
	}
	return s.snapshots
}

func (s *LocalStore) Runtimes() RuntimeStore {
	if s == nil {
		return nil
	}
	return s.runtimes
}

func (s *LocalStore) Events() EventStore {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *LocalStore) Backend() string {
	if s == nil {
		return ""
	}
	return s.backend
}

func (s *LocalStore) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	return s.close()
}

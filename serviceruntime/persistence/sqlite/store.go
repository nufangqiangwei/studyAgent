package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 5

var _ persistence.RuntimeStorage = (*Store)(nil)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type Options struct {
	Clock       contract.Clock
	BusyTimeout time.Duration
}

type Store struct {
	db    *sql.DB
	clock contract.Clock

	closeOnce sync.Once
	closeErr  error
}

// Open opens or creates a durable service-runtime database at path.
func Open(ctx context.Context, path string, options Options) (*Store, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("sqlite runtime store path is required")
	}
	if options.Clock == nil {
		options.Clock = systemClock{}
	}
	if options.BusyTimeout <= 0 {
		options.BusyTimeout = 5 * time.Second
	}
	dsn, err := dataSource(path, options.BusyTimeout)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite runtime store: %w", err)
	}
	// A single local writer connection keeps transaction and PRAGMA semantics
	// deterministic. Multiple Store processes still coordinate through SQLite.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite runtime store: %w", err)
	}
	store := &Store{db: db, clock: options.Clock}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func dataSource(path string, busyTimeout time.Duration) (string, error) {
	var source string
	if path == ":memory:" {
		source = "file:serviceruntime?mode=memory&cache=shared"
	} else {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("resolve sqlite runtime store path: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			return "", fmt.Errorf("create sqlite runtime store directory: %w", err)
		}
		normalized := filepath.ToSlash(absolute)
		if volume := filepath.VolumeName(absolute); volume != "" && !strings.HasPrefix(normalized, "/") {
			normalized = "/" + normalized
		}
		source = (&url.URL{Scheme: "file", Path: normalized}).String()
	}
	separator := "?"
	if strings.Contains(source, "?") {
		separator = "&"
	}
	parameters := url.Values{}
	parameters.Set("_txlock", "immediate")
	parameters.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds()))
	parameters.Add("_pragma", "foreign_keys(1)")
	parameters.Add("_pragma", "journal_mode(WAL)")
	parameters.Add("_pragma", "synchronous(FULL)")
	return source + separator + parameters.Encode(), nil
}

func (s *Store) Journal() persistence.JournalStore           { return s }
func (s *Store) Snapshots() persistence.SnapshotStore        { return s }
func (s *Store) Inbox() persistence.InboxStore               { return &inboxStore{owner: s} }
func (s *Store) Outbox() persistence.OutboxStore             { return &outboxStore{owner: s} }
func (s *Store) Effects() persistence.EffectStore            { return &effectStore{owner: s} }
func (s *Store) Instances() instance.Store                   { return s }
func (s *Store) Leases() instance.ActivationLeaseStore       { return s }
func (s *Store) Committer() persistence.MessageCommitStore   { return s }
func (s *Store) Plans() persistence.PlanStore                { return &planStore{owner: s} }
func (s *Store) Sequences() persistence.MessageSequenceStore { return &sequenceStore{owner: s} }

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	s.closeOnce.Do(func() { s.closeErr = s.db.Close() })
	return s.closeErr
}

func (s *Store) now() time.Time { return s.clock.Now().UTC() }

func newToken(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create %s lease token: %w", prefix, err)
	}
	return prefix + "-" + hex.EncodeToString(value[:]), nil
}

func encodeJSON(value interface{}) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode sqlite runtime value: %w", err)
	}
	return data, nil
}

func decodeJSON(data []byte, target interface{}) error {
	if len(data) == 0 {
		data = []byte("null")
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode sqlite runtime value: %w", err)
	}
	return nil
}

func timeValue(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UTC().UnixNano()
}

func timeFromValue(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

func timePointer(value int64) *time.Time {
	if value == 0 {
		return nil
	}
	result := timeFromValue(value)
	return &result
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func rowsChanged(result sql.Result, lost error) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return lost
	}
	return nil
}

func rollback(tx *sql.Tx) { _ = tx.Rollback() }

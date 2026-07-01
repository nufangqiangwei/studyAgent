package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	runtimeevent "agent/internal/event"
)

const fileStoreSchemaVersion = 1

var _ StateStore = (*FileStateStore)(nil)
var _ EventStore = (*FileEventStore)(nil)
var _ EffectStore = (*FileEffectStore)(nil)
var _ EventInboxStore = (*FileEventInbox)(nil)

type FileStore struct {
	States  *FileStateStore
	Events  *FileEventStore
	Effects *FileEffectStore
	Inbox   *FileEventInbox
}

type FileStateStore struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type FileEventStore struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type FileEffectStore struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type FileEventInbox struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type fileStateRecord struct {
	SchemaVersion int       `json:"schema_version"`
	WrittenAt     time.Time `json:"written_at"`
	State         RunState  `json:"state"`
}

type fileEventRecord struct {
	SchemaVersion int                `json:"schema_version"`
	WrittenAt     time.Time          `json:"written_at"`
	Event         runtimeevent.Event `json:"event"`
}

type fileEffectRecord struct {
	SchemaVersion int          `json:"schema_version"`
	WrittenAt     time.Time    `json:"written_at"`
	Effect        StoredEffect `json:"effect"`
}

type fileInboxRecord struct {
	SchemaVersion int         `json:"schema_version"`
	WrittenAt     time.Time   `json:"written_at"`
	Event         StoredEvent `json:"event"`
}

func NewFileStore(rootDir string) (*FileStore, error) {
	rootDir, err := prepareFileStoreDir(rootDir)
	if err != nil {
		return nil, err
	}
	return &FileStore{
		States:  newFileStateStore(filepath.Join(rootDir, "states.jsonl")),
		Events:  newFileEventStore(filepath.Join(rootDir, "events.jsonl")),
		Effects: newFileEffectStore(filepath.Join(rootDir, "effects.jsonl")),
		Inbox:   newFileEventInbox(filepath.Join(rootDir, "event_inbox.jsonl")),
	}, nil
}

func NewFileStateStore(rootDir string) (*FileStateStore, error) {
	rootDir, err := prepareFileStoreDir(rootDir)
	if err != nil {
		return nil, err
	}
	return newFileStateStore(filepath.Join(rootDir, "states.jsonl")), nil
}

func NewFileEventStore(rootDir string) (*FileEventStore, error) {
	rootDir, err := prepareFileStoreDir(rootDir)
	if err != nil {
		return nil, err
	}
	return newFileEventStore(filepath.Join(rootDir, "events.jsonl")), nil
}

func NewFileEffectStore(rootDir string) (*FileEffectStore, error) {
	rootDir, err := prepareFileStoreDir(rootDir)
	if err != nil {
		return nil, err
	}
	return newFileEffectStore(filepath.Join(rootDir, "effects.jsonl")), nil
}

func NewFileEventInbox(rootDir string) (*FileEventInbox, error) {
	rootDir, err := prepareFileStoreDir(rootDir)
	if err != nil {
		return nil, err
	}
	return newFileEventInbox(filepath.Join(rootDir, "event_inbox.jsonl")), nil
}

func newFileStateStore(path string) *FileStateStore {
	return &FileStateStore{path: path, now: time.Now}
}

func newFileEventStore(path string) *FileEventStore {
	return &FileEventStore{path: path, now: time.Now}
}

func newFileEffectStore(path string) *FileEffectStore {
	return &FileEffectStore{path: path, now: time.Now}
}

func newFileEventInbox(path string) *FileEventInbox {
	return &FileEventInbox{path: path, now: time.Now}
}

func prepareFileStoreDir(rootDir string) (string, error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return "", fmt.Errorf("state file store: directory is required")
	}
	if err := os.MkdirAll(rootDir, 0700); err != nil {
		return "", fmt.Errorf("create state store directory %s: %w", rootDir, err)
	}
	return rootDir, nil
}

func (s *FileStateStore) Load(ctx context.Context, runID string) (RunState, error) {
	if s == nil {
		return RunState{}, fmt.Errorf("state file store is nil")
	}
	if strings.TrimSpace(runID) == "" {
		return RunState{}, fmt.Errorf("run_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileStateRecord](ctx, s.path)
	if err != nil {
		return RunState{}, err
	}
	for i := len(records) - 1; i >= 0; i-- {
		st := records[i].State
		if st.RunID == runID {
			return cloneRunState(st), nil
		}
	}
	return RunState{}, fmt.Errorf("state not found: %s", runID)
}

func (s *FileStateStore) Save(ctx context.Context, st RunState) error {
	if s == nil {
		return fmt.Errorf("state file store is nil")
	}
	if st.RunID == "" {
		return fmt.Errorf("run_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return appendJSONLRecord(ctx, s.path, fileStateRecord{
		SchemaVersion: fileStoreSchemaVersion,
		WrittenAt:     currentFileStoreTime(s.now),
		State:         cloneRunState(st),
	})
}

func (s *FileEventStore) Append(ctx context.Context, event runtimeevent.Event) (bool, error) {
	if s == nil {
		return false, fmt.Errorf("event file store is nil")
	}
	if event.ID == "" {
		return false, fmt.Errorf("event id is required")
	}
	if event.RunID == "" {
		return false, fmt.Errorf("run_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileEventRecord](ctx, s.path)
	if err != nil {
		return false, err
	}
	for _, record := range records {
		if record.Event.ID == event.ID {
			return false, nil
		}
	}

	return true, appendJSONLRecord(ctx, s.path, fileEventRecord{
		SchemaVersion: fileStoreSchemaVersion,
		WrittenAt:     currentFileStoreTime(s.now),
		Event:         event.Clone(),
	})
}

func (s *FileEventStore) List(ctx context.Context, runID string) ([]runtimeevent.Event, error) {
	if s == nil {
		return nil, fmt.Errorf("event file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileEventRecord](ctx, s.path)
	if err != nil {
		return nil, err
	}
	events := make([]runtimeevent.Event, 0, len(records))
	for _, record := range records {
		if runID != "" && record.Event.RunID != runID {
			continue
		}
		events = append(events, record.Event.Clone())
	}
	return events, nil
}

func (s *FileEffectStore) Append(ctx context.Context, effect Effect) (StoredEffect, error) {
	if s == nil {
		return StoredEffect{}, fmt.Errorf("effect file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	effects, _, err := s.loadLocked(ctx)
	if err != nil {
		return StoredEffect{}, err
	}
	if existing, ok := effects[effect.ID]; ok {
		return existing.Clone(), nil
	}

	stored, err := normalizeStoredEffect(effect, currentFileStoreTime(s.now))
	if err != nil {
		return StoredEffect{}, err
	}
	if err := s.appendLocked(ctx, stored); err != nil {
		return StoredEffect{}, err
	}
	return stored.Clone(), nil
}

func (s *FileEffectStore) ListPending(ctx context.Context, runID string) ([]StoredEffect, error) {
	if s == nil {
		return nil, fmt.Errorf("effect file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	effects, order, err := s.loadLocked(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]StoredEffect, 0, len(order))
	for _, id := range order {
		stored, ok := effects[id]
		if !ok || !stored.Status.PendingWork() {
			continue
		}
		if runID != "" && stored.Effect.RunID != runID {
			continue
		}
		out = append(out, stored.Clone())
	}
	return out, nil
}

func (s *FileEffectStore) Claim(ctx context.Context, runID string) (StoredEffect, bool, error) {
	if s == nil {
		return StoredEffect{}, false, fmt.Errorf("effect file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	effects, order, err := s.loadLocked(ctx)
	if err != nil {
		return StoredEffect{}, false, err
	}
	for _, id := range order {
		stored, ok := effects[id]
		if !ok || !stored.Status.PendingWork() {
			continue
		}
		if runID != "" && stored.Effect.RunID != runID {
			continue
		}
		if stored.Status == EffectStatusPending {
			markEffectDispatched(&stored, currentFileStoreTime(s.now))
			if err := s.appendLocked(ctx, stored); err != nil {
				return StoredEffect{}, false, err
			}
		}
		return stored.Clone(), true, nil
	}
	return StoredEffect{}, false, nil
}

func (s *FileEffectStore) MarkDispatched(ctx context.Context, effectID string) error {
	if s == nil {
		return fmt.Errorf("effect file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored, err := s.loadOneLocked(ctx, effectID)
	if err != nil {
		return err
	}
	if stored.Status == EffectStatusCompleted || stored.Status == EffectStatusFailed {
		return nil
	}
	markEffectDispatched(&stored, currentFileStoreTime(s.now))
	return s.appendLocked(ctx, stored)
}

func (s *FileEffectStore) MarkCompleted(ctx context.Context, effectID string) error {
	if s == nil {
		return fmt.Errorf("effect file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored, err := s.loadOneLocked(ctx, effectID)
	if err != nil {
		return err
	}
	now := currentFileStoreTime(s.now)
	stored.Status = EffectStatusCompleted
	stored.Error = ""
	stored.UpdatedAt = now
	stored.CompletedAt = &now
	return s.appendLocked(ctx, stored)
}

func (s *FileEffectStore) MarkFailed(ctx context.Context, effectID string, cause error) error {
	if s == nil {
		return fmt.Errorf("effect file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored, err := s.loadOneLocked(ctx, effectID)
	if err != nil {
		return err
	}
	now := currentFileStoreTime(s.now)
	stored.Status = EffectStatusFailed
	stored.Error = ""
	if cause != nil {
		stored.Error = cause.Error()
	}
	stored.UpdatedAt = now
	stored.FailedAt = &now
	return s.appendLocked(ctx, stored)
}

func (s *FileEffectStore) loadOneLocked(ctx context.Context, effectID string) (StoredEffect, error) {
	effects, _, err := s.loadLocked(ctx)
	if err != nil {
		return StoredEffect{}, err
	}
	stored, ok := effects[effectID]
	if !ok {
		return StoredEffect{}, fmt.Errorf("effect not found: %s", effectID)
	}
	return stored.Clone(), nil
}

func (s *FileEffectStore) loadLocked(ctx context.Context) (map[string]StoredEffect, []string, error) {
	records, err := readJSONLRecords[fileEffectRecord](ctx, s.path)
	if err != nil {
		return nil, nil, err
	}
	effects := make(map[string]StoredEffect, len(records))
	order := make([]string, 0, len(records))
	for _, record := range records {
		stored := record.Effect.Clone()
		id := stored.Effect.ID
		if id == "" {
			continue
		}
		if _, exists := effects[id]; !exists {
			order = append(order, id)
		}
		effects[id] = stored
	}
	return effects, order, nil
}

func (s *FileEffectStore) appendLocked(ctx context.Context, stored StoredEffect) error {
	return appendJSONLRecord(ctx, s.path, fileEffectRecord{
		SchemaVersion: fileStoreSchemaVersion,
		WrittenAt:     currentFileStoreTime(s.now),
		Effect:        stored.Clone(),
	})
}

func (s *FileEventInbox) Append(ctx context.Context, event runtimeevent.Event) (StoredEvent, bool, error) {
	if s == nil {
		return StoredEvent{}, false, fmt.Errorf("event inbox file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	events, _, err := s.loadLocked(ctx)
	if err != nil {
		return StoredEvent{}, false, err
	}
	if existing, ok := events[event.ID]; ok {
		return existing.Clone(), false, nil
	}

	stored, err := normalizeStoredEvent(event, currentFileStoreTime(s.now))
	if err != nil {
		return StoredEvent{}, false, err
	}
	if err := s.appendLocked(ctx, stored); err != nil {
		return StoredEvent{}, false, err
	}
	return stored.Clone(), true, nil
}

func (s *FileEventInbox) ListPending(ctx context.Context, runID string) ([]StoredEvent, error) {
	if s == nil {
		return nil, fmt.Errorf("event inbox file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	events, order, err := s.loadLocked(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]StoredEvent, 0, len(order))
	for _, id := range order {
		stored, ok := events[id]
		if !ok || !stored.Status.Claimable() {
			continue
		}
		if runID != "" && stored.Event.RunID != runID {
			continue
		}
		out = append(out, stored.Clone())
	}
	return out, nil
}

func (s *FileEventInbox) Claim(ctx context.Context, runID string) (StoredEvent, bool, error) {
	if s == nil {
		return StoredEvent{}, false, fmt.Errorf("event inbox file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	events, order, err := s.loadLocked(ctx)
	if err != nil {
		return StoredEvent{}, false, err
	}
	for _, id := range order {
		stored, ok := events[id]
		if !ok || !stored.Status.Claimable() {
			continue
		}
		if runID != "" && stored.Event.RunID != runID {
			continue
		}
		if stored.Status == EventInboxStatusPending {
			markEventClaimed(&stored, currentFileStoreTime(s.now))
			if err := s.appendLocked(ctx, stored); err != nil {
				return StoredEvent{}, false, err
			}
		}
		return stored.Clone(), true, nil
	}
	return StoredEvent{}, false, nil
}

func (s *FileEventInbox) MarkProcessed(ctx context.Context, eventID string) error {
	if s == nil {
		return fmt.Errorf("event inbox file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored, err := s.loadOneLocked(ctx, eventID)
	if err != nil {
		return err
	}
	now := currentFileStoreTime(s.now)
	stored.Status = EventInboxStatusProcessed
	stored.Error = ""
	stored.UpdatedAt = now
	stored.ProcessedAt = &now
	return s.appendLocked(ctx, stored)
}

func (s *FileEventInbox) MarkFailed(ctx context.Context, eventID string, cause error) error {
	if s == nil {
		return fmt.Errorf("event inbox file store is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored, err := s.loadOneLocked(ctx, eventID)
	if err != nil {
		return err
	}
	now := currentFileStoreTime(s.now)
	stored.Status = EventInboxStatusFailed
	stored.Error = ""
	if cause != nil {
		stored.Error = cause.Error()
	}
	stored.UpdatedAt = now
	stored.FailedAt = &now
	return s.appendLocked(ctx, stored)
}

func (s *FileEventInbox) loadOneLocked(ctx context.Context, eventID string) (StoredEvent, error) {
	events, _, err := s.loadLocked(ctx)
	if err != nil {
		return StoredEvent{}, err
	}
	stored, ok := events[eventID]
	if !ok {
		return StoredEvent{}, fmt.Errorf("event not found: %s", eventID)
	}
	return stored.Clone(), nil
}

func (s *FileEventInbox) loadLocked(ctx context.Context) (map[string]StoredEvent, []string, error) {
	records, err := readJSONLRecords[fileInboxRecord](ctx, s.path)
	if err != nil {
		return nil, nil, err
	}
	events := make(map[string]StoredEvent, len(records))
	order := make([]string, 0, len(records))
	for _, record := range records {
		stored := record.Event.Clone()
		id := stored.Event.ID
		if id == "" {
			continue
		}
		if _, exists := events[id]; !exists {
			order = append(order, id)
		}
		events[id] = stored
	}
	return events, order, nil
}

func (s *FileEventInbox) appendLocked(ctx context.Context, stored StoredEvent) error {
	return appendJSONLRecord(ctx, s.path, fileInboxRecord{
		SchemaVersion: fileStoreSchemaVersion,
		WrittenAt:     currentFileStoreTime(s.now),
		Event:         stored.Clone(),
	})
}

func currentFileStoreTime(now func() time.Time) time.Time {
	if now != nil {
		return now().UTC()
	}
	return time.Now().UTC()
}

func appendJSONLRecord(ctx context.Context, path string, record any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create state store directory for %s: %w", path, err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("open state store log %s: %w", path, err)
	}
	defer file.Close()

	if err := ensureTrailingNewline(file); err != nil {
		return fmt.Errorf("prepare state store log %s: %w", path, err)
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal state store record: %w", err)
	}
	data = append(data, '\n')
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write state store log %s: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync state store log %s: %w", path, err)
	}
	return nil
}

func ensureTrailingNewline(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	if _, err := file.Seek(-1, io.SeekEnd); err != nil {
		return err
	}
	var last [1]byte
	if _, err := file.Read(last[:]); err != nil {
		return err
	}
	if last[0] == '\n' {
		_, err = file.Seek(0, io.SeekEnd)
		return err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	_, err = file.Write([]byte{'\n'})
	return err
}

func readJSONLRecords[T any](ctx context.Context, path string) ([]T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open state store log %s: %w", path, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var records []T
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			line = bytes.TrimSpace(line)
			if len(line) > 0 {
				var record T
				if jsonErr := json.Unmarshal(line, &record); jsonErr == nil {
					records = append(records, record)
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read state store log %s: %w", path, err)
		}
	}
	return records, nil
}

func cloneRunState(st RunState) RunState {
	cloned := st
	if st.Waiting != nil {
		waiting := *st.Waiting
		cloned.Waiting = &waiting
	}
	if st.Error != nil {
		errState := *st.Error
		cloned.Error = &errState
	}
	if len(st.Extensions) > 0 {
		cloned.Extensions = make(map[string]json.RawMessage, len(st.Extensions))
		for key, value := range st.Extensions {
			cloned.Extensions[key] = append(json.RawMessage(nil), value...)
		}
	}
	return cloned
}

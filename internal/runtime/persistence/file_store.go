package persistence

import (
	"agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/statemachine"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var _ RuntimeStorage = (*LocalStore)(nil)
var _ TaskStateStore = (*fileTaskStateStore)(nil)
var _ SnapshotStore = (*fileSnapshotStore)(nil)
var _ RuntimeStore = (*fileRuntimeStore)(nil)
var _ EventStore = (*fileEventStore)(nil)

type fileTaskStateStore struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type fileSnapshotStore struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type fileRuntimeStore struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type fileEventStore struct {
	mu   sync.Mutex
	path string
	now  func() time.Time
}

type fileTaskStateRecord struct {
	SchemaVersion int                    `json:"schema_version"`
	WrittenAt     time.Time              `json:"written_at"`
	State         statemachine.TaskState `json:"state"`
}

type fileSnapshotRecord struct {
	SchemaVersion int                  `json:"schema_version"`
	WrittenAt     time.Time            `json:"written_at"`
	Snapshot      agents.AgentSnapshot `json:"snapshot"`
}

type fileRuntimeRecord struct {
	SchemaVersion int             `json:"schema_version"`
	WrittenAt     time.Time       `json:"written_at"`
	Deleted       bool            `json:"deleted,omitempty"`
	Runtime       RuntimeSnapshot `json:"runtime"`
}

type fileEventRecord struct {
	SchemaVersion int            `json:"schema_version"`
	WrittenAt     time.Time      `json:"written_at"`
	Event         eventbus.Event `json:"event"`
}

func NewFileStore(rootDir string) (*LocalStore, error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return nil, fmt.Errorf("file persistence: directory is required")
	}
	if err := os.MkdirAll(rootDir, 0700); err != nil {
		return nil, fmt.Errorf("create file persistence directory %s: %w", rootDir, err)
	}
	now := func() time.Time { return time.Now().UTC() }
	return &LocalStore{
		backend: BackendFile,
		states: &fileTaskStateStore{
			path: filepath.Join(rootDir, "task_states.jsonl"),
			now:  now,
		},
		snapshots: &fileSnapshotStore{
			path: filepath.Join(rootDir, "agent_snapshots.jsonl"),
			now:  now,
		},
		runtimes: &fileRuntimeStore{
			path: filepath.Join(rootDir, "task_runtimes.jsonl"),
			now:  now,
		},
		events: &fileEventStore{
			path: filepath.Join(rootDir, "events.jsonl"),
			now:  now,
		},
	}, nil
}

func (s *fileTaskStateStore) Load(ctx context.Context, taskID string) (statemachine.TaskState, bool, error) {
	if s == nil {
		return statemachine.TaskState{}, false, fmt.Errorf("file task state store is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return statemachine.TaskState{}, false, fmt.Errorf("task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileTaskStateRecord](ctx, s.path)
	if err != nil {
		return statemachine.TaskState{}, false, err
	}
	for i := len(records) - 1; i >= 0; i-- {
		state := records[i].State
		if state.TaskID == taskID {
			return state.Clone(), true, nil
		}
	}
	return statemachine.TaskState{}, false, nil
}

func (s *fileTaskStateStore) Save(ctx context.Context, state statemachine.TaskState) error {
	if s == nil {
		return fmt.Errorf("file task state store is nil")
	}
	if strings.TrimSpace(state.TaskID) == "" {
		return fmt.Errorf("task state task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return appendJSONLRecord(ctx, s.path, fileTaskStateRecord{
		SchemaVersion: schemaVersion,
		WrittenAt:     currentTime(s.now),
		State:         state.Clone(),
	})
}

func (s *fileTaskStateStore) List(ctx context.Context) ([]statemachine.TaskState, error) {
	if s == nil {
		return nil, fmt.Errorf("file task state store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileTaskStateRecord](ctx, s.path)
	if err != nil {
		return nil, err
	}
	latest := make(map[string]statemachine.TaskState)
	for _, record := range records {
		if record.State.TaskID != "" {
			latest[record.State.TaskID] = record.State.Clone()
		}
	}
	taskIDs := sortedKeys(latest)
	out := make([]statemachine.TaskState, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		out = append(out, latest[taskID].Clone())
	}
	return out, nil
}

func (s *fileSnapshotStore) Load(ctx context.Context, agentName string, taskID string) (agents.AgentSnapshot, bool, error) {
	if s == nil {
		return agents.AgentSnapshot{}, false, fmt.Errorf("file snapshot store is nil")
	}
	agentName = strings.TrimSpace(agentName)
	taskID = strings.TrimSpace(taskID)
	if agentName == "" {
		return agents.AgentSnapshot{}, false, fmt.Errorf("agent name is required")
	}
	if taskID == "" {
		return agents.AgentSnapshot{}, false, fmt.Errorf("task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileSnapshotRecord](ctx, s.path)
	if err != nil {
		return agents.AgentSnapshot{}, false, err
	}
	for i := len(records) - 1; i >= 0; i-- {
		snapshot := records[i].Snapshot
		if snapshot.Agent == agentName && snapshot.TaskID == taskID {
			return snapshot.Clone(), true, nil
		}
	}
	return agents.AgentSnapshot{}, false, nil
}

func (s *fileSnapshotStore) Save(ctx context.Context, snapshot agents.AgentSnapshot) error {
	if s == nil {
		return fmt.Errorf("file snapshot store is nil")
	}
	if strings.TrimSpace(snapshot.Agent) == "" {
		return fmt.Errorf("agent snapshot agent is required")
	}
	if strings.TrimSpace(snapshot.TaskID) == "" {
		return fmt.Errorf("agent snapshot task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return appendJSONLRecord(ctx, s.path, fileSnapshotRecord{
		SchemaVersion: schemaVersion,
		WrittenAt:     currentTime(s.now),
		Snapshot:      snapshot.Clone(),
	})
}

func (s *fileSnapshotStore) List(ctx context.Context) ([]agents.AgentSnapshot, error) {
	if s == nil {
		return nil, fmt.Errorf("file snapshot store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileSnapshotRecord](ctx, s.path)
	if err != nil {
		return nil, err
	}
	latest := make(map[string]agents.AgentSnapshot)
	for _, record := range records {
		snapshot := record.Snapshot
		if snapshot.Agent == "" || snapshot.TaskID == "" {
			continue
		}
		latest[snapshot.Agent+"\x00"+snapshot.TaskID] = snapshot.Clone()
	}
	keys := sortedKeys(latest)
	out := make([]agents.AgentSnapshot, 0, len(keys))
	for _, key := range keys {
		out = append(out, latest[key].Clone())
	}
	return out, nil
}

func (s *fileRuntimeStore) Load(ctx context.Context, taskID string) (RuntimeSnapshot, bool, error) {
	if s == nil {
		return RuntimeSnapshot{}, false, fmt.Errorf("file runtime store is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return RuntimeSnapshot{}, false, fmt.Errorf("task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileRuntimeRecord](ctx, s.path)
	if err != nil {
		return RuntimeSnapshot{}, false, err
	}
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Runtime.TaskID != taskID {
			continue
		}
		if record.Deleted {
			return RuntimeSnapshot{}, false, nil
		}
		return record.Runtime.Clone(), true, nil
	}
	return RuntimeSnapshot{}, false, nil
}

func (s *fileRuntimeStore) Save(ctx context.Context, snapshot RuntimeSnapshot) error {
	if s == nil {
		return fmt.Errorf("file runtime store is nil")
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

	s.mu.Lock()
	defer s.mu.Unlock()

	return appendJSONLRecord(ctx, s.path, fileRuntimeRecord{
		SchemaVersion: schemaVersion,
		WrittenAt:     now,
		Runtime:       snapshot,
	})
}

func (s *fileRuntimeStore) Delete(ctx context.Context, taskID string) error {
	if s == nil {
		return fmt.Errorf("file runtime store is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("task_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := currentTime(s.now)
	return appendJSONLRecord(ctx, s.path, fileRuntimeRecord{
		SchemaVersion: schemaVersion,
		WrittenAt:     now,
		Deleted:       true,
		Runtime: RuntimeSnapshot{
			TaskID:    taskID,
			UpdatedAt: now,
		},
	})
}

func (s *fileRuntimeStore) List(ctx context.Context) ([]RuntimeSnapshot, error) {
	if s == nil {
		return nil, fmt.Errorf("file runtime store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileRuntimeRecord](ctx, s.path)
	if err != nil {
		return nil, err
	}
	latest := make(map[string]RuntimeSnapshot)
	deleted := make(map[string]bool)
	for _, record := range records {
		taskID := strings.TrimSpace(record.Runtime.TaskID)
		if taskID == "" {
			continue
		}
		if record.Deleted {
			delete(latest, taskID)
			deleted[taskID] = true
			continue
		}
		if deleted[taskID] {
			delete(deleted, taskID)
		}
		latest[taskID] = record.Runtime.Clone()
	}
	taskIDs := sortedKeys(latest)
	out := make([]RuntimeSnapshot, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		out = append(out, latest[taskID].Clone())
	}
	return out, nil
}

func (s *fileEventStore) Append(ctx context.Context, event eventbus.Event) (bool, error) {
	if s == nil {
		return false, fmt.Errorf("file event store is nil")
	}
	if strings.TrimSpace(event.ID) == "" {
		return false, fmt.Errorf("event id is required")
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
		SchemaVersion: schemaVersion,
		WrittenAt:     currentTime(s.now),
		Event:         event.Clone(),
	})
}

func (s *fileEventStore) List(ctx context.Context, taskID string) ([]eventbus.Event, error) {
	if s == nil {
		return nil, fmt.Errorf("file event store is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileEventRecord](ctx, s.path)
	if err != nil {
		return nil, err
	}
	events := make([]eventbus.Event, 0, len(records))
	for _, record := range records {
		if taskID != "" && record.Event.TaskID != taskID {
			continue
		}
		events = append(events, record.Event.Clone())
	}
	return events, nil
}

func (s *fileEventStore) Last(ctx context.Context, taskID string) (eventbus.Event, bool, error) {
	if s == nil {
		return eventbus.Event{}, false, fmt.Errorf("file event store is nil")
	}
	taskID = strings.TrimSpace(taskID)
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := readJSONLRecords[fileEventRecord](ctx, s.path)
	if err != nil {
		return eventbus.Event{}, false, err
	}
	for i := len(records) - 1; i >= 0; i-- {
		event := records[i].Event
		if taskID == "" || event.TaskID == taskID {
			return event.Clone(), true, nil
		}
	}
	return eventbus.Event{}, false, nil
}

func appendJSONLRecord(ctx context.Context, path string, record any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create persistence directory for %s: %w", path, err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("open persistence log %s: %w", path, err)
	}
	defer file.Close()

	if err := ensureTrailingNewline(file); err != nil {
		return fmt.Errorf("prepare persistence log %s: %w", path, err)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal persistence record: %w", err)
	}
	data = append(data, '\n')
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write persistence log %s: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync persistence log %s: %w", path, err)
	}
	return nil
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
		return nil, fmt.Errorf("open persistence log %s: %w", path, err)
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
			return nil, fmt.Errorf("read persistence log %s: %w", path, err)
		}
	}
	return records, nil
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

func currentTime(now func() time.Time) time.Time {
	if now == nil {
		return time.Now().UTC()
	}
	return now().UTC()
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

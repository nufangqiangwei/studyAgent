package session

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"agent/internal/llm"
)

const (
	defaultLockRetry = 20 * time.Millisecond
	defaultStaleAge  = 10 * time.Minute
	manifestName     = "manifest.json"
	agentsDirName    = "agents"
)

const (
	RecordKindMessage      = "message"
	RecordKindEvent        = "event"
	RecordKindUsageSummary = "usage_summary"
)

const (
	EventTypeLLMRequest     = "llm_request"
	EventTypeLLMResponse    = "llm_response"
	EventTypeToolCall       = "tool_call"
	EventTypeToolResult     = "tool_result"
	EventTypePolicyDecision = "policy_decision"
	EventTypeSummary        = "summary"
)

type Recorder interface {
	Save(ctx context.Context, record Record) error
}

type Loader interface {
	Load(ctx context.Context) ([]Record, error)
}

type Record struct {
	Kind         string       `json:"kind"`
	Timestamp    time.Time    `json:"timestamp"`
	SessionID    string       `json:"session_id"`
	AgentID      string       `json:"agent_id"`
	TurnID       string       `json:"turn_id,omitempty"`
	AgentName    string       `json:"agent_name,omitempty"`
	Task         string       `json:"task,omitempty"`
	WorkDir      string       `json:"work_dir,omitempty"`
	StepIndex    int          `json:"step_index,omitempty"`
	Message      *llm.Message `json:"message,omitempty"`
	Event        *Event       `json:"event,omitempty"`
	Usage        *llm.Usage   `json:"usage,omitempty"`
	UsageSummary *llm.Usage   `json:"usage_summary,omitempty"`
	LLMCalls     int          `json:"llm_calls,omitempty"`
}

type Event struct {
	ID        string          `json:"id"`
	Time      time.Time       `json:"time"`
	Type      string          `json:"type"`
	AgentName string          `json:"agent_name,omitempty"`
	Step      int             `json:"step,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type EventScope struct {
	TurnID    string
	Task      string
	WorkDir   string
	AgentName string
	Step      int
}

type Manifest struct {
	ID        string           `json:"id"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
	Layout    Layout           `json:"layout"`
	Agents    []AgentFileEntry `json:"agents"`
}

type Layout struct {
	ManifestFile string `json:"manifest_file"`
	AgentsDir    string `json:"agents_dir"`
	AgentFiles   string `json:"agent_files"`
	LockFiles    string `json:"lock_files"`
}

type AgentFileEntry struct {
	ID        string    `json:"id"`
	Path      string    `json:"path"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type FileStore struct {
	mu               sync.Mutex
	id               string
	agentID          string
	rootDir          string
	sessionDir       string
	agentsDir        string
	path             string
	lockPath         string
	manifestPath     string
	manifestLockPath string
	lockRetry        time.Duration
	staleAfter       time.Duration
	now              func() time.Time
}

func DefaultDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(homeDir, ".testAgent", "sessions"), nil
}

func NewFileStore(rootDir string) (*FileStore, error) {
	sessionID, err := NewID()
	if err != nil {
		return nil, err
	}
	agentID, err := NewID()
	if err != nil {
		return nil, err
	}
	return OpenAgentFileStore(rootDir, sessionID, agentID)
}

func OpenFileStore(rootDir, sessionID string) (*FileStore, error) {
	agentID, err := NewID()
	if err != nil {
		return nil, err
	}
	return OpenAgentFileStore(rootDir, sessionID, agentID)
}

func OpenAgentFileStore(rootDir, sessionID, agentID string) (*FileStore, error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return nil, fmt.Errorf("session: directory is required")
	}
	sessionID = strings.TrimSpace(sessionID)
	if err := validateID("session id", sessionID); err != nil {
		return nil, err
	}
	agentID = strings.TrimSpace(agentID)
	if err := validateID("agent id", agentID); err != nil {
		return nil, err
	}

	sessionDir := filepath.Join(rootDir, sessionID)
	agentsDir := filepath.Join(sessionDir, agentsDirName)
	if err := os.MkdirAll(agentsDir, 0700); err != nil {
		return nil, fmt.Errorf("create session directory %s: %w", agentsDir, err)
	}

	path := filepath.Join(agentsDir, agentID+".jsonl")
	return &FileStore{
		id:               sessionID,
		agentID:          agentID,
		rootDir:          rootDir,
		sessionDir:       sessionDir,
		agentsDir:        agentsDir,
		path:             path,
		lockPath:         path + ".lock",
		manifestPath:     filepath.Join(sessionDir, manifestName),
		manifestLockPath: filepath.Join(sessionDir, manifestName+".lock"),
		lockRetry:        defaultLockRetry,
		staleAfter:       defaultStaleAge,
		now:              time.Now,
	}, nil
}

func NewID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	encoded := make([]byte, 32)
	hex.Encode(encoded, raw[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		encoded[0:8],
		encoded[8:12],
		encoded[12:16],
		encoded[16:20],
		encoded[20:32],
	), nil
}

func SaveEvent(ctx context.Context, recorder Recorder, scope EventScope, eventType string, payload any) error {
	if recorder == nil {
		return nil
	}
	rawPayload, err := MarshalEventPayload(payload)
	if err != nil {
		return fmt.Errorf("marshal session event payload %s: %w", eventType, err)
	}
	return recorder.Save(ctx, Record{
		Kind:      RecordKindEvent,
		TurnID:    scope.TurnID,
		Task:      scope.Task,
		WorkDir:   scope.WorkDir,
		AgentName: scope.AgentName,
		StepIndex: scope.Step,
		Event: &Event{
			Type:      eventType,
			AgentName: scope.AgentName,
			Step:      scope.Step,
			Payload:   rawPayload,
		},
	})
}

func MarshalEventPayload(payload any) (json.RawMessage, error) {
	if payload == nil {
		return nil, nil
	}
	switch value := payload.(type) {
	case json.RawMessage:
		return append(json.RawMessage(nil), value...), nil
	case []byte:
		return append(json.RawMessage(nil), value...), nil
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(data), nil
	}
}

func (s *FileStore) ID() string {
	if s == nil {
		return ""
	}
	return s.id
}

func (s *FileStore) AgentID() string {
	if s == nil {
		return ""
	}
	return s.agentID
}

func (s *FileStore) SessionDir() string {
	if s == nil {
		return ""
	}
	return s.sessionDir
}

func (s *FileStore) ManifestPath() string {
	if s == nil {
		return ""
	}
	return s.manifestPath
}

func (s *FileStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *FileStore) Save(ctx context.Context, record Record) error {
	if s == nil {
		return fmt.Errorf("session: file store is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := os.MkdirAll(s.agentsDir, 0700); err != nil {
		return fmt.Errorf("create session directory %s: %w", s.agentsDir, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	if record.Kind == "" {
		if record.Event != nil {
			record.Kind = RecordKindEvent
		} else {
			record.Kind = RecordKindMessage
		}
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = now
	}
	record.SessionID = s.id
	record.AgentID = s.agentID
	record.Message = cloneMessagePtr(record.Message)
	record.Event = cloneEventPtr(record.Event)
	record.Usage = cloneUsage(record.Usage)
	record.UsageSummary = cloneUsage(record.UsageSummary)
	if record.Event != nil {
		if record.Event.ID == "" {
			eventID, err := NewID()
			if err != nil {
				return err
			}
			record.Event.ID = eventID
		}
		if record.Event.Time.IsZero() {
			record.Event.Time = record.Timestamp
		}
		if record.Event.AgentName == "" {
			record.Event.AgentName = record.AgentName
		}
		if record.Event.Step == 0 {
			record.Event.Step = record.StepIndex
		}
	}

	if err := s.withPathLock(ctx, s.lockPath, func() error {
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("encode session record %s: %w", s.path, err)
		}
		data = append(data, '\n')
		file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("open session file %s: %w", s.path, err)
		}
		defer file.Close()
		if _, err := file.Write(data); err != nil {
			return fmt.Errorf("write session file %s: %w", s.path, err)
		}
		return nil
	}); err != nil {
		return err
	}

	return s.saveManifest(ctx, record.Timestamp)
}

func (s *FileStore) Load(ctx context.Context) ([]Record, error) {
	if s == nil {
		return nil, fmt.Errorf("session: file store is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var records []Record
	err := s.withPathLock(ctx, s.lockPath, func() error {
		file, err := os.Open(s.path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("read session file %s: %w", s.path, err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var record Record
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				return fmt.Errorf("parse session record %s: %w", s.path, err)
			}
			records = append(records, record)
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scan session file %s: %w", s.path, err)
		}
		return nil
	})
	return records, err
}

func (s *FileStore) LoadManifest(ctx context.Context) (Manifest, error) {
	if s == nil {
		return Manifest{}, fmt.Errorf("session: file store is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var manifest Manifest
	err := s.withPathLock(ctx, s.manifestLockPath, func() error {
		loaded, err := s.loadManifestUnlocked()
		if err != nil {
			return err
		}
		manifest = loaded
		return nil
	})
	return manifest, err
}

func (s *FileStore) saveManifest(ctx context.Context, updatedAt time.Time) error {
	if updatedAt.IsZero() {
		updatedAt = s.now().UTC()
	}
	return s.withPathLock(ctx, s.manifestLockPath, func() error {
		manifest, err := s.loadManifestUnlocked()
		if err != nil {
			return err
		}
		if manifest.ID == "" {
			manifest.ID = s.id
		}
		if manifest.ID != s.id {
			return fmt.Errorf("session: manifest id %s does not match store id %s", manifest.ID, s.id)
		}
		if manifest.CreatedAt.IsZero() {
			manifest.CreatedAt = updatedAt
		}
		manifest.UpdatedAt = updatedAt
		manifest.Layout = defaultLayout()
		manifest.Agents = upsertAgentFile(manifest.Agents, AgentFileEntry{
			ID:        s.agentID,
			Path:      filepath.ToSlash(filepath.Join(agentsDirName, s.agentID+".jsonl")),
			CreatedAt: updatedAt,
			UpdatedAt: updatedAt,
		})
		return writeJSONFile(s.manifestPath, manifest, "session manifest")
	})
}

func (s *FileStore) withPathLock(ctx context.Context, lockPath string, fn func() error) error {
	for {
		lockFile, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			_, writeErr := fmt.Fprintf(lockFile, "pid=%d\ncreated_at=%s\n", os.Getpid(), s.now().UTC().Format(time.RFC3339Nano))
			closeErr := lockFile.Close()
			if writeErr != nil {
				_ = os.Remove(lockPath)
				return fmt.Errorf("write session lock %s: %w", lockPath, writeErr)
			}
			if closeErr != nil {
				_ = os.Remove(lockPath)
				return fmt.Errorf("close session lock %s: %w", lockPath, closeErr)
			}
			defer os.Remove(lockPath)
			return fn()
		}
		if !lockBusy(err, lockPath) {
			return fmt.Errorf("acquire session lock %s: %w", lockPath, err)
		}
		if s.removeStaleLock(lockPath) {
			continue
		}

		timer := time.NewTimer(s.lockRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("acquire session lock %s: %w", lockPath, ctx.Err())
		case <-timer.C:
		}
	}
}

func lockBusy(err error, lockPath string) bool {
	if os.IsExist(err) {
		return true
	}
	if !os.IsPermission(err) {
		return false
	}
	_, statErr := os.Stat(lockPath)
	return statErr == nil
}

func (s *FileStore) removeStaleLock(lockPath string) bool {
	if s.staleAfter <= 0 {
		return false
	}
	info, err := os.Stat(lockPath)
	if err != nil {
		return os.IsNotExist(err)
	}
	if s.now().Sub(info.ModTime()) < s.staleAfter {
		return false
	}
	return os.Remove(lockPath) == nil
}

func (s *FileStore) loadManifestUnlocked() (Manifest, error) {
	data, err := os.ReadFile(s.manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			now := s.now().UTC()
			return Manifest{ID: s.id, CreatedAt: now, UpdatedAt: now, Layout: defaultLayout()}, nil
		}
		return Manifest{}, fmt.Errorf("read session manifest %s: %w", s.manifestPath, err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		now := s.now().UTC()
		return Manifest{ID: s.id, CreatedAt: now, UpdatedAt: now, Layout: defaultLayout()}, nil
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("parse session manifest %s: %w", s.manifestPath, err)
	}
	return manifest, nil
}

func writeJSONFile(path string, value any, label string) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s %s: %w", label, path, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write %s %s: %w", label, path, err)
	}
	return nil
}

func validateID(label, id string) error {
	if id == "" {
		return fmt.Errorf("session: %s is required", label)
	}
	if strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return fmt.Errorf("session: invalid %s %q", label, id)
	}
	return nil
}

func defaultLayout() Layout {
	return Layout{
		ManifestFile: manifestName,
		AgentsDir:    agentsDirName,
		AgentFiles:   agentsDirName + "/<agent-id>.jsonl",
		LockFiles:    "<jsonl-file>.lock",
	}
}

func upsertAgentFile(entries []AgentFileEntry, next AgentFileEntry) []AgentFileEntry {
	for i, entry := range entries {
		if entry.ID == next.ID {
			if entry.CreatedAt.IsZero() {
				entry.CreatedAt = next.CreatedAt
			}
			entry.Path = next.Path
			entry.UpdatedAt = next.UpdatedAt
			entries[i] = entry
			return entries
		}
	}
	return append(entries, next)
}

func cloneMessagePtr(message *llm.Message) *llm.Message {
	if message == nil {
		return nil
	}
	cloned := *message
	cloned.ToolCalls = cloneLLMToolCalls(message.ToolCalls)
	cloned.Usage = cloneUsage(message.Usage)
	return &cloned
}

func cloneEventPtr(event *Event) *Event {
	if event == nil {
		return nil
	}
	cloned := *event
	cloned.Payload = append(json.RawMessage(nil), event.Payload...)
	return &cloned
}

func cloneLLMToolCalls(calls []llm.ToolCall) []llm.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	cloned := make([]llm.ToolCall, 0, len(calls))
	for _, call := range calls {
		cloned = append(cloned, llm.ToolCall{
			ID:    call.ID,
			Name:  call.Name,
			Input: append(json.RawMessage(nil), call.Input...),
		})
	}
	return cloned
}

func cloneUsage(usage *llm.Usage) *llm.Usage {
	if usage == nil {
		return nil
	}
	cloned := *usage
	return &cloned
}

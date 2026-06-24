package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrNoInterruptedTurn = errors.New("session: no interrupted turn")

type ResumeCheckpoint struct {
	SessionID   string   `json:"session_id"`
	AgentID     string   `json:"agent_id"`
	TurnID      string   `json:"turn_id"`
	Task        string   `json:"task,omitempty"`
	WorkDir     string   `json:"work_dir,omitempty"`
	AgentName   string   `json:"agent_name,omitempty"`
	StepIndex   int      `json:"step_index,omitempty"`
	Records     []Record `json:"records,omitempty"`
	TurnRecords []Record `json:"turn_records,omitempty"`
}

func OpenResumeFileStore(ctx context.Context, rootDir, sessionID, agentID string) (*FileStore, ResumeCheckpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return nil, ResumeCheckpoint{}, fmt.Errorf("session: directory is required")
	}
	sessionID = strings.TrimSpace(sessionID)
	if err := validateID("session id", sessionID); err != nil {
		return nil, ResumeCheckpoint{}, err
	}

	sessionDir := filepath.Join(rootDir, sessionID)
	manifestPath := filepath.Join(sessionDir, manifestName)
	manifest, err := readExistingManifest(manifestPath)
	if err != nil {
		return nil, ResumeCheckpoint{}, err
	}
	if manifest.ID != "" && manifest.ID != sessionID {
		return nil, ResumeCheckpoint{}, fmt.Errorf("session: manifest id %s does not match resume id %s", manifest.ID, sessionID)
	}

	entry, err := selectResumeAgent(manifest, agentID)
	if err != nil {
		return nil, ResumeCheckpoint{}, err
	}
	if err := validateID("agent id", entry.ID); err != nil {
		return nil, ResumeCheckpoint{}, err
	}

	agentPath, err := resolveManifestAgentPath(sessionDir, entry)
	if err != nil {
		return nil, ResumeCheckpoint{}, err
	}
	if _, err := os.Stat(agentPath); err != nil {
		if os.IsNotExist(err) {
			return nil, ResumeCheckpoint{}, fmt.Errorf("session: resume agent file %s does not exist", agentPath)
		}
		return nil, ResumeCheckpoint{}, fmt.Errorf("session: stat resume agent file %s: %w", agentPath, err)
	}

	store := &FileStore{
		id:               sessionID,
		agentID:          entry.ID,
		rootDir:          rootDir,
		sessionDir:       sessionDir,
		agentsDir:        filepath.Join(sessionDir, agentsDirName),
		path:             agentPath,
		lockPath:         agentPath + ".lock",
		manifestPath:     manifestPath,
		manifestLockPath: manifestPath + ".lock",
		lockRetry:        defaultLockRetry,
		staleAfter:       defaultStaleAge,
		now:              timeNow,
	}

	records, err := store.Load(ctx)
	if err != nil {
		return nil, ResumeCheckpoint{}, err
	}
	checkpoint, err := FindInterruptedTurn(records)
	if err != nil {
		return nil, ResumeCheckpoint{}, err
	}
	checkpoint.SessionID = sessionID
	checkpoint.AgentID = entry.ID
	return store, checkpoint, nil
}

func FindInterruptedTurn(records []Record) (ResumeCheckpoint, error) {
	if len(records) == 0 {
		return ResumeCheckpoint{}, ErrNoInterruptedTurn
	}

	completed := make(map[string]bool)
	for _, record := range records {
		if record.TurnID == "" {
			continue
		}
		if record.Kind == RecordKindUsageSummary {
			completed[record.TurnID] = true
		}
	}

	seen := make(map[string]bool)
	turnID := ""
	for i := len(records) - 1; i >= 0; i-- {
		candidate := records[i].TurnID
		if candidate == "" || seen[candidate] {
			continue
		}
		seen[candidate] = true
		if !completed[candidate] {
			turnID = candidate
			break
		}
	}
	if turnID == "" {
		return ResumeCheckpoint{}, ErrNoInterruptedTurn
	}

	checkpoint := ResumeCheckpoint{
		TurnID:  turnID,
		Records: cloneRecords(records),
	}
	for _, record := range records {
		if record.TurnID != turnID {
			continue
		}
		checkpoint.TurnRecords = append(checkpoint.TurnRecords, cloneRecord(record))
		if record.StepIndex > checkpoint.StepIndex {
			checkpoint.StepIndex = record.StepIndex
		}
		if record.Task != "" {
			checkpoint.Task = record.Task
		}
		if record.WorkDir != "" {
			checkpoint.WorkDir = record.WorkDir
		}
		if record.AgentName != "" {
			checkpoint.AgentName = record.AgentName
		}
		if record.SessionID != "" {
			checkpoint.SessionID = record.SessionID
		}
		if record.AgentID != "" {
			checkpoint.AgentID = record.AgentID
		}
	}
	return checkpoint, nil
}

func readExistingManifest(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, fmt.Errorf("session: resume manifest %s does not exist", path)
		}
		return Manifest{}, fmt.Errorf("session: read resume manifest %s: %w", path, err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return Manifest{}, fmt.Errorf("session: resume manifest %s is empty", path)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("session: parse resume manifest %s: %w", path, err)
	}
	if len(manifest.Agents) == 0 {
		return Manifest{}, fmt.Errorf("session: resume manifest %s has no agents", path)
	}
	return manifest, nil
}

func selectResumeAgent(manifest Manifest, requestedAgentID string) (AgentFileEntry, error) {
	requestedAgentID = strings.TrimSpace(requestedAgentID)
	if requestedAgentID != "" {
		if err := validateID("agent id", requestedAgentID); err != nil {
			return AgentFileEntry{}, err
		}
		for _, entry := range manifest.Agents {
			if entry.ID == requestedAgentID {
				return entry, nil
			}
		}
		return AgentFileEntry{}, fmt.Errorf("session: resume agent %s not found in manifest", requestedAgentID)
	}

	var selected AgentFileEntry
	for _, entry := range manifest.Agents {
		if selected.ID == "" || entry.UpdatedAt.After(selected.UpdatedAt) {
			selected = entry
		}
	}
	if selected.ID == "" {
		return AgentFileEntry{}, fmt.Errorf("session: resume manifest has no selectable agents")
	}
	return selected, nil
}

func resolveManifestAgentPath(sessionDir string, entry AgentFileEntry) (string, error) {
	agentPath := strings.TrimSpace(entry.Path)
	if agentPath == "" {
		agentPath = filepath.ToSlash(filepath.Join(agentsDirName, entry.ID+".jsonl"))
	}
	if filepath.IsAbs(agentPath) {
		return "", fmt.Errorf("session: resume agent path must be relative: %s", agentPath)
	}
	cleaned := filepath.Clean(filepath.FromSlash(agentPath))
	fullPath := filepath.Join(sessionDir, cleaned)
	rel, err := filepath.Rel(sessionDir, fullPath)
	if err != nil {
		return "", fmt.Errorf("session: resolve resume agent path %s: %w", agentPath, err)
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return "", fmt.Errorf("session: resume agent path escapes session directory: %s", agentPath)
	}
	return fullPath, nil
}

func cloneRecords(records []Record) []Record {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]Record, 0, len(records))
	for _, record := range records {
		cloned = append(cloned, cloneRecord(record))
	}
	return cloned
}

func cloneRecord(record Record) Record {
	record.Message = cloneMessagePtr(record.Message)
	record.Event = cloneEventPtr(record.Event)
	record.Usage = cloneUsage(record.Usage)
	record.UsageSummary = cloneUsage(record.UsageSummary)
	record.ContextSnapshot = cloneContextSnapshotPtr(record.ContextSnapshot)
	return record
}

func timeNow() time.Time {
	return time.Now()
}

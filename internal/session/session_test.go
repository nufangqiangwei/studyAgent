package session

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"agent/internal/llm"
)

func TestNewIDReturnsUUID(t *testing.T) {
	id, err := NewID()
	if err != nil {
		t.Fatalf("NewID returned error: %v", err)
	}

	pattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !pattern.MatchString(id) {
		t.Fatalf("id = %q, want UUID v4", id)
	}
}

func TestFileStoreSaveAppendsJSONLRecords(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}

	firstMessage := llm.Message{Role: llm.RoleUser, Content: "first"}
	secondUsage := llm.Usage{InputTokens: 3, OutputTokens: 5, TotalTokens: 8}
	if err := store.Save(context.Background(), Record{
		Kind:    RecordKindMessage,
		TurnID:  "turn-1",
		Task:    "first task",
		WorkDir: "C:\\Code\\GO\\agent",
		Message: &firstMessage,
	}); err != nil {
		t.Fatalf("first Save returned error: %v", err)
	}
	if err := store.Save(context.Background(), Record{
		Kind:         RecordKindUsageSummary,
		TurnID:       "turn-1",
		Task:         "first task",
		UsageSummary: &secondUsage,
		LLMCalls:     2,
	}); err != nil {
		t.Fatalf("second Save returned error: %v", err)
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2: %#v", len(records), records)
	}
	if records[0].SessionID != store.ID() || records[0].AgentID != store.AgentID() {
		t.Fatalf("record IDs = %#v", records[0])
	}
	if records[0].Kind != RecordKindMessage || records[0].Message == nil || records[0].Message.Content != "first" {
		t.Fatalf("first record = %#v", records[0])
	}
	if records[1].Kind != RecordKindUsageSummary || records[1].UsageSummary == nil || records[1].UsageSummary.TotalTokens != 8 || records[1].LLMCalls != 2 {
		t.Fatalf("second record = %#v", records[1])
	}

	manifest, err := store.LoadManifest(context.Background())
	if err != nil {
		t.Fatalf("LoadManifest returned error: %v", err)
	}
	if manifest.ID != store.ID() {
		t.Fatalf("manifest ID = %q, want %q", manifest.ID, store.ID())
	}
	if manifest.Layout.ManifestFile != manifestName || manifest.Layout.AgentsDir != agentsDirName || manifest.Layout.AgentFiles != "agents/<agent-id>.jsonl" {
		t.Fatalf("manifest layout = %#v", manifest.Layout)
	}
	if len(manifest.Agents) != 1 || manifest.Agents[0].ID != store.AgentID() {
		t.Fatalf("manifest agents = %#v, want one current agent", manifest.Agents)
	}
}

func TestFileStoreSaveContextSnapshot(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}

	if err := store.Save(context.Background(), Record{
		Kind:      RecordKindContextSnapshot,
		TurnID:    "turn-1",
		StepIndex: 2,
		ContextSnapshot: &ContextSnapshot{
			Messages: []llm.Message{
				{Role: llm.RoleSystem, Content: "system"},
				{Role: llm.RoleUser, Content: "Conversation summary:\nsummary"},
			},
			Summary:              "summary",
			TriggerTokens:        20_000,
			ContextWindowTokens:  32_000,
			OriginalMessageCount: 5,
		},
	}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1: %#v", len(records), records)
	}
	snapshot := records[0].ContextSnapshot
	if records[0].Kind != RecordKindContextSnapshot || snapshot == nil {
		t.Fatalf("record = %#v, want context snapshot", records[0])
	}
	if len(snapshot.Messages) != 2 || snapshot.Summary != "summary" || snapshot.TriggerTokens != 20_000 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestFileStoreSaveSerializesConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}

	const writers = 20
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			otherStore, err := OpenAgentFileStore(dir, store.ID(), store.AgentID())
			if err != nil {
				errs <- err
				return
			}
			message := llm.Message{Role: llm.RoleUser, Content: "task-" + strconv.Itoa(i)}
			errs <- otherStore.Save(context.Background(), Record{
				Kind:    RecordKindMessage,
				TurnID:  "turn-" + strconv.Itoa(i),
				Message: &message,
			})
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("Save returned error: %v", err)
		}
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(records) != writers {
		t.Fatalf("records = %d, want %d", len(records), writers)
	}
}

func TestFileStoreSaveEventAppendsStructuredEvent(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}

	if err := SaveEvent(context.Background(), store, EventScope{
		TurnID:    "turn-1",
		Task:      "build",
		WorkDir:   "C:\\Code\\GO\\agent",
		AgentName: "default",
		Step:      2,
	}, EventTypeToolCall, map[string]any{
		"id":   "call_1",
		"name": "read_file",
	}); err != nil {
		t.Fatalf("SaveEvent returned error: %v", err)
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1: %#v", len(records), records)
	}
	record := records[0]
	if record.Kind != RecordKindEvent || record.Event == nil {
		t.Fatalf("record = %#v, want event", record)
	}
	if record.Event.ID == "" || record.Event.Time.IsZero() {
		t.Fatalf("event identifiers missing: %#v", record.Event)
	}
	if record.Event.Type != EventTypeToolCall || record.Event.AgentName != "default" || record.Event.Step != 2 {
		t.Fatalf("event metadata = %#v", record.Event)
	}
	var payload struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(record.Event.Payload, &payload); err != nil {
		t.Fatalf("parse event payload: %v", err)
	}
	if payload.ID != "call_1" || payload.Name != "read_file" {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestFileStoreUsesSeparateFilesForSeparateAgents(t *testing.T) {
	dir := t.TempDir()
	first, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	second, err := OpenFileStore(dir, first.ID())
	if err != nil {
		t.Fatalf("OpenFileStore returned error: %v", err)
	}
	if first.AgentID() == second.AgentID() {
		t.Fatalf("agent IDs should differ: %s", first.AgentID())
	}

	firstMessage := llm.Message{Role: llm.RoleUser, Content: "first"}
	secondMessage := llm.Message{Role: llm.RoleUser, Content: "second"}
	if err := first.Save(context.Background(), Record{Kind: RecordKindMessage, AgentName: "worker-a", Message: &firstMessage}); err != nil {
		t.Fatalf("first Save returned error: %v", err)
	}
	if err := second.Save(context.Background(), Record{Kind: RecordKindMessage, AgentName: "worker-b", Message: &secondMessage}); err != nil {
		t.Fatalf("second Save returned error: %v", err)
	}

	firstRecords, err := first.Load(context.Background())
	if err != nil {
		t.Fatalf("first Load returned error: %v", err)
	}
	secondRecords, err := second.Load(context.Background())
	if err != nil {
		t.Fatalf("second Load returned error: %v", err)
	}
	if len(firstRecords) != 1 || firstRecords[0].Message.Content != "first" {
		t.Fatalf("first records = %#v", firstRecords)
	}
	if len(secondRecords) != 1 || secondRecords[0].Message.Content != "second" {
		t.Fatalf("second records = %#v", secondRecords)
	}

	manifest, err := first.LoadManifest(context.Background())
	if err != nil {
		t.Fatalf("LoadManifest returned error: %v", err)
	}
	if len(manifest.Agents) != 2 {
		t.Fatalf("manifest agents = %d, want 2: %#v", len(manifest.Agents), manifest.Agents)
	}
}

func TestOpenResumeFileStoreFindsInterruptedTurn(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	msg := llm.Message{Role: llm.RoleUser, Content: "Task:\nresume me"}
	if err := store.Save(context.Background(), Record{
		Kind:      RecordKindMessage,
		TurnID:    "turn-1",
		Task:      "resume me",
		WorkDir:   "C:\\Code\\GO\\agent",
		AgentName: "default",
		StepIndex: 2,
		Message:   &msg,
	}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	resumeStore, checkpoint, err := OpenResumeFileStore(context.Background(), dir, store.ID(), "")
	if err != nil {
		t.Fatalf("OpenResumeFileStore returned error: %v", err)
	}
	if resumeStore.ID() != store.ID() || resumeStore.AgentID() != store.AgentID() {
		t.Fatalf("resume store ids = %s/%s, want %s/%s", resumeStore.ID(), resumeStore.AgentID(), store.ID(), store.AgentID())
	}
	if checkpoint.TurnID != "turn-1" || checkpoint.Task != "resume me" || checkpoint.WorkDir != "C:\\Code\\GO\\agent" || checkpoint.AgentName != "default" || checkpoint.StepIndex != 2 {
		t.Fatalf("checkpoint = %#v", checkpoint)
	}
	if len(checkpoint.TurnRecords) != 1 || len(checkpoint.Records) != 1 {
		t.Fatalf("checkpoint records = %#v", checkpoint)
	}
}

func TestOpenResumeFileStoreReturnsNoInterruptedTurnForCompletedSession(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	msg := llm.Message{Role: llm.RoleUser, Content: "done"}
	usage := llm.Usage{TotalTokens: 1}
	if err := store.Save(context.Background(), Record{Kind: RecordKindMessage, TurnID: "turn-1", Task: "done", Message: &msg}); err != nil {
		t.Fatalf("Save message returned error: %v", err)
	}
	if err := store.Save(context.Background(), Record{Kind: RecordKindUsageSummary, TurnID: "turn-1", Task: "done", UsageSummary: &usage, LLMCalls: 1}); err != nil {
		t.Fatalf("Save summary returned error: %v", err)
	}

	_, _, err = OpenResumeFileStore(context.Background(), dir, store.ID(), "")
	if !errors.Is(err, ErrNoInterruptedTurn) {
		t.Fatalf("error = %v, want ErrNoInterruptedTurn", err)
	}
}

func TestOpenResumeFileStoreSelectsMostRecentlyUpdatedAgent(t *testing.T) {
	dir := t.TempDir()
	first, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	firstMsg := llm.Message{Role: llm.RoleUser, Content: "first"}
	if err := first.Save(context.Background(), Record{Kind: RecordKindMessage, TurnID: "turn-1", Task: "first", AgentName: "default", Message: &firstMsg}); err != nil {
		t.Fatalf("first Save returned error: %v", err)
	}
	time.Sleep(time.Millisecond)
	second, err := OpenFileStore(dir, first.ID())
	if err != nil {
		t.Fatalf("OpenFileStore returned error: %v", err)
	}
	secondMsg := llm.Message{Role: llm.RoleUser, Content: "second"}
	if err := second.Save(context.Background(), Record{Kind: RecordKindMessage, TurnID: "turn-2", Task: "second", AgentName: "analyze", Message: &secondMsg}); err != nil {
		t.Fatalf("second Save returned error: %v", err)
	}

	resumeStore, checkpoint, err := OpenResumeFileStore(context.Background(), dir, first.ID(), "")
	if err != nil {
		t.Fatalf("OpenResumeFileStore returned error: %v", err)
	}
	if resumeStore.AgentID() != second.AgentID() || checkpoint.TurnID != "turn-2" || checkpoint.Task != "second" {
		t.Fatalf("selected agent/checkpoint = %s/%#v, want second agent %s", resumeStore.AgentID(), checkpoint, second.AgentID())
	}
}

func TestOpenResumeFileStoreReportsMissingManifest(t *testing.T) {
	_, _, err := OpenResumeFileStore(context.Background(), t.TempDir(), "11111111-1111-4111-8111-111111111111", "")
	if err == nil || !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("error = %v, want missing manifest", err)
	}
}

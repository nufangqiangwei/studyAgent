package session

import (
	"context"
	"regexp"
	"strconv"
	"sync"
	"testing"

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

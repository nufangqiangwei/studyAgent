package tools

import (
	"agent/internal/content"
	"agent/internal/policy"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewDefaultRegistryRegistersAndPublishesTools(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry returned error: %v", err)
	}

	assertDefaultTools(t, registry.List())
	assertDefaultTools(t, RegisteredTools())
}

func assertDefaultTools(t *testing.T, got []Tool) {
	t.Helper()

	want := []string{
		ApplyPatchToolName,
		AskUserToolName,
		GetWorkspaceSummaryToolName,
		ListFilesToolName,
		ReadFileToolName,
		SearchTextToolName,
		WriteFileToolName,
	}
	if len(got) != len(want) {
		t.Fatalf("tools = %d, want %d: %#v", len(got), len(want), got)
	}
	for i, tool := range got {
		if tool.Name() != want[i] {
			t.Fatalf("tool[%d] name = %q, want %q", i, tool.Name(), want[i])
		}
	}
}

func TestRegistryChecksPolicyBeforeExecutingTool(t *testing.T) {
	tool := &recordingTool{name: WriteFileToolName}
	registry := NewRegistry()
	if err := registry.Register(tool); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	_, err := registry.Execute(context.Background(), WriteFileToolName, json.RawMessage(`{"path":"notes.txt","content":"hello"}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "policy denied tool") {
		t.Fatalf("error = %q, want policy denied tool", err.Error())
	}
	if tool.called {
		t.Fatal("tool executed even though policy denied it")
	}
}

func TestRegistryAsksForPolicyConfirmation(t *testing.T) {
	tool := &recordingTool{name: "network", result: Result{Content: "sent"}}
	registry := NewRegistry(WithPolicy(policy.New(policy.ModeModify)))
	if err := registry.Register(tool); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	var out strings.Builder
	ctx := content.WithEnv(context.Background(), &content.Env{
		IO: content.IO{
			In:  strings.NewReader("yes\n"),
			Out: &out,
		},
	})

	result, err := registry.Execute(ctx, "network", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !tool.called {
		t.Fatal("tool did not execute after confirmation")
	}
	if result.Content != "sent" {
		t.Fatalf("content = %q, want sent", result.Content)
	}
	if !strings.Contains(out.String(), "Policy confirmation required") {
		t.Fatalf("confirmation prompt missing:\n%s", out.String())
	}
}

type recordingTool struct {
	name   string
	result Result
	called bool
}

func (t *recordingTool) Name() string {
	return t.name
}

func (t *recordingTool) Description() string {
	return "record calls"
}

func (t *recordingTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object"}`)
}

func (t *recordingTool) Execute(context.Context, json.RawMessage) (Result, error) {
	t.called = true
	return t.result, nil
}

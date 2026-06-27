package tool

import (
	"agent/internal/capability/builtin/askUser"
	"agent/internal/capability/builtin/workspace"
	"agent/internal/content"
	"agent/internal/foundation/policy"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
		askUser.Name,
		workspace.GetWorkspaceSummaryToolName,
		workspace.ListFilesToolName,
		workspace.ReadFileToolName,
		workspace.SearchTextToolName,
		WriteFileToolName,
	}
	if len(got) != len(want) {
		t.Fatalf("tool = %d, want %d: %#v", len(got), len(want), got)
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

	_, err := registry.Execute(context.Background(), WriteFileToolName, json.RawMessage(`{"path":"notes.txt","content":"hello","dry_run":false}`))
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

func TestRegistryAllowsDryRunWriteValidationInReadOnlyMode(t *testing.T) {
	root := t.TempDir()
	registry := NewRegistry()
	if err := registry.Register(NewWriteFileTool()); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	result, err := registry.Execute(workspace.workspaceToolContext(root), WriteFileToolName, json.RawMessage(`{"path":"notes.txt","content":"hello"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !strings.Contains(result.Content, "dry-run") {
		t.Fatalf("content missing dry-run summary:\n%s", result.Content)
	}
	if _, err := os.Stat(filepath.Join(root, "notes.txt")); !os.IsNotExist(err) {
		t.Fatalf("notes.txt stat error = %v, want not exist", err)
	}
}

func TestRegistryAllowsWriteFileInModifyMode(t *testing.T) {
	root := t.TempDir()
	registry := NewRegistry(WithPolicy(policy.New(policy.ModeModify)))
	if err := registry.Register(NewWriteFileTool()); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	_, err := registry.Execute(workspace.workspaceToolContext(root), WriteFileToolName, json.RawMessage(`{"path":"notes.txt","content":"hello\n","dry_run":false}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if got := readToolTestFile(t, root, "notes.txt"); got != "hello\n" {
		t.Fatalf("notes.txt content = %q, want hello", got)
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

package defaulttools

import (
	"agent/internal/capability/builtin/askUser"
	"agent/internal/capability/builtin/workspace"
	"agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/foundation/policy"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewRegistryRegistersAndPublishesDefaultTools(t *testing.T) {
	registry, err := NewRegistry()
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}

	assertDefaultTools(t, registry.List())
	assertDefaultTools(t, tool.RegisteredTools())
}

func TestDefaultWriteFileDryRunAllowedInReadOnlyMode(t *testing.T) {
	root := t.TempDir()
	registry, err := NewRegistry()
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	writeTool, ok := registry.Lookup(workspace.WriteFileToolName)
	if !ok {
		t.Fatal("write_file not found")
	}

	result, err := writeTool.Execute(workspaceContext(root), json.RawMessage(`{"path":"notes.txt","content":"hello"}`))
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

func TestDefaultWriteFileNonDryRunChecksWritePolicy(t *testing.T) {
	registry, err := NewRegistry()
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	writeTool, ok := registry.Lookup(workspace.WriteFileToolName)
	if !ok {
		t.Fatal("write_file not found")
	}

	_, err = writeTool.Execute(context.Background(), json.RawMessage(`{"path":"notes.txt","content":"hello","dry_run":false}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "policy denied tool") {
		t.Fatalf("error = %q, want policy denied tool", err.Error())
	}
}

func TestDefaultWriteFileAllowedInModifyMode(t *testing.T) {
	root := t.TempDir()
	registry, err := NewRegistry(tool.WithPolicy(policy.New(policy.ModeModify)))
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	writeTool, ok := registry.Lookup(workspace.WriteFileToolName)
	if !ok {
		t.Fatal("write_file not found")
	}

	_, err = writeTool.Execute(workspaceContext(root), json.RawMessage(`{"path":"notes.txt","content":"hello\n","dry_run":false}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(root, "notes.txt"))
	if err != nil {
		t.Fatalf("read notes.txt: %v", err)
	}
	if string(data) != "hello\n" {
		t.Fatalf("notes.txt content = %q, want hello", string(data))
	}
}

func assertDefaultTools(t *testing.T, got []tool.Tool) {
	t.Helper()

	want := []string{
		workspace.ApplyPatchToolName,
		askUser.Name,
		workspace.GetWorkspaceSummaryToolName,
		workspace.ListFilesToolName,
		workspace.ReadFileToolName,
		workspace.SearchTextToolName,
		workspace.WriteFileToolName,
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

func workspaceContext(root string) context.Context {
	return content.WithEnv(context.Background(), &content.Env{
		Config: content.Config{WorkDir: root},
	})
}

package tool

import (
	"agent/internal/capability/builtin/askUser"
	"agent/internal/capability/builtin/workspace"
	"agent/internal/content"
	"agent/internal/foundation/policy"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewDefaultManagePublishesTools(t *testing.T) {
	registry, err := NewDefaultManage()
	if err != nil {
		t.Fatalf("NewDefaultManage returned error: %v", err)
	}

	assertDefaultTools(t, registry.List())
}

func assertDefaultTools(t *testing.T, got []Tool) {
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

func TestAddToolCopiesRegisteredToolByName(t *testing.T) {
	registry := NewManage()
	if err := AddTool(workspace.ListFilesToolName, registry); err != nil {
		t.Fatalf("AddTool returned error: %v", err)
	}

	got := registry.List()
	if len(got) != 1 || got[0].Name() != workspace.ListFilesToolName {
		t.Fatalf("managed tools = %#v, want only %q", got, workspace.ListFilesToolName)
	}

	_, err := registry.Execute(registryToolContext(t.TempDir()), workspace.ListFilesToolName, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Execute added tool returned error: %v", err)
	}
}

func TestManageSubsetSelectsRegisteredToolsByName(t *testing.T) {
	registry := NewManage()
	readTool := &recordingTool{name: workspace.ReadFileToolName, result: Result{Content: "read"}}
	writeTool := &recordingTool{name: workspace.WriteFileToolName}
	if err := registry.register(readTool); err != nil {
		t.Fatalf("Register read tool returned error: %v", err)
	}
	if err := registry.register(writeTool); err != nil {
		t.Fatalf("Register write tool returned error: %v", err)
	}

	subset, err := registry.Subset([]string{workspace.ReadFileToolName})
	if err != nil {
		t.Fatalf("Subset returned error: %v", err)
	}

	got := subset.List()
	if len(got) != 1 || got[0].Name() != workspace.ReadFileToolName {
		t.Fatalf("subset tools = %#v, want only %q", got, workspace.ReadFileToolName)
	}
	result, err := subset.Execute(context.Background(), workspace.ReadFileToolName, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Execute read tool returned error: %v", err)
	}
	if result.Content != "read" || !readTool.called {
		t.Fatalf("read tool result = %#v called=%t, want content read and called", result, readTool.called)
	}

	_, err = subset.Execute(context.Background(), workspace.WriteFileToolName, json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "unknown tool") {
		t.Fatalf("Execute unselected tool error = %v, want unknown tool", err)
	}
	if writeTool.called {
		t.Fatal("unselected write tool executed")
	}
}

func TestManageSubsetAppliesPolicyOptions(t *testing.T) {
	registry := NewManage()
	writeTool := &recordingTool{name: workspace.WriteFileToolName}
	if err := registry.register(writeTool); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	subset, err := registry.Subset([]string{workspace.WriteFileToolName}, WithPolicy(policy.New(policy.ModeModify)))
	if err != nil {
		t.Fatalf("Subset returned error: %v", err)
	}
	_, err = subset.Execute(context.Background(), workspace.WriteFileToolName, json.RawMessage(`{"path":"notes.txt","content":"hello","dry_run":false}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !writeTool.called {
		t.Fatal("write tool did not execute with subset policy")
	}
}

func TestManageChecksPolicyBeforeExecutingTool(t *testing.T) {
	tool := &recordingTool{name: workspace.WriteFileToolName}
	registry := NewManage()
	if err := registry.register(tool); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	_, err := registry.Execute(context.Background(), workspace.WriteFileToolName, json.RawMessage(`{"path":"notes.txt","content":"hello","dry_run":false}`))
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

func TestManageAllowsDryRunWriteValidationInReadOnlyMode(t *testing.T) {
	root := t.TempDir()
	registry := NewManage()
	if err := registry.register(workspace.NewWriteFileTool()); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	result, err := registry.Execute(registryToolContext(root), workspace.WriteFileToolName, json.RawMessage(`{"path":"notes.txt","content":"hello"}`))
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

func TestManageAllowsWriteFileInModifyMode(t *testing.T) {
	root := t.TempDir()
	registry := NewManage(WithPolicy(policy.New(policy.ModeModify)))
	if err := registry.register(workspace.NewWriteFileTool()); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	_, err := registry.Execute(registryToolContext(root), workspace.WriteFileToolName, json.RawMessage(`{"path":"notes.txt","content":"hello\n","dry_run":false}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if got := readRegistryTestFile(t, root, "notes.txt"); got != "hello\n" {
		t.Fatalf("notes.txt content = %q, want hello", got)
	}
}

func TestManageAsksForPolicyConfirmation(t *testing.T) {
	tool := &recordingTool{name: "network", result: Result{Content: "sent"}}
	registry := NewManage(WithPolicy(policy.New(policy.ModeModify)))
	if err := registry.register(tool); err != nil {
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

func TestManageAsyncPolicyApprovalReturnsStructuredError(t *testing.T) {
	tool := &recordingTool{name: "network", result: Result{Content: "sent"}}
	registry := NewManage(WithPolicy(policy.New(policy.ModeModify)), WithAsyncPolicyApproval())
	if err := registry.register(tool); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	_, err := registry.Execute(context.Background(), "network", json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	var approval *ApprovalRequiredError
	if !errors.As(err, &approval) {
		t.Fatalf("error = %T %[1]v, want ApprovalRequiredError", err)
	}
	if approval.Request.ToolName != "network" || approval.Result.Decision != policy.Ask {
		t.Fatalf("approval = %#v, want network ask decision", approval)
	}
	if tool.called {
		t.Fatal("tool executed before async approval")
	}

	result, err := registry.ExecuteApproved(context.Background(), "network", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("ExecuteApproved returned error: %v", err)
	}
	if result.Content != "sent" || !tool.called {
		t.Fatalf("result = %#v called=%t, want approved execution", result, tool.called)
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

func registryToolContext(root string) context.Context {
	return content.WithEnv(context.Background(), &content.Env{
		Config: content.Config{WorkDir: root},
	})
}

func readRegistryTestFile(t *testing.T, root, relPath string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relPath)))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return string(data)
}

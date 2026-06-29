package workspace

import (
	"agent/internal/content"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestListFilesToolListsWorkspaceEntriesWithDepth(t *testing.T) {
	root := t.TempDir()
	writeToolTestFile(t, root, "main.go", "package main\n")
	writeToolTestFile(t, root, "internal/app/app.go", "package app\n")
	writeToolTestFile(t, root, "internal/agent/native_loop.go", "package agent\n")
	writeToolTestFile(t, root, "internal/agent/deep/skip.go", "package deep\n")

	tool := NewListFilesTool()
	result, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{"path":".","max_depth":3,"limit":200}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out listFilesOutput
	if err := json.Unmarshal(result.Raw, &out); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if out.Root != "." {
		t.Fatalf("root = %q, want .", out.Root)
	}
	if out.Truncated {
		t.Fatal("truncated = true, want false")
	}
	entries := entryMap(out.Entries)
	for _, want := range []string{"main.go", "internal/app/app.go", "internal/agent/native_loop.go"} {
		if _, ok := entries[want]; !ok {
			t.Fatalf("missing entry %q in %#v", want, out.Entries)
		}
	}
	if _, ok := entries["internal/agent/deep/skip.go"]; ok {
		t.Fatalf("entry deeper than max_depth was included: %#v", out.Entries)
	}
}

func TestReadFileToolDefaultsToBoundedRange(t *testing.T) {
	root := t.TempDir()
	var builder strings.Builder
	for i := 1; i <= 250; i++ {
		fmt.Fprintf(&builder, "line %03d\n", i)
	}
	writeToolTestFile(t, root, "large.txt", builder.String())

	tool := NewReadFileTool()
	result, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{"path":"large.txt"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out readFileOutput
	if err := json.Unmarshal(result.Raw, &out); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if out.StartLine != 1 || out.EndLine != 200 {
		t.Fatalf("line range = %d-%d, want 1-200", out.StartLine, out.EndLine)
	}
	if !out.Truncated {
		t.Fatal("truncated = false, want true")
	}
	if strings.Contains(out.Content, "line 201") {
		t.Fatalf("content included line 201:\n%s", out.Content)
	}
	if !strings.Contains(result.Content, "200: line 200") {
		t.Fatalf("numbered content missing line 200:\n%s", result.Content)
	}
}

func TestReadFileToolReadsRequestedRange(t *testing.T) {
	root := t.TempDir()
	writeToolTestFile(t, root, "notes.txt", "alpha\nbeta\ngamma\ndelta\n")

	tool := NewReadFileTool()
	result, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{"path":"notes.txt","start_line":2,"end_line":3}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if !strings.Contains(result.Content, "2: beta") || !strings.Contains(result.Content, "3: gamma") {
		t.Fatalf("result content missing requested lines:\n%s", result.Content)
	}
	if strings.Contains(result.Content, "alpha") || strings.Contains(result.Content, "delta") {
		t.Fatalf("result content included lines outside range:\n%s", result.Content)
	}
}

func TestSearchTextToolReturnsLiteralMatches(t *testing.T) {
	root := t.TempDir()
	writeToolTestFile(t, root, "internal/content/env.go", "package content\n\ntype Env struct {}\n")
	writeToolTestFile(t, root, "internal/app/app.go", "package app\n")

	tool := NewSearchTextTool()
	result, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{"query":"type Env struct","path":"internal","limit":50}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out searchTextOutput
	if err := json.Unmarshal(result.Raw, &out); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if len(out.Matches) != 1 {
		t.Fatalf("matches = %#v, want one match", out.Matches)
	}
	if out.Matches[0].Path != "internal/content/env.go" || out.Matches[0].LineNumber != 3 {
		t.Fatalf("match = %#v", out.Matches[0])
	}
}

func TestGetWorkspaceSummaryToolReportsProjectContext(t *testing.T) {
	root := t.TempDir()
	writeToolTestFile(t, root, "go.mod", "module example.com/agent\n")
	writeToolTestFile(t, root, "main.go", "package main\n")
	writeToolTestFile(t, root, "README.md", "# Agent\n")
	writeToolTestFile(t, root, "internal/tool/tool.go", "package tool\n")
	writeToolTestFile(t, root, "docs/overview.md", "# Overview\n")

	tool := NewGetWorkspaceSummaryTool()
	result, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out workspaceSummary
	if err := json.Unmarshal(result.Raw, &out); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if out.ModuleName != "example.com/agent" {
		t.Fatalf("module name = %q", out.ModuleName)
	}
	if out.TestCommand != "go test ./..." {
		t.Fatalf("test command = %q", out.TestCommand)
	}
	for _, want := range []string{"module name: example.com/agent", "go test ./...", "Go", "Markdown"} {
		if !strings.Contains(result.Content, want) {
			t.Fatalf("summary missing %q:\n%s", want, result.Content)
		}
	}
}

func TestWorkspaceToolsRejectPathsOutsideWorkspace(t *testing.T) {
	root := t.TempDir()
	tool := NewReadFileTool()

	_, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{"path":"../outside.txt"}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "escapes workspace root") {
		t.Fatalf("error = %q, want escapes workspace root", err.Error())
	}
}

func workspaceToolContext(root string) context.Context {
	return content.WithEnv(context.Background(), &content.Env{
		Config: content.Config{WorkDir: root},
	})
}

func writeToolTestFile(t *testing.T, root, relPath, data string) {
	t.Helper()

	fullPath := filepath.Join(root, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0700); err != nil {
		t.Fatalf("create dir for %s: %v", relPath, err)
	}
	if err := os.WriteFile(fullPath, []byte(data), 0600); err != nil {
		t.Fatalf("write %s: %v", relPath, err)
	}
}

func entryMap(entries []workspaceEntry) map[string]string {
	result := make(map[string]string, len(entries))
	for _, entry := range entries {
		result[entry.Path] = entry.Type
	}
	return result
}

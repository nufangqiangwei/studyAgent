package workspace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplyPatchToolDefaultsToDryRun(t *testing.T) {
	root := t.TempDir()
	writeToolTestFile(t, root, "notes.txt", "alpha\nbeta\n")

	tool := NewApplyPatchTool()
	result, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{
		"patch":"--- a/notes.txt\n+++ b/notes.txt\n@@ -1,2 +1,2 @@\n alpha\n-beta\n+gamma\n"
	}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out applyPatchOutput
	if err := json.Unmarshal(result.Raw, &out); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if !out.DryRun || out.Applied {
		t.Fatalf("output dry_run/applied = %t/%t, want true/false", out.DryRun, out.Applied)
	}
	if got := readToolTestFile(t, root, "notes.txt"); got != "alpha\nbeta\n" {
		t.Fatalf("file content = %q, want original content", got)
	}
	if !strings.Contains(result.Content, "dry-run: patch is valid") {
		t.Fatalf("content missing dry-run summary:\n%s", result.Content)
	}
}

func TestApplyPatchToolAppliesUnifiedDiff(t *testing.T) {
	root := t.TempDir()
	writeToolTestFile(t, root, "notes.txt", "alpha\nbeta\n")

	dryRun := false
	input, err := json.Marshal(applyPatchInput{
		Patch:  "--- a/notes.txt\n+++ b/notes.txt\n@@ -1,2 +1,2 @@\n alpha\n-beta\n+gamma\n",
		DryRun: &dryRun,
	})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	tool := NewApplyPatchTool()
	result, err := tool.Execute(workspaceToolContext(root), input)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if got := readToolTestFile(t, root, "notes.txt"); got != "alpha\ngamma\n" {
		t.Fatalf("file content = %q, want patched content", got)
	}
	if !strings.Contains(result.Content, "applied patch") {
		t.Fatalf("content missing applied summary:\n%s", result.Content)
	}
}

func TestApplyPatchToolRejectsPathsOutsideWorkspace(t *testing.T) {
	root := t.TempDir()
	tool := NewApplyPatchTool()

	_, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{
		"patch":"--- /dev/null\n+++ b/../outside.txt\n@@ -0,0 +1,1 @@\n+outside\n"
	}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "escapes workspace root") {
		t.Fatalf("error = %q, want escapes workspace root", err.Error())
	}
}

func TestApplyPatchToolRejectsGitPath(t *testing.T) {
	root := t.TempDir()
	tool := NewApplyPatchTool()

	_, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{
		"patch":"--- /dev/null\n+++ b/.git/config\n@@ -0,0 +1,1 @@\n+secret\n"
	}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "modifying .git is not allowed") {
		t.Fatalf("error = %q, want .git restriction", err.Error())
	}
}

func TestApplyPatchToolRejectsOversizedPatch(t *testing.T) {
	root := t.TempDir()
	tool := NewApplyPatchTool()

	_, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{"patch":"`+strings.Repeat("x", maxPatchBytes+1)+`"}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "patch size") {
		t.Fatalf("error = %q, want patch size", err.Error())
	}
}

func TestWriteFileToolDefaultsToDryRun(t *testing.T) {
	root := t.TempDir()
	tool := NewWriteFileTool()

	result, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{"path":"new.txt","content":"hello\n"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	var out writeFileOutput
	if err := json.Unmarshal(result.Raw, &out); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if !out.DryRun || out.Applied {
		t.Fatalf("output dry_run/applied = %t/%t, want true/false", out.DryRun, out.Applied)
	}
	if _, err := os.Stat(filepath.Join(root, "new.txt")); !os.IsNotExist(err) {
		t.Fatalf("new.txt stat error = %v, want not exist", err)
	}
}

func TestWriteFileToolWritesWhenDryRunFalse(t *testing.T) {
	root := t.TempDir()
	tool := NewWriteFileTool()
	dryRun := false
	input, err := json.Marshal(writeFileInput{
		Path:    "nested/new.txt",
		Content: "hello\n",
		DryRun:  &dryRun,
	})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	if _, err := tool.Execute(workspaceToolContext(root), input); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if got := readToolTestFile(t, root, "nested/new.txt"); got != "hello\n" {
		t.Fatalf("file content = %q, want hello", got)
	}
}

func TestWriteFileToolRequiresOverwriteForExistingFiles(t *testing.T) {
	root := t.TempDir()
	writeToolTestFile(t, root, "notes.txt", "original\n")
	tool := NewWriteFileTool()

	dryRun := false
	input, err := json.Marshal(writeFileInput{
		Path:    "notes.txt",
		Content: "replacement\n",
		DryRun:  &dryRun,
	})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	_, err = tool.Execute(workspaceToolContext(root), input)
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "overwrite=true") {
		t.Fatalf("error = %q, want overwrite=true", err.Error())
	}
	if got := readToolTestFile(t, root, "notes.txt"); got != "original\n" {
		t.Fatalf("file content = %q, want original content", got)
	}
}

func TestWriteFileToolRejectsBinaryContent(t *testing.T) {
	root := t.TempDir()
	tool := NewWriteFileTool()

	_, err := tool.Execute(workspaceToolContext(root), json.RawMessage(`{"path":"bad.txt","content":"abc\u0000def"}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "binary content") {
		t.Fatalf("error = %q, want binary content", err.Error())
	}
}

func TestWriteToolsRequireConfirmationForHighRiskPaths(t *testing.T) {
	root := t.TempDir()
	tool := NewWriteFileTool()
	dryRun := false
	input, err := json.Marshal(writeFileInput{
		Path:    "go.mod",
		Content: "module example.com/test\n",
		DryRun:  &dryRun,
	})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}

	_, err = tool.Execute(workspaceToolContext(root), input)
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "confirm_high_risk=true") {
		t.Fatalf("error = %q, want confirm_high_risk=true", err.Error())
	}
}

func TestAnalyzeApplyPatchPolicyDetectsDelete(t *testing.T) {
	facts, err := AnalyzeApplyPatchPolicy(json.RawMessage(`{"patch":"--- a/notes.txt\n+++ /dev/null\n@@ -1,1 +0,0 @@\n-alpha\n"}`))
	if err != nil {
		t.Fatalf("AnalyzeApplyPatchPolicy returned error: %v", err)
	}
	if !facts.Delete || facts.Operation != "delete" || len(facts.Paths) != 1 || facts.Paths[0] != "notes.txt" {
		t.Fatalf("facts = %#v, want delete notes.txt", facts)
	}
}

func TestAnalyzeApplyPatchPolicyDetectsHighRiskWrite(t *testing.T) {
	dryRun := false
	input, err := json.Marshal(applyPatchInput{
		Patch:  "--- a/go.mod\n+++ b/go.mod\n@@ -1,1 +1,1 @@\n-module old\n+module new\n",
		DryRun: &dryRun,
	})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	facts, err := AnalyzeApplyPatchPolicy(input)
	if err != nil {
		t.Fatalf("AnalyzeApplyPatchPolicy returned error: %v", err)
	}
	if !facts.HighRisk || facts.Operation != "high-risk write" || len(facts.Paths) != 1 || facts.Paths[0] != "go.mod" {
		t.Fatalf("facts = %#v, want high-risk go.mod", facts)
	}
}

func readToolTestFile(t *testing.T, root, relPath string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relPath)))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return string(data)
}

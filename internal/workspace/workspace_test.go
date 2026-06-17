package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveNormalizesAndRejectsEscapes(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "src"), 0755); err != nil {
		t.Fatal(err)
	}

	ws, err := New(root)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	got, err := ws.Resolve("src/../src")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	want := filepath.Join(ws.Root(), "src")
	if got != want {
		t.Fatalf("resolved path = %q, want %q", got, want)
	}

	if _, err := ws.Resolve("../outside"); !errors.Is(err, ErrPathEscapesRoot) {
		t.Fatalf("Resolve escape error = %v, want ErrPathEscapesRoot", err)
	}
	if _, err := ws.Resolve(filepath.Join(root, "src")); err == nil {
		t.Fatal("Resolve absolute path returned nil error")
	}
}

func TestListAppliesDefaultAndGitignoreRules(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, ".gitignore", "ignored.log\ntmp/cache\n")
	writeFile(t, root, "src/main.go", "package main\n")
	writeFile(t, root, "ignored.log", "ignored\n")
	writeFile(t, root, "tmp/cache/file.txt", "cached\n")
	writeFile(t, root, ".git/config", "config\n")
	writeFile(t, root, "vendor/pkg/pkg.go", "package pkg\n")
	writeFile(t, root, "node_modules/pkg/index.js", "module.exports = {}\n")
	writeBytes(t, root, "binary.dat", []byte{'a', 0, 'b'})
	writeBytes(t, root, "large.txt", bytesOf('x', DefaultMaxFileBytes+1))

	ws, err := New(root)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	entries, err := ws.List(context.Background(), ListOptions{Recursive: true, MaxEntries: 20})
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	paths := entryPaths(entries)

	assertPathPresent(t, paths, ".gitignore")
	assertPathPresent(t, paths, "src/main.go")
	assertPathMissing(t, paths, "ignored.log")
	assertPathMissing(t, paths, "tmp/cache/file.txt")
	assertPathMissing(t, paths, ".git/config")
	assertPathMissing(t, paths, "vendor/pkg/pkg.go")
	assertPathMissing(t, paths, "node_modules/pkg/index.js")
	assertPathMissing(t, paths, "binary.dat")
	assertPathMissing(t, paths, "large.txt")
}

func TestReadEnforcesRelativeTextAndSizeLimits(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "src/main.go", "package main\n")
	writeBytes(t, root, "binary.dat", []byte{'a', 0, 'b'})
	writeFile(t, root, "small.txt", "hello")

	ws, err := New(root)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	content, err := ws.Read(context.Background(), "src/main.go", ReadOptions{})
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if content.Path != "src/main.go" || content.Content != "package main\n" {
		t.Fatalf("content = %#v", content)
	}

	if _, err := ws.Read(context.Background(), "../outside", ReadOptions{}); !errors.Is(err, ErrPathEscapesRoot) {
		t.Fatalf("Read escape error = %v, want ErrPathEscapesRoot", err)
	}
	if _, err := ws.Read(context.Background(), "small.txt", ReadOptions{MaxBytes: 4}); err == nil {
		t.Fatal("Read over size limit returned nil error")
	}
	if _, err := ws.Read(context.Background(), "binary.dat", ReadOptions{}); err == nil {
		t.Fatal("Read binary file returned nil error")
	}
}

func TestSearchLimitsMatchesAndContext(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "src/main.go", strings.Join([]string{
		"before-a",
		"before-b",
		"needle one",
		"after-a",
		"after-b",
		"after-c",
		"after-d",
		"after-e",
		"after-f",
		"needle two",
	}, "\n"))

	ws, err := New(root)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	matches, err := ws.Search(context.Background(), "needle", SearchOptions{
		MaxMatches:   1,
		ContextLines: 99,
	})
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("matches = %d, want 1: %#v", len(matches), matches)
	}
	match := matches[0]
	if match.Path != "src/main.go" || match.Line != 3 || match.Column != 1 || match.LineText != "needle one" {
		t.Fatalf("match = %#v", match)
	}
	if len(match.Before) > MaxSearchContextLines || len(match.After) > MaxSearchContextLines {
		t.Fatalf("context lengths = before %d after %d, want <= %d", len(match.Before), len(match.After), MaxSearchContextLines)
	}
	if len(match.After) != MaxSearchContextLines {
		t.Fatalf("after context = %d, want capped at %d", len(match.After), MaxSearchContextLines)
	}
}

func TestSnapshotSummarizesVisibleWorkspace(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "src/main.go", "package main\n")
	writeFile(t, root, "vendor/pkg/pkg.go", "package pkg\n")

	ws, err := New(root)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	snapshot, err := ws.Snapshot(context.Background(), SnapshotOptions{MaxEntries: 10, MaxDepth: 3})
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	paths := entryPaths(snapshot.Entries)

	assertPathPresent(t, paths, "src")
	assertPathPresent(t, paths, "src/main.go")
	assertPathMissing(t, paths, "vendor")
	assertPathMissing(t, paths, "vendor/pkg/pkg.go")
	if snapshot.Files != 1 || snapshot.Dirs != 1 || snapshot.TotalSize != int64(len("package main\n")) {
		t.Fatalf("snapshot counts = files %d dirs %d size %d", snapshot.Files, snapshot.Dirs, snapshot.TotalSize)
	}
	if !strings.Contains(snapshot.Tree, "src/") || !strings.Contains(snapshot.Tree, "main.go") {
		t.Fatalf("snapshot tree = %q", snapshot.Tree)
	}
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	writeBytes(t, root, rel, []byte(content))
}

func writeBytes(t *testing.T, root, rel string, content []byte) {
	t.Helper()
	abs := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		t.Fatalf("mkdir %q: %v", filepath.Dir(abs), err)
	}
	if err := os.WriteFile(abs, content, 0644); err != nil {
		t.Fatalf("write %q: %v", rel, err)
	}
}

func bytesOf(b byte, n int64) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = b
	}
	return data
}

func entryPaths(entries []FileEntry) map[string]struct{} {
	paths := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		paths[entry.Path] = struct{}{}
	}
	return paths
}

func assertPathPresent(t *testing.T, paths map[string]struct{}, path string) {
	t.Helper()
	if _, ok := paths[path]; !ok {
		t.Fatalf("path %q missing from %#v", path, paths)
	}
}

func assertPathMissing(t *testing.T, paths map[string]struct{}, path string) {
	t.Helper()
	if _, ok := paths[path]; ok {
		t.Fatalf("path %q unexpectedly present in %#v", path, paths)
	}
}

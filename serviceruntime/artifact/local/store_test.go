package local_test

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/artifact/local"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStorePersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	store, err := local.Open(root, local.Options{Name: "local"})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: "llm/responses/one", ContentType: "text/markdown"}, strings.NewReader("large response"))
	if err != nil {
		t.Fatalf("WriteAll: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := local.Open(root, local.Options{Name: "local"})
	if err != nil {
		t.Fatal(err)
	}
	reader, info, err := reopened.Open(ctx, ref)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "large response" || info.Ref.Checksum != ref.Checksum {
		t.Fatalf("read %q with %#v", data, info.Ref)
	}
	stat, err := reopened.Stat(ctx, ref)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Ref.Checksum != ref.Checksum || stat.Ref.Size != ref.Size {
		t.Fatalf("stat = %#v, want %#v", stat.Ref, ref)
	}
}

func TestStoreDetectsCorruptionWhileReading(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	store, _ := local.Open(root, local.Options{Name: "local"})
	ref, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: "result/one"}, strings.NewReader("first"))
	if err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(root, "objects", "result", "one")
	if err := os.WriteFile(objectPath, []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.Open(ctx, ref)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = io.ReadAll(reader)
	_ = reader.Close()
	if !errors.Is(err, artifact.ErrCorrupt) {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestStoreRejectsStableKeyConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := local.Open(t.TempDir(), local.Options{Name: "local"})
	if _, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: "result/one"}, strings.NewReader("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: "result/one"}, strings.NewReader("second")); !errors.Is(err, artifact.ErrConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

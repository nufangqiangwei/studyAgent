package memory_test

import (
	"agent/serviceruntime/artifact"
	artifactmemory "agent/serviceruntime/artifact/memory"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestStoreWriteReadAndIdempotentCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := artifactmemory.New("test")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: "llm/responses/one", ContentType: "text/plain"}, strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("WriteAll: %v", err)
	}
	second, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: ref.Key, ContentType: ref.ContentType}, strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("idempotent WriteAll: %v", err)
	}
	if second != ref {
		t.Fatalf("second ref = %#v, want %#v", second, ref)
	}
	reader, info, err := store.Open(ctx, ref)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "hello" || info.Ref != ref {
		t.Fatalf("read %q with %#v", data, info.Ref)
	}
}

func TestStoreRejectsStableKeyConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := artifactmemory.New("test")
	if _, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: "result/one"}, strings.NewReader("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: "result/one"}, strings.NewReader("second")); !errors.Is(err, artifact.ErrConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

type cancelingReader struct {
	cancel context.CancelFunc
	read   bool
}

func (r *cancelingReader) Read(buffer []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	r.cancel()
	return copy(buffer, "partial"), nil
}

func TestWriteAllHonorsCancellationAndAbortsStaging(t *testing.T) {
	t.Parallel()
	store, _ := artifactmemory.New("test")
	ctx, cancel := context.WithCancel(context.Background())
	_, err := artifact.WriteAll(ctx, store, artifact.WriteRequest{Key: "result/canceled"}, &cancelingReader{cancel: cancel})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write error = %v", err)
	}
	if _, err := artifact.WriteAll(context.Background(), store, artifact.WriteRequest{Key: "result/canceled"}, strings.NewReader("complete")); err != nil {
		t.Fatalf("write after canceled staging: %v", err)
	}
}

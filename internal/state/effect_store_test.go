package state

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryEffectStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEffectStore()
	effect := NewEffect("run_1", EffectCallModel)

	stored, err := store.Append(ctx, effect)
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if stored.Status != EffectStatusPending || stored.Effect.ID != effect.ID {
		t.Fatalf("stored = %#v, want pending effect", stored)
	}

	pending, err := store.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(pending) != 1 || pending[0].Effect.Type != EffectCallModel {
		t.Fatalf("pending = %#v, want one model effect", pending)
	}

	claimed, ok, err := store.Claim(ctx, "run_1")
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if !ok || claimed.Status != EffectStatusDispatched || claimed.DispatchedAt == nil {
		t.Fatalf("claim = %#v/%v, want dispatched effect", claimed, ok)
	}

	if err := store.MarkCompleted(ctx, effect.ID); err != nil {
		t.Fatalf("MarkCompleted returned error: %v", err)
	}
	pending, err = store.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %#v, want none after completion", pending)
	}
}

func TestMemoryEffectStoreMarkFailed(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEffectStore()
	effect := NewEffect("run_1", EffectDispatchTool)
	if _, err := store.Append(ctx, effect); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := store.MarkFailed(ctx, effect.ID, errors.New("boom")); err != nil {
		t.Fatalf("MarkFailed returned error: %v", err)
	}
	pending, err := store.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %#v, want none after failure", pending)
	}
}

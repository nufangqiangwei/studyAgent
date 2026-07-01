package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryEffectStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryEffectStore()
	effect := NewEffect("run_1", EffectCallModel)
	owner := "worker_a"

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

	claimed, ok, err := store.Claim(ctx, "run_1", owner, time.Minute)
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if !ok || claimed.Status != EffectStatusDispatched || claimed.DispatchedAt == nil || claimed.Owner != owner || claimed.LeaseDeadline == nil || claimed.ClaimCount != 1 {
		t.Fatalf("claim = %#v/%v, want dispatched effect", claimed, ok)
	}

	if err := store.MarkCompleted(ctx, effect.ID, owner); err != nil {
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
	owner := "worker_a"
	if _, err := store.Append(ctx, effect); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if _, ok, err := store.Claim(ctx, "run_1", owner, time.Minute); err != nil || !ok {
		t.Fatalf("Claim returned ok=%v err=%v, want claimed", ok, err)
	}
	if err := store.MarkFailed(ctx, effect.ID, owner, errors.New("boom")); err != nil {
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

func TestMemoryEffectStoreLeasePreventsConcurrentClaimsAndRecoversAfterExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	store := NewMemoryEffectStore()
	store.now = func() time.Time { return now }
	effect := NewEffect("run_1", EffectCallModel)
	if _, err := store.Append(ctx, effect); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	first, ok, err := store.Claim(ctx, "run_1", "worker_a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first Claim ok=%v err=%v, want claimed", ok, err)
	}
	if first.Owner != "worker_a" || first.LeaseDeadline == nil || !first.LeaseDeadline.Equal(now.Add(time.Minute)) {
		t.Fatalf("first claim = %#v, want worker_a lease", first)
	}

	if _, ok, err := store.Claim(ctx, "run_1", "worker_b", time.Minute); err != nil || ok {
		t.Fatalf("second Claim before expiry ok=%v err=%v, want not claimed", ok, err)
	}

	now = now.Add(time.Minute + time.Second)
	second, ok, err := store.Claim(ctx, "run_1", "worker_b", 2*time.Minute)
	if err != nil || !ok {
		t.Fatalf("second Claim after expiry ok=%v err=%v, want claimed", ok, err)
	}
	if second.Owner != "worker_b" || second.ClaimCount != 2 || second.LeaseDeadline == nil || !second.LeaseDeadline.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("second claim = %#v, want worker_b renewed ownership", second)
	}

	if err := store.MarkCompleted(ctx, effect.ID, "worker_a"); !errors.Is(err, ErrLeaseOwnerMismatch) {
		t.Fatalf("old owner MarkCompleted error = %v, want ErrLeaseOwnerMismatch", err)
	}
	if err := store.MarkCompleted(ctx, effect.ID, "worker_b"); err != nil {
		t.Fatalf("current owner MarkCompleted returned error: %v", err)
	}
}

func TestMemoryEffectStoreRejectsExpiredOwnerCompletionAndSupportsRenewal(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	store := NewMemoryEffectStore()
	store.now = func() time.Time { return now }
	effect := NewEffect("run_1", EffectDispatchTool)
	if _, err := store.Append(ctx, effect); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if _, ok, err := store.Claim(ctx, "run_1", "worker_a", time.Minute); err != nil || !ok {
		t.Fatalf("Claim returned ok=%v err=%v, want claimed", ok, err)
	}

	renewed, err := store.RenewLease(ctx, effect.ID, "worker_a", 3*time.Minute)
	if err != nil {
		t.Fatalf("RenewLease returned error: %v", err)
	}
	if renewed.LeaseDeadline == nil || !renewed.LeaseDeadline.Equal(now.Add(3*time.Minute)) {
		t.Fatalf("renewed = %#v, want extended deadline", renewed)
	}
	if _, err := store.RenewLease(ctx, effect.ID, "worker_b", time.Minute); !errors.Is(err, ErrLeaseOwnerMismatch) {
		t.Fatalf("wrong owner RenewLease error = %v, want ErrLeaseOwnerMismatch", err)
	}

	now = now.Add(3*time.Minute + time.Second)
	if err := store.MarkFailed(ctx, effect.ID, "worker_a", errors.New("late")); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired owner MarkFailed error = %v, want ErrLeaseExpired", err)
	}
	if _, ok, err := store.Claim(ctx, "run_1", "worker_b", time.Minute); err != nil || !ok {
		t.Fatalf("Claim after expired failed writeback ok=%v err=%v, want claimed", ok, err)
	}
}

package state

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimeevent "agent/internal/event"
)

func TestMemoryEventInboxLifecycle(t *testing.T) {
	ctx := context.Background()
	inbox := NewMemoryEventInbox()
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)
	owner := "worker_a"

	stored, appended, err := inbox.Append(ctx, event)
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if !appended || stored.Status != EventInboxStatusPending || stored.Event.ID != event.ID {
		t.Fatalf("stored/appended = %#v/%v, want pending appended event", stored, appended)
	}

	_, appended, err = inbox.Append(ctx, event)
	if err != nil {
		t.Fatalf("duplicate Append returned error: %v", err)
	}
	if appended {
		t.Fatal("duplicate Append appended = true, want false")
	}

	pending, err := inbox.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(pending) != 1 || pending[0].Event.Type != runtimeevent.EventRunStarted {
		t.Fatalf("pending = %#v, want RunStarted", pending)
	}

	claimed, ok, err := inbox.Claim(ctx, "run_1", owner, time.Minute)
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if !ok || claimed.Status != EventInboxStatusClaimed || claimed.ClaimedAt == nil || claimed.Owner != owner || claimed.LeaseDeadline == nil || claimed.ClaimCount != 1 {
		t.Fatalf("claimed/ok = %#v/%v, want claimed event", claimed, ok)
	}

	if err := inbox.MarkProcessed(ctx, event.ID, owner); err != nil {
		t.Fatalf("MarkProcessed returned error: %v", err)
	}
	pending, err = inbox.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %#v, want none after processed", pending)
	}
}

func TestMemoryEventInboxMarkFailed(t *testing.T) {
	ctx := context.Background()
	inbox := NewMemoryEventInbox()
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventModelResponseReceived, nil)
	owner := "worker_a"
	if _, _, err := inbox.Append(ctx, event); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if _, ok, err := inbox.Claim(ctx, "run_1", owner, time.Minute); err != nil || !ok {
		t.Fatalf("Claim returned ok=%v err=%v, want claimed", ok, err)
	}
	if err := inbox.MarkFailed(ctx, event.ID, owner, errors.New("boom")); err != nil {
		t.Fatalf("MarkFailed returned error: %v", err)
	}
	pending, err := inbox.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %#v, want none after failure", pending)
	}
}

func TestMemoryEventInboxLeasePreventsConcurrentClaimsAndRecoversAfterExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	inbox := NewMemoryEventInbox()
	inbox.now = func() time.Time { return now }
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventModelResponseReceived, nil)
	if _, _, err := inbox.Append(ctx, event); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	first, ok, err := inbox.Claim(ctx, "run_1", "worker_a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first Claim ok=%v err=%v, want claimed", ok, err)
	}
	if first.Owner != "worker_a" || first.LeaseDeadline == nil || !first.LeaseDeadline.Equal(now.Add(time.Minute)) {
		t.Fatalf("first claim = %#v, want worker_a lease", first)
	}

	if _, ok, err := inbox.Claim(ctx, "run_1", "worker_b", time.Minute); err != nil || ok {
		t.Fatalf("second Claim before expiry ok=%v err=%v, want not claimed", ok, err)
	}

	now = now.Add(time.Minute + time.Second)
	second, ok, err := inbox.Claim(ctx, "run_1", "worker_b", 2*time.Minute)
	if err != nil || !ok {
		t.Fatalf("second Claim after expiry ok=%v err=%v, want claimed", ok, err)
	}
	if second.Owner != "worker_b" || second.ClaimCount != 2 || second.LeaseDeadline == nil || !second.LeaseDeadline.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("second claim = %#v, want worker_b ownership", second)
	}

	if err := inbox.MarkProcessed(ctx, event.ID, "worker_a"); !errors.Is(err, ErrLeaseOwnerMismatch) {
		t.Fatalf("old owner MarkProcessed error = %v, want ErrLeaseOwnerMismatch", err)
	}
	if err := inbox.MarkProcessed(ctx, event.ID, "worker_b"); err != nil {
		t.Fatalf("current owner MarkProcessed returned error: %v", err)
	}
}

func TestMemoryEventInboxRejectsExpiredOwnerCompletionAndSupportsRenewal(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	inbox := NewMemoryEventInbox()
	inbox.now = func() time.Time { return now }
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventToolCallCompleted, nil)
	if _, _, err := inbox.Append(ctx, event); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if _, ok, err := inbox.Claim(ctx, "run_1", "worker_a", time.Minute); err != nil || !ok {
		t.Fatalf("Claim returned ok=%v err=%v, want claimed", ok, err)
	}

	renewed, err := inbox.RenewLease(ctx, event.ID, "worker_a", 3*time.Minute)
	if err != nil {
		t.Fatalf("RenewLease returned error: %v", err)
	}
	if renewed.LeaseDeadline == nil || !renewed.LeaseDeadline.Equal(now.Add(3*time.Minute)) {
		t.Fatalf("renewed = %#v, want extended deadline", renewed)
	}
	if _, err := inbox.RenewLease(ctx, event.ID, "worker_b", time.Minute); !errors.Is(err, ErrLeaseOwnerMismatch) {
		t.Fatalf("wrong owner RenewLease error = %v, want ErrLeaseOwnerMismatch", err)
	}

	now = now.Add(3*time.Minute + time.Second)
	if err := inbox.MarkFailed(ctx, event.ID, "worker_a", errors.New("late")); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired owner MarkFailed error = %v, want ErrLeaseExpired", err)
	}
	if _, ok, err := inbox.Claim(ctx, "run_1", "worker_b", time.Minute); err != nil || !ok {
		t.Fatalf("Claim after expired failed writeback ok=%v err=%v, want claimed", ok, err)
	}
}

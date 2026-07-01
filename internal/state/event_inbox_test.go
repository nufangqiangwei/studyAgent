package state

import (
	"context"
	"errors"
	"testing"

	runtimeevent "agent/internal/event"
)

func TestMemoryEventInboxLifecycle(t *testing.T) {
	ctx := context.Background()
	inbox := NewMemoryEventInbox()
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)

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

	claimed, ok, err := inbox.Claim(ctx, "run_1")
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if !ok || claimed.Status != EventInboxStatusClaimed || claimed.ClaimedAt == nil {
		t.Fatalf("claimed/ok = %#v/%v, want claimed event", claimed, ok)
	}

	if err := inbox.MarkProcessed(ctx, event.ID); err != nil {
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
	if _, _, err := inbox.Append(ctx, event); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := inbox.MarkFailed(ctx, event.ID, errors.New("boom")); err != nil {
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

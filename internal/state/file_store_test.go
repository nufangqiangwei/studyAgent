package state

import (
	"context"
	"os"
	"testing"

	runtimeevent "agent/internal/event"
)

func TestFileStorePersistsMachineWritesAcrossOpen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}

	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})
	if err := stores.States.Save(ctx, NewRunState("run_1", 2)); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	machine := NewMachine(stores.States, stores.Events, stores.Effects, registry)
	started := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)
	if _, err := machine.Advance(ctx, started); err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}

	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("reopen NewFileStore returned error: %v", err)
	}
	storedState, err := reopened.States.Load(ctx, "run_1")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if storedState.Phase != PhaseWaiting || storedState.LastEventID != started.ID {
		t.Fatalf("state = %#v, want waiting with last event", storedState)
	}
	storedEvents, err := reopened.Events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(storedEvents) != 1 || storedEvents[0].ID != started.ID {
		t.Fatalf("events = %#v, want started event", storedEvents)
	}
	storedEffects, err := reopened.Effects.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(storedEffects) != 1 || storedEffects[0].Effect.Type != EffectCallModel {
		t.Fatalf("effects = %#v, want persisted model effect", storedEffects)
	}
}

func TestFileEventStoreSkipsTruncatedTailAndContinuesAppending(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	event1 := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)

	appended, err := stores.Events.Append(ctx, event1)
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if !appended {
		t.Fatal("first Append appended = false, want true")
	}
	appended, err = stores.Events.Append(ctx, event1)
	if err != nil {
		t.Fatalf("duplicate Append returned error: %v", err)
	}
	if appended {
		t.Fatal("duplicate Append appended = true, want false")
	}

	file, err := os.OpenFile(stores.Events.path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatalf("open event log: %v", err)
	}
	if _, err := file.WriteString(`{"schema_version":`); err != nil {
		_ = file.Close()
		t.Fatalf("write truncated record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close event log: %v", err)
	}

	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("reopen NewFileStore returned error: %v", err)
	}
	storedEvents, err := reopened.Events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List after truncation returned error: %v", err)
	}
	if len(storedEvents) != 1 || storedEvents[0].ID != event1.ID {
		t.Fatalf("events after truncation = %#v, want first event", storedEvents)
	}

	event2 := mustRuntimeEvent(t, "run_1", runtimeevent.EventWaitStarted, WaitingState{Reason: "tool_result"})
	appended, err = reopened.Events.Append(ctx, event2)
	if err != nil {
		t.Fatalf("Append after truncation returned error: %v", err)
	}
	if !appended {
		t.Fatal("Append after truncation appended = false, want true")
	}

	reopenedAgain, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("second reopen NewFileStore returned error: %v", err)
	}
	storedEvents, err = reopenedAgain.Events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List after second reopen returned error: %v", err)
	}
	if len(storedEvents) != 2 || storedEvents[0].ID != event1.ID || storedEvents[1].ID != event2.ID {
		t.Fatalf("events after append = %#v, want first and second event", storedEvents)
	}
}

func TestFileEffectStorePersistsClaimAndCompletion(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	effect := NewEffect("run_1", EffectDispatchTool)
	if _, err := stores.Effects.Append(ctx, effect); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	claimed, ok, err := stores.Effects.Claim(ctx, "run_1")
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if !ok || claimed.Status != EffectStatusDispatched || claimed.DispatchedAt == nil {
		t.Fatalf("claim = %#v/%v, want dispatched effect", claimed, ok)
	}

	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("reopen NewFileStore returned error: %v", err)
	}
	pending, err := reopened.Effects.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending after reopen returned error: %v", err)
	}
	if len(pending) != 1 || pending[0].Status != EffectStatusDispatched {
		t.Fatalf("pending after reopen = %#v, want dispatched pending work", pending)
	}
	if err := reopened.Effects.MarkCompleted(ctx, effect.ID); err != nil {
		t.Fatalf("MarkCompleted returned error: %v", err)
	}

	reopenedAgain, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("second reopen NewFileStore returned error: %v", err)
	}
	pending, err = reopenedAgain.Effects.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending after completion returned error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after completion = %#v, want none", pending)
	}
}

func TestFileEventInboxPersistsClaimedAndProcessedEvents(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventModelResponseReceived, nil)
	if _, _, err := stores.Inbox.Append(ctx, event); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	claimed, ok, err := stores.Inbox.Claim(ctx, "run_1")
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if !ok || claimed.Status != EventInboxStatusClaimed || claimed.ClaimedAt == nil {
		t.Fatalf("claim = %#v/%v, want claimed event", claimed, ok)
	}

	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("reopen NewFileStore returned error: %v", err)
	}
	pending, err := reopened.Inbox.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending after reopen returned error: %v", err)
	}
	if len(pending) != 1 || pending[0].Status != EventInboxStatusClaimed {
		t.Fatalf("pending after reopen = %#v, want claimed event", pending)
	}
	if err := reopened.Inbox.MarkProcessed(ctx, event.ID); err != nil {
		t.Fatalf("MarkProcessed returned error: %v", err)
	}

	reopenedAgain, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("second reopen NewFileStore returned error: %v", err)
	}
	pending, err = reopenedAgain.Inbox.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending after processed returned error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after processed = %#v, want none", pending)
	}
}

func TestMachineAdvanceRecoversWhenEventStoredBeforeState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	if err := stores.States.Save(ctx, NewRunState("run_1", 2)); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	started := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)
	if appended, err := stores.Events.Append(ctx, started); err != nil || !appended {
		t.Fatalf("prewrite event appended=%v err=%v, want appended", appended, err)
	}

	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})
	machine := NewMachine(stores.States, stores.Events, stores.Effects, registry)
	if _, err := machine.Advance(ctx, started); err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}

	storedState, err := stores.States.Load(ctx, "run_1")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if storedState.LastEventID != started.ID || storedState.Phase != PhaseWaiting {
		t.Fatalf("state = %#v, want recovered event application", storedState)
	}
	pending, err := stores.Effects.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(pending) != 1 || pending[0].Effect.ID != effectIDForEvent(started.ID, 0) {
		t.Fatalf("pending effects = %#v, want one deterministic recovered effect", pending)
	}
}

func TestMachineAdvanceDoesNotDuplicateEffectWhenEffectStoredBeforeState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	if err := stores.States.Save(ctx, NewRunState("run_1", 2)); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	started := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)
	if _, err := stores.Events.Append(ctx, started); err != nil {
		t.Fatalf("prewrite event returned error: %v", err)
	}
	prewritten := NewEffect("run_1", EffectCallModel)
	prewritten.ID = effectIDForEvent(started.ID, 0)
	if _, err := stores.Effects.Append(ctx, prewritten); err != nil {
		t.Fatalf("prewrite effect returned error: %v", err)
	}

	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})
	machine := NewMachine(stores.States, stores.Events, stores.Effects, registry)
	if _, err := machine.Advance(ctx, started); err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}

	pending, err := stores.Effects.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(pending) != 1 || pending[0].Effect.ID != prewritten.ID {
		t.Fatalf("pending effects = %#v, want one existing effect", pending)
	}
	storedState, err := stores.States.Load(ctx, "run_1")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if storedState.LastEventID != started.ID {
		t.Fatalf("LastEventID = %q, want %q", storedState.LastEventID, started.ID)
	}
}

package state

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

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

func TestFileStateStoreListReturnsLatestStatePerRunAcrossOpen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}

	run1 := NewRunState("run_1", 2)
	run2 := NewRunState("run_2", 3)
	run2.Phase = PhaseWaiting
	run2.Waiting = &WaitingState{Reason: "model_result"}
	if err := stores.States.Save(ctx, run1); err != nil {
		t.Fatalf("Save run1 returned error: %v", err)
	}
	if err := stores.States.Save(ctx, run2); err != nil {
		t.Fatalf("Save run2 returned error: %v", err)
	}
	run1.Phase = PhaseCompleted
	if err := stores.States.Save(ctx, run1); err != nil {
		t.Fatalf("Save updated run1 returned error: %v", err)
	}

	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("reopen NewFileStore returned error: %v", err)
	}
	listed, err := reopened.States.List(ctx)
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("states = %#v, want two latest states", listed)
	}
	if listed[0].RunID != "run_1" || listed[0].Phase != PhaseCompleted {
		t.Fatalf("first state = %#v, want latest completed run_1", listed[0])
	}
	if listed[1].RunID != "run_2" || listed[1].Phase != PhaseWaiting {
		t.Fatalf("second state = %#v, want waiting run_2", listed[1])
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
	owner := "worker_a"
	if _, err := stores.Effects.Append(ctx, effect); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	claimed, ok, err := stores.Effects.Claim(ctx, "run_1", owner, time.Hour)
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if !ok || claimed.Status != EffectStatusDispatched || claimed.DispatchedAt == nil || claimed.Owner != owner || claimed.LeaseDeadline == nil {
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
	if err := reopened.Effects.MarkCompleted(ctx, effect.ID, owner); err != nil {
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

func TestFileEffectStoreRecoversExpiredLeaseAcrossOpen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	stores.Effects.now = func() time.Time { return now }
	effect := NewEffect("run_1", EffectCallModel)
	if _, err := stores.Effects.Append(ctx, effect); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if _, ok, err := stores.Effects.Claim(ctx, "run_1", "worker_a", time.Minute); err != nil || !ok {
		t.Fatalf("first Claim ok=%v err=%v, want claimed", ok, err)
	}

	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("reopen NewFileStore returned error: %v", err)
	}
	reopened.Effects.now = func() time.Time { return now }
	if _, ok, err := reopened.Effects.Claim(ctx, "run_1", "worker_b", time.Minute); err != nil || ok {
		t.Fatalf("Claim before expiry ok=%v err=%v, want not claimed", ok, err)
	}

	now = now.Add(time.Minute + time.Second)
	reclaimed, ok, err := reopened.Effects.Claim(ctx, "run_1", "worker_b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim after expiry ok=%v err=%v, want claimed", ok, err)
	}
	if reclaimed.Owner != "worker_b" || reclaimed.ClaimCount != 2 {
		t.Fatalf("reclaimed = %#v, want worker_b second claim", reclaimed)
	}
	if err := reopened.Effects.MarkCompleted(ctx, effect.ID, "worker_a"); !errors.Is(err, ErrLeaseOwnerMismatch) {
		t.Fatalf("old owner MarkCompleted error = %v, want ErrLeaseOwnerMismatch", err)
	}
	if err := reopened.Effects.MarkCompleted(ctx, effect.ID, "worker_b"); err != nil {
		t.Fatalf("current owner MarkCompleted returned error: %v", err)
	}
}

func TestFileEffectStoreAllowsOnlyOneConcurrentClaimAcrossInstances(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	first, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("first NewFileStore returned error: %v", err)
	}
	second, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("second NewFileStore returned error: %v", err)
	}
	first.Effects.now = func() time.Time { return now }
	second.Effects.now = func() time.Time { return now }

	if _, err := first.Effects.Append(ctx, NewEffect("run_1", EffectCallModel)); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	type claimResult struct {
		owner string
		ok    bool
		err   error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	var wg sync.WaitGroup
	for owner, store := range map[string]*FileEffectStore{"worker_a": first.Effects, "worker_b": second.Effects} {
		wg.Add(1)
		go func(owner string, store *FileEffectStore) {
			defer wg.Done()
			<-start
			claimed, ok, err := store.Claim(ctx, "run_1", owner, time.Minute)
			if ok {
				owner = claimed.Owner
			}
			results <- claimResult{owner: owner, ok: ok, err: err}
		}(owner, store)
	}
	close(start)
	wg.Wait()
	close(results)

	claims := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("Claim for %s returned error: %v", result.owner, result.err)
		}
		if result.ok {
			claims++
		}
	}
	if claims != 1 {
		t.Fatalf("claims = %d, want exactly one owner", claims)
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
	owner := "worker_a"
	if _, _, err := stores.Inbox.Append(ctx, event); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	claimed, ok, err := stores.Inbox.Claim(ctx, "run_1", owner, time.Hour)
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if !ok || claimed.Status != EventInboxStatusClaimed || claimed.ClaimedAt == nil || claimed.Owner != owner || claimed.LeaseDeadline == nil {
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
	if err := reopened.Inbox.MarkProcessed(ctx, event.ID, owner); err != nil {
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

func TestFileEventInboxAllowsOnlyOneConcurrentClaimAcrossInstances(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	first, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("first NewFileStore returned error: %v", err)
	}
	second, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("second NewFileStore returned error: %v", err)
	}
	first.Inbox.now = func() time.Time { return now }
	second.Inbox.now = func() time.Time { return now }

	if _, _, err := first.Inbox.Append(ctx, mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	type claimResult struct {
		owner string
		ok    bool
		err   error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	var wg sync.WaitGroup
	for owner, store := range map[string]*FileEventInbox{"worker_a": first.Inbox, "worker_b": second.Inbox} {
		wg.Add(1)
		go func(owner string, store *FileEventInbox) {
			defer wg.Done()
			<-start
			claimed, ok, err := store.Claim(ctx, "run_1", owner, time.Minute)
			if ok {
				owner = claimed.Owner
			}
			results <- claimResult{owner: owner, ok: ok, err: err}
		}(owner, store)
	}
	close(start)
	wg.Wait()
	close(results)

	claims := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("Claim for %s returned error: %v", result.owner, result.err)
		}
		if result.ok {
			claims++
		}
	}
	if claims != 1 {
		t.Fatalf("claims = %d, want exactly one owner", claims)
	}
}

func TestFileEventInboxRecoversExpiredLeaseAcrossOpen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	stores, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	stores.Inbox.now = func() time.Time { return now }
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventModelResponseReceived, nil)
	if _, _, err := stores.Inbox.Append(ctx, event); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if _, ok, err := stores.Inbox.Claim(ctx, "run_1", "worker_a", time.Minute); err != nil || !ok {
		t.Fatalf("first Claim ok=%v err=%v, want claimed", ok, err)
	}

	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatalf("reopen NewFileStore returned error: %v", err)
	}
	reopened.Inbox.now = func() time.Time { return now }
	if _, ok, err := reopened.Inbox.Claim(ctx, "run_1", "worker_b", time.Minute); err != nil || ok {
		t.Fatalf("Claim before expiry ok=%v err=%v, want not claimed", ok, err)
	}

	now = now.Add(time.Minute + time.Second)
	reclaimed, ok, err := reopened.Inbox.Claim(ctx, "run_1", "worker_b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Claim after expiry ok=%v err=%v, want claimed", ok, err)
	}
	if reclaimed.Owner != "worker_b" || reclaimed.ClaimCount != 2 {
		t.Fatalf("reclaimed = %#v, want worker_b second claim", reclaimed)
	}
	if err := reopened.Inbox.MarkProcessed(ctx, event.ID, "worker_a"); !errors.Is(err, ErrLeaseOwnerMismatch) {
		t.Fatalf("old owner MarkProcessed error = %v, want ErrLeaseOwnerMismatch", err)
	}
	if err := reopened.Inbox.MarkProcessed(ctx, event.ID, "worker_b"); err != nil {
		t.Fatalf("current owner MarkProcessed returned error: %v", err)
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

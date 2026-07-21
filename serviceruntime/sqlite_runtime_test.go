package serviceruntime

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	sqlitestore "agent/serviceruntime/persistence/sqlite"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeRecoversFromReopenedSQLiteStore(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "runtime.db")
	manifest := testManifest()
	manifest.Recovery.SnapshotEveryEvents = 50
	auditCalls := 0

	// 1. Persist one completed state transition, then leave the next inbox
	// message, the produced outbox message, and the effect unfinished.
	firstStore, err := sqlitestore.Open(ctx, path, sqlitestore.Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	builder := newTestBuilder(t, firstStore, clock, &auditCalls)
	first, err := builder.Build(ctx, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Publish(ctx, contract.Message{Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, Payload: json.RawMessage(`{"amount":2}`)}); err != nil {
		t.Fatal(err)
	}
	if result, err := first.HandleNext(ctx, "counter.main"); err != nil || result.LastSequence != 1 {
		t.Fatalf("first result=%#v err=%v", result, err)
	}
	if _, err := first.Publish(ctx, contract.Message{Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, Payload: json.RawMessage(`{"amount":3}`)}); err != nil {
		t.Fatal(err)
	}

	// 2. Cross a real persistence boundary. No pending inbox, outbox, or
	// effect work is drained before the original runtime and Store are closed.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	// 3. Reopen the same SQLite file through a new Store object and verify
	// that recovery inputs came from disk rather than the old object graph.
	reopenedStore, err := sqlitestore.Open(ctx, path, sqlitestore.Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedStore.Close()
	counterRecord, found, err := reopenedStore.Instances().GetByAddress(ctx, "test-runtime", "v1", "counter.main")
	if err != nil || !found {
		t.Fatalf("persisted counter instance found=%v err=%v", found, err)
	}
	if head, err := reopenedStore.Journal().Head(ctx, counterRecord.StateStreamID); err != nil || head != 1 {
		t.Fatalf("persisted journal head=%d err=%v", head, err)
	}
	if _, found, err := reopenedStore.Snapshots().LoadLatest(ctx, counterRecord.StateStreamID); err != nil || found {
		t.Fatalf("snapshot found=%v err=%v; recovery should replay the journal", found, err)
	}
	if pending, err := reopenedStore.Inbox().CountPending(ctx, counterRecord.MailboxID); err != nil || pending != 1 {
		t.Fatalf("persisted inbox pending=%d err=%v", pending, err)
	}
	if pending, err := reopenedStore.Outbox().CountPending(ctx, "test-runtime"); err != nil || pending != 1 {
		t.Fatalf("persisted outbox pending=%d err=%v", pending, err)
	}
	unfinished, err := reopenedStore.Effects().ListUnfinished(ctx, "test-runtime")
	if err != nil || len(unfinished) != 1 || unfinished[0].Status != persistence.EffectPlanned {
		t.Fatalf("persisted unfinished effects=%#v err=%v", unfinished, err)
	}

	// 4. Build a fresh runtime around the reopened Store. Start restores the
	// service state and pending work; normal processing can then continue.
	builder = newTestBuilder(t, reopenedStore, clock, &auditCalls)
	second, err := builder.Build(ctx, manifest)
	if err != nil {
		t.Fatal(err)
	}
	report, err := second.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if report.EventsReplayed != 1 || report.PendingInbox != 1 || report.PendingOutbox != 1 || report.StreamsRestored != 3 {
		t.Fatalf("recovery report=%#v", report)
	}
	if effectResult, err := second.DispatchNextEffect(ctx); err != nil || effectResult.Status != persistence.EffectSucceeded || auditCalls != 1 {
		t.Fatalf("effect result=%#v audit_calls=%d err=%v", effectResult, auditCalls, err)
	}
	if outboxResult, err := second.DispatchNextOutbox(ctx); err != nil || outboxResult.Delivered != 1 {
		t.Fatalf("outbox result=%#v err=%v", outboxResult, err)
	}
	if sinkResult, err := second.HandleNext(ctx, "sink.main"); err != nil || sinkResult.LastSequence != 1 {
		t.Fatalf("sink result=%#v err=%v", sinkResult, err)
	}
	result, err := second.HandleNext(ctx, "counter.main")
	if err != nil || result.LastSequence != 2 {
		t.Fatalf("second result=%#v err=%v", result, err)
	}
	active, found := second.activator.Lookup(result.InstanceID)
	if !found {
		t.Fatal("counter activation was not restored")
	}
	state, sequence := active.Current()
	if sequence != 2 || string(state.Data) != `{"count":5}` {
		t.Fatalf("sequence=%d state=%s", sequence, state.Data)
	}
}

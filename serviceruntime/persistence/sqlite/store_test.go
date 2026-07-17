package sqlite

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }

func TestStorePersistsAtomicCommitAcrossReopen(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 7, 18, 8, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "runtime.db")
	store := openTestStore(t, ctx, path, clock)
	fixture := commitFixture(t, ctx, store, clock, "message-1")

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, ctx, path, clock)
	defer store.Close()
	duplicate, err := store.Committer().CommitMessage(ctx, fixture.commit)
	if err != nil || !duplicate.Duplicate || duplicate.LastSequence != 1 {
		t.Fatalf("duplicate=%#v err=%v", duplicate, err)
	}

	events, err := store.LoadStream(ctx, fixture.record.StateStreamID, 0, 0)
	if err != nil || len(events) != 1 || events[0].EventID != "event-1" {
		t.Fatalf("events=%#v err=%v", events, err)
	}
	snapshot, found, err := store.LoadLatest(ctx, fixture.record.StateStreamID)
	if err != nil || !found || snapshot.LastSequence != 1 || string(snapshot.State) != `{"count":1}` {
		t.Fatalf("snapshot=%#v found=%v err=%v", snapshot, found, err)
	}
	pendingInbox, err := store.Inbox().CountPending(ctx, fixture.record.MailboxID)
	if err != nil || pendingInbox != 0 {
		t.Fatalf("pending inbox=%d err=%v", pendingInbox, err)
	}
	pendingOutbox, err := store.Outbox().CountPending(ctx, fixture.record.RuntimeID)
	if err != nil || pendingOutbox != 1 {
		t.Fatalf("pending outbox=%d err=%v", pendingOutbox, err)
	}
	effects, err := store.Effects().ListUnfinished(ctx, fixture.record.RuntimeID)
	if err != nil || len(effects) != 1 || effects[0].EffectID != "effect-1" {
		t.Fatalf("effects=%#v err=%v", effects, err)
	}
	restored, found, err := store.Instances().Get(ctx, fixture.record.InstanceID)
	if err != nil || !found || restored.ActivationEpoch != fixture.lease.Epoch {
		t.Fatalf("instance=%#v found=%v err=%v", restored, found, err)
	}

	message := contract.Message{ID: "message-next", Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, RuntimeID: fixture.record.RuntimeID, PlanRevision: fixture.record.PlanRevision}
	target := instance.DeliveryTarget{RuntimeID: fixture.record.RuntimeID, PlanRevision: fixture.record.PlanRevision, Address: fixture.record.Address, InstanceID: fixture.record.InstanceID, MailboxID: fixture.record.MailboxID}
	inbox, _, err := store.Inbox().Enqueue(ctx, target, message)
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := store.Inbox().ClaimNext(ctx, fixture.record.MailboxID, "host", time.Minute)
	if err != nil || !ok {
		t.Fatalf("second claim ok=%v err=%v", ok, err)
	}
	newLease, err := store.Leases().Acquire(ctx, fixture.record.InstanceID, "activation", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	stale := persistence.MessageCommit{
		RuntimeID: fixture.record.RuntimeID, PlanRevision: fixture.record.PlanRevision,
		InstanceID: fixture.record.InstanceID, ActivationEpoch: fixture.lease.Epoch,
		Ack:      persistence.InboxAck{InboxID: inbox.InboxID, MessageID: message.ID, LeaseToken: claim.LeaseToken, AckedAt: clock.now},
		StreamID: fixture.record.StateStreamID, ExpectedSequence: 1,
	}
	if _, err := store.Committer().CommitMessage(ctx, stale); !errors.Is(err, persistence.ErrStaleActivation) {
		t.Fatalf("stale error=%v", err)
	}
	stale.ActivationEpoch = newLease.Epoch
	stale.ExpectedSequence = 0
	if _, err := store.Committer().CommitMessage(ctx, stale); !errors.Is(err, persistence.ErrSequenceConflict) {
		t.Fatalf("conflict error=%v", err)
	}
}

func TestExpiredStartedEffectIsReconciledInsteadOfExecuted(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "runtime.db")
	store := openTestStore(t, ctx, path, clock)
	fixture := commitFixture(t, ctx, store, clock, "message-2")
	claim, found, err := store.Effects().ClaimNext(ctx, fixture.record.RuntimeID, "crashed-worker", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	if err := store.Effects().MarkStarted(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Minute)
	store = openTestStore(t, ctx, path, clock)
	defer store.Close()

	register := building.NewRegister(nil)
	plan, err := register.Compile(ctx, building.RuntimeManifest{Runtime: building.RuntimeSpec{ID: fixture.record.RuntimeID, Revision: fixture.record.PlanRevision}})
	if err != nil {
		t.Fatal(err)
	}
	registry := effect.NewRegistry()
	executed, reconciled := 0, 0
	if err := registry.Register(effect.Spec{
		Ref:  "audit.local",
		Type: "audit.write",
		Executor: effect.ExecutorFunc(func(context.Context, persistence.EffectRecord) (effect.ExecutionResult, error) {
			executed++
			return effect.ExecutionResult{}, nil
		}),
		Reconciler: effect.ReconcilerFunc(func(context.Context, persistence.EffectRecord) (effect.ReconciliationResult, error) {
			reconciled++
			return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: json.RawMessage(`{"recovered":true}`)}, nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	worker, err := effect.NewWorker(effect.WorkerOptions{Plan: plan, Store: store.Effects(), Registry: registry, Clock: clock, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.DispatchNext(ctx, "recovery-worker")
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != persistence.EffectSucceeded || executed != 0 || reconciled != 1 {
		t.Fatalf("result=%#v executed=%d reconciled=%d", result, executed, reconciled)
	}
}

type fixtureResult struct {
	record instance.Record
	lease  instance.ActivationLease
	commit persistence.MessageCommit
}

func commitFixture(t *testing.T, ctx context.Context, store *Store, clock *testClock, messageID string) fixtureResult {
	t.Helper()
	record := instance.Record{
		InstanceID: "instance-1", Address: "counter.main", Kind: instance.ServiceStatic,
		DefinitionRef: contract.ComponentRef{Type: "counter", Version: "v1"}, RuntimeID: "runtime-1", PlanRevision: "v1",
		RootID: "instance-1", MailboxID: "mailbox-1", StateStreamID: "service/instance-1",
		Lifecycle: instance.Declared, CreatedAt: clock.now, UpdatedAt: clock.now,
	}
	if err := store.Instances().Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	message := contract.Message{ID: messageID, Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision}
	_, _, err := store.Inbox().Enqueue(ctx, instance.DeliveryTarget{RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision, Address: record.Address, InstanceID: record.InstanceID, MailboxID: record.MailboxID}, message)
	if err != nil {
		t.Fatal(err)
	}
	claim, found, err := store.Inbox().ClaimNext(ctx, record.MailboxID, "host", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	lease, err := store.Leases().Acquire(ctx, record.InstanceID, "activation", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	event := contract.StoredEvent{EventID: "event-1", StreamID: record.StateStreamID, StreamType: "counter", Sequence: 1, EventType: "counter.incremented", EventVersion: 1, PlanRevision: record.PlanRevision, ServiceVersion: "v1", CausationID: message.ID, Payload: json.RawMessage(`{"amount":1}`), OccurredAt: clock.now}
	snapshot := contract.Snapshot{StreamID: record.StateStreamID, AggregateType: "counter", OwnerService: record.Address, PlanRevision: record.PlanRevision, SchemaVersion: 1, LastSequence: 1, State: json.RawMessage(`{"count":1}`), Checksum: "checksum", CreatedAt: clock.now}
	outgoing := contract.Message{ID: "outgoing-1", Kind: contract.MessageEvent, Type: "counter.changed", Version: 1, RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision}
	commit := persistence.MessageCommit{
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision, InstanceID: record.InstanceID, ActivationEpoch: lease.Epoch,
		Ack:      persistence.InboxAck{InboxID: claim.Record.InboxID, MessageID: message.ID, LeaseToken: claim.LeaseToken, AckedAt: clock.now},
		StreamID: record.StateStreamID, Events: []contract.StoredEvent{event}, Snapshot: &snapshot,
		Outbox:  []persistence.OutboxRecord{{OutboxID: "outbox-1", Message: outgoing, Status: persistence.OutboxPending, CreatedAt: clock.now}},
		Effects: []persistence.EffectRecord{{EffectID: "effect-1", SourceMessageID: message.ID, Type: "audit.write", Version: 1, ExecutorRef: "audit.local", IdempotencyKey: "audit:" + message.ID, Status: persistence.EffectPlanned, PlannedAt: clock.now}},
	}
	if _, err := store.Committer().CommitMessage(ctx, commit); err != nil {
		t.Fatal(err)
	}
	return fixtureResult{record: record, lease: lease, commit: commit}
}

func openTestStore(t *testing.T, ctx context.Context, path string, clock *testClock) *Store {
	t.Helper()
	store, err := Open(ctx, path, Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

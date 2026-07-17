package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

func TestCommitMessageIsAtomicIdempotentAndFenced(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	store := New(testClock{now: now})
	record := instance.Record{
		InstanceID: "instance-1", Address: "counter.main", Kind: instance.ServiceStatic,
		DefinitionRef: contract.ComponentRef{Type: "counter", Version: "v1"},
		RuntimeID:     "runtime-1", PlanRevision: "v1",
		MailboxID: "mailbox-1", StateStreamID: "service/instance-1",
		Lifecycle: instance.Declared, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, record.InstanceID, "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	target := instance.DeliveryTarget{RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision, Address: record.Address, InstanceID: record.InstanceID, MailboxID: record.MailboxID}
	message := contract.Message{ID: "message-1", Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision}
	inbox, _, err := store.Inbox().Enqueue(ctx, target, message)
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := store.Inbox().ClaimNext(ctx, record.MailboxID, "worker-1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	state := json.RawMessage(`{"count":1}`)
	commit := persistence.MessageCommit{
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		InstanceID: record.InstanceID, ActivationEpoch: lease.Epoch,
		Ack:      persistence.InboxAck{InboxID: inbox.InboxID, MessageID: message.ID, LeaseToken: claim.LeaseToken, AckedAt: now},
		StreamID: record.StateStreamID, ExpectedSequence: 0,
		Events: []contract.StoredEvent{{
			EventID: "event-1", StreamID: record.StateStreamID, StreamType: "counter", Sequence: 1,
			EventType: "counter.incremented", EventVersion: 1, PlanRevision: record.PlanRevision,
			ServiceVersion: "v1", OccurredAt: now,
		}},
		Snapshot: &contract.Snapshot{
			StreamID: record.StateStreamID, AggregateType: "counter", OwnerService: record.Address,
			PlanRevision: record.PlanRevision, SchemaVersion: 1, LastSequence: 1, State: state, CreatedAt: now,
		},
		Outbox: []persistence.OutboxRecord{{
			OutboxID: "outbox-1", Message: contract.Message{ID: "event-message-1", Kind: contract.MessageEvent, Type: "counter.changed", Version: 1, RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision},
		}},
		Effects: []persistence.EffectRecord{{
			EffectID: "effect-1", Type: "audit.write", Version: 1,
			ExecutorRef: "audit.local", IdempotencyKey: "audit:message-1",
		}},
	}
	result, err := store.CommitMessage(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	if result.LastSequence != 1 || len(result.StoredEventIDs) != 1 || len(result.StoredOutboxIDs) != 1 || len(result.StoredEffectIDs) != 1 {
		t.Fatalf("commit result = %#v", result)
	}
	duplicate, err := store.CommitMessage(ctx, commit)
	if err != nil || !duplicate.Duplicate || duplicate.LastSequence != 1 {
		t.Fatalf("duplicate result=%#v err=%v", duplicate, err)
	}
	head, _ := store.Head(ctx, record.StateStreamID)
	snapshot, found, _ := store.LoadLatest(ctx, record.StateStreamID)
	if head != 1 || !found || string(snapshot.State) != string(state) {
		t.Fatalf("head=%d snapshot_found=%v snapshot=%s", head, found, snapshot.State)
	}

	secondMessage := message.Clone()
	secondMessage.ID = "message-2"
	secondInbox, _, err := store.Inbox().Enqueue(ctx, target, secondMessage)
	if err != nil {
		t.Fatal(err)
	}
	secondClaim, ok, err := store.Inbox().ClaimNext(ctx, record.MailboxID, "worker-1", time.Minute)
	if err != nil || !ok || secondClaim.Record.InboxID != secondInbox.InboxID {
		t.Fatalf("second claim=%#v ok=%v err=%v", secondClaim, ok, err)
	}
	newLease, err := store.Acquire(ctx, record.InstanceID, "worker-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	stale := persistence.MessageCommit{
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		InstanceID: record.InstanceID, ActivationEpoch: lease.Epoch,
		Ack:      persistence.InboxAck{InboxID: secondInbox.InboxID, MessageID: secondMessage.ID, LeaseToken: secondClaim.LeaseToken, AckedAt: now},
		StreamID: record.StateStreamID, ExpectedSequence: 1,
	}
	if _, err := store.CommitMessage(ctx, stale); !errors.Is(err, persistence.ErrStaleActivation) {
		t.Fatalf("stale commit error=%v, want ErrStaleActivation", err)
	}
	conflict := stale
	conflict.ActivationEpoch = newLease.Epoch
	conflict.ExpectedSequence = 0
	if _, err := store.CommitMessage(ctx, conflict); !errors.Is(err, persistence.ErrSequenceConflict) {
		t.Fatalf("conflict error=%v, want ErrSequenceConflict", err)
	}
	head, _ = store.Head(ctx, record.StateStreamID)
	if head != 1 {
		t.Fatalf("failed commits changed journal head to %d", head)
	}
}

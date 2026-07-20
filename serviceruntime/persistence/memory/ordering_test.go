package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"context"
	"testing"
	"time"
)

func TestInboxStreamRetryBlocksLaterSequenceUntilTerminal(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)}
	store := New(clock)
	target := instance.DeliveryTarget{RuntimeID: "runtime", PlanRevision: "v1", Address: "worker", InstanceID: "worker-1", MailboxID: "mailbox-1"}
	first := contract.Message{ID: "message-1", Kind: contract.MessageCommand, Type: "work", Version: 1, RuntimeID: "runtime", PlanRevision: "v1", StreamID: "run/1"}
	second := first.Clone()
	second.ID = "message-2"
	var err error
	first, err = store.Sequences().Assign(ctx, "mailbox/mailbox-1", first)
	if err != nil {
		t.Fatal(err)
	}
	second, err = store.Sequences().Assign(ctx, "mailbox/mailbox-1", second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("sequences=%d,%d", first.Sequence, second.Sequence)
	}
	if _, _, err := store.Inbox().Enqueue(ctx, target, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Inbox().Enqueue(ctx, target, second); err != nil {
		t.Fatal(err)
	}
	claim, found, err := store.Inbox().ClaimNext(ctx, target.MailboxID, "host", time.Minute)
	if err != nil || !found || claim.Record.Message.ID != first.ID {
		t.Fatalf("first claim=%#v found=%v err=%v", claim, found, err)
	}
	if err := store.Inbox().ReleaseClaim(ctx, claim, clock.now.Add(time.Hour), context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Inbox().ClaimNext(ctx, target.MailboxID, "host", time.Minute); err != nil || found {
		t.Fatalf("later sequence bypassed retry: found=%v err=%v", found, err)
	}
	clock.now = clock.now.Add(2 * time.Hour)
	claim, found, err = store.Inbox().ClaimNext(ctx, target.MailboxID, "host", time.Minute)
	if err != nil || !found || claim.Record.Message.ID != first.ID {
		t.Fatalf("retry claim=%#v found=%v err=%v", claim, found, err)
	}
	if err := store.Inbox().MoveToDeadLetter(ctx, claim, context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	claim, found, err = store.Inbox().ClaimNext(ctx, target.MailboxID, "host", time.Minute)
	if err != nil || !found || claim.Record.Message.ID != second.ID {
		t.Fatalf("second claim=%#v found=%v err=%v", claim, found, err)
	}
}

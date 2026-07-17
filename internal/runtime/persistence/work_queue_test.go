package persistence

import (
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/statemachine"
	"context"
	"testing"
)

func TestWorkQueuePersistsPendingEvent(t *testing.T) {
	queue, err := NewWorkQueue(t.TempDir())
	if err != nil {
		t.Fatalf("NewWorkQueue returned error: %v", err)
	}
	event, err := eventbus.NewEvent(statemachine.TopicTask, statemachine.EventTaskStartRequested, nil,
		eventbus.WithTaskID("task_1"), eventbus.WithSource("test"))
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	if err := queue.EnqueueEvent(context.Background(), event); err != nil {
		t.Fatalf("EnqueueEvent returned error: %v", err)
	}
	work, ok, err := queue.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("Next = %#v, %v, %v", work, ok, err)
	}
	if work.Kind != WorkEvent || work.TaskID != "task_1" {
		t.Fatalf("work = %#v", work)
	}
	if _, ok, err := queue.PopEvent(context.Background(), "task_1"); err != nil || !ok {
		t.Fatalf("PopEvent ok=%v err=%v", ok, err)
	}
	if events, effects, err := queue.PendingCounts(context.Background(), "task_1"); err != nil || events != 0 || effects != 0 {
		t.Fatalf("PendingCounts = %d, %d, %v", events, effects, err)
	}
}

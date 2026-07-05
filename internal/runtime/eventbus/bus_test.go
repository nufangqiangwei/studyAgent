package eventbus

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestPublishRoutesByTopicTypeAndTaskID(t *testing.T) {
	bus := newTestBus(t)
	defer bus.Close()
	var calls []string

	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "ToolCallCompleted", TaskID: "task_1"}, func(context.Context, Event) error {
		calls = append(calls, "exact")
		return nil
	}, WithSubscriptionID("exact"))
	mustSubscribe(t, bus, Filter{Topic: "other", Type: "ToolCallCompleted", TaskID: "task_1"}, func(context.Context, Event) error {
		calls = append(calls, "wrong-topic")
		return nil
	}, WithSubscriptionID("wrong-topic"))
	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "ModelResponseReceived", TaskID: "task_1"}, func(context.Context, Event) error {
		calls = append(calls, "wrong-type")
		return nil
	}, WithSubscriptionID("wrong-type"))
	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "ToolCallCompleted", TaskID: "task_2"}, func(context.Context, Event) error {
		calls = append(calls, "wrong-task")
		return nil
	}, WithSubscriptionID("wrong-task"))

	event := mustNewEvent(t, "runtime", "ToolCallCompleted", WithTaskID("task_1"))
	result, err := bus.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	wantCalls := []string{"exact"}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", calls, wantCalls)
	}
	if len(result.Deliveries) != 1 || result.Mode != DeliverySync || result.Broadcast {
		t.Fatalf("result = %#v, want one sync non-broadcast delivery", result)
	}
}

func TestSubscribeRejectsOverlappingFilters(t *testing.T) {
	tests := []struct {
		name     string
		existing Filter
		next     Filter
	}{
		{
			name:     "same filter",
			existing: Filter{Topic: "runtime", Type: "RunStarted", TaskID: "task_1"},
			next:     Filter{Topic: "runtime", Type: "RunStarted", TaskID: "task_1"},
		},
		{
			name:     "broad topic type overlaps exact task",
			existing: Filter{Topic: "runtime", Type: "RunStarted"},
			next:     Filter{Topic: "runtime", Type: "RunStarted", TaskID: "task_1"},
		},
		{
			name:     "wildcard topic overlaps concrete topic",
			existing: Filter{Topic: Any, Type: "RunStarted", TaskID: "task_1"},
			next:     Filter{Topic: "runtime", Type: "RunStarted", TaskID: "task_1"},
		},
		{
			name:     "empty filter overlaps everything",
			existing: Filter{},
			next:     Filter{Topic: "runtime", Type: "RunStarted", TaskID: "task_1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bus := newTestBus(t)
			defer bus.Close()

			mustSubscribe(t, bus, tt.existing, func(context.Context, Event) error {
				return nil
			}, WithSubscriptionID("existing"))

			_, err := bus.SubscribeFunc(tt.next, func(context.Context, Event) error {
				return nil
			}, WithSubscriptionID("next"))
			if err == nil {
				t.Fatal("SubscribeFunc returned nil error, want overlapping filter rejection")
			}
		})
	}
}

func TestSubscribeAllowsNonOverlappingFilters(t *testing.T) {
	bus := newTestBus(t)
	defer bus.Close()

	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "RunStarted", TaskID: "task_1"}, func(context.Context, Event) error {
		return nil
	}, WithSubscriptionID("task-1"))
	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "RunStarted", TaskID: "task_2"}, func(context.Context, Event) error {
		return nil
	}, WithSubscriptionID("task-2"))
	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "RunCompleted", TaskID: "task_1"}, func(context.Context, Event) error {
		return nil
	}, WithSubscriptionID("other-type"))
	mustSubscribe(t, bus, Filter{Topic: "tools", Type: "RunStarted", TaskID: "task_1"}, func(context.Context, Event) error {
		return nil
	}, WithSubscriptionID("other-topic"))
}

func TestSubscribeReadOnlyAllowsOverlappingFilters(t *testing.T) {
	bus := newTestBus(t)
	defer bus.Close()
	var calls []string

	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "RunStarted"}, func(context.Context, Event) error {
		calls = append(calls, "owner")
		return nil
	}, WithSubscriptionID("owner"))
	readonly, err := bus.SubscribeReadOnlyFunc(Filter{Topic: "runtime"}, func(context.Context, Event) error {
		calls = append(calls, "readonly")
		return nil
	}, WithSubscriptionID("readonly"))
	if err != nil {
		t.Fatalf("SubscribeReadOnlyFunc returned error: %v", err)
	}
	if !readonly.ReadOnly {
		t.Fatalf("readonly subscription = %#v, want ReadOnly=true", readonly)
	}

	if _, err := bus.Publish(context.Background(), mustNewEvent(t, "runtime", "RunStarted")); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"owner", "readonly"}) {
		t.Fatalf("calls = %#v, want owner and readonly", calls)
	}
}

func TestBroadcastIgnoresSubscriptionFilters(t *testing.T) {
	bus := newTestBus(t)
	defer bus.Close()
	var calls []string

	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "RunStarted"}, func(context.Context, Event) error {
		calls = append(calls, "runtime")
		return nil
	}, WithSubscriptionID("runtime"))
	mustSubscribe(t, bus, Filter{Topic: "tools", Type: "ToolCallCompleted", TaskID: "task_9"}, func(context.Context, Event) error {
		calls = append(calls, "tools")
		return nil
	}, WithSubscriptionID("tools"))

	result, err := bus.Broadcast(context.Background(), mustNewEvent(t, "announcements", "SystemTick"))
	if err != nil {
		t.Fatalf("Broadcast returned error: %v", err)
	}

	wantCalls := []string{"runtime", "tools"}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", calls, wantCalls)
	}
	if !result.Broadcast || len(result.Deliveries) != 2 {
		t.Fatalf("result = %#v, want broadcast to both subscribers", result)
	}
}

func TestPublishAsyncQueuesAndDeliversEvent(t *testing.T) {
	bus := newTestBus(t, WithQueueSize(4))
	defer bus.Close()
	delivered := make(chan Event, 1)

	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "RunStarted", TaskID: "task_1"}, func(_ context.Context, event Event) error {
		delivered <- event
		return nil
	})

	event := mustNewEvent(t, "runtime", "RunStarted", WithTaskID("task_1"))
	result, err := bus.Enqueue(context.Background(), event)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if !result.Queued || result.Mode != DeliveryAsync {
		t.Fatalf("result = %#v, want queued async result", result)
	}

	select {
	case got := <-delivered:
		if got.ID != event.ID || got.TaskID != "task_1" {
			t.Fatalf("delivered event = %#v, want original event identity", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async delivery")
	}
}

func TestPublishReportsHandlerErrors(t *testing.T) {
	bus := newTestBus(t)
	defer bus.Close()
	var calls []string

	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "RunStarted"}, func(context.Context, Event) error {
		calls = append(calls, "first")
		return errors.New("boom")
	}, WithSubscriptionID("first"))

	result, err := bus.Publish(context.Background(), mustNewEvent(t, "runtime", "RunStarted"))
	if err == nil {
		t.Fatal("Publish returned nil error, want handler failure")
	}
	if !reflect.DeepEqual(calls, []string{"first"}) {
		t.Fatalf("calls = %#v, want failing handler only", calls)
	}
	if len(result.Deliveries) != 1 || result.Deliveries[0].Error == "" {
		t.Fatalf("deliveries = %#v, want one failed delivery", result.Deliveries)
	}
}

func TestLoggerRecordsQueueAndDeliveryActions(t *testing.T) {
	var mu sync.Mutex
	var actions []string
	logger := LogFunc(func(_ context.Context, entry LogEntry) {
		mu.Lock()
		defer mu.Unlock()
		actions = append(actions, entry.Action)
	})
	bus := newTestBus(t, WithLogger(logger))
	defer bus.Close()

	mustSubscribe(t, bus, Filter{Topic: "runtime", Type: "RunStarted"}, func(context.Context, Event) error {
		return nil
	})
	if _, err := bus.Publish(context.Background(), mustNewEvent(t, "runtime", "RunStarted")); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{LogSubscribe, LogPublishSync, LogDeliverStarted, LogDeliverCompleted}
	if len(actions) < len(want) {
		t.Fatalf("actions = %#v, want at least %#v", actions, want)
	}
	for i, action := range want {
		if actions[i] != action {
			t.Fatalf("actions = %#v, want prefix %#v", actions, want)
		}
	}
}

func newTestBus(t *testing.T, options ...BusOption) *Bus {
	t.Helper()
	bus, err := New(options...)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return bus
}

func mustNewEvent(t *testing.T, topic string, eventType EventType, options ...EventOption) Event {
	t.Helper()
	event, err := NewEvent(topic, eventType, nil, options...)
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	return event
}

func mustSubscribe(t *testing.T, bus *Bus, filter Filter, handler func(context.Context, Event) error, options ...SubscribeOption) Subscription {
	t.Helper()
	subscription, err := bus.SubscribeFunc(filter, handler, options...)
	if err != nil {
		t.Fatalf("SubscribeFunc returned error: %v", err)
	}
	return subscription
}

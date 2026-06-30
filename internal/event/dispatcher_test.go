package event

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const testEvent Type = "TestEvent"

func TestRegistryCreatesDefinedEvents(t *testing.T) {
	registry := newTestRegistry(t, Definition{Type: testEvent})
	occurredAt := time.Date(2026, 6, 30, 1, 2, 3, 0, time.UTC)

	event, err := registry.NewEvent(testEvent, map[string]string{"task": "build"},
		WithID("event-1"),
		WithTime(occurredAt),
		WithSource("unit-test"),
		WithRunID("run-1"),
		WithMetadataValue("scope", "llm"),
	)
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}

	if event.ID != "event-1" || event.Type != testEvent || !event.OccurredAt.Equal(occurredAt) {
		t.Fatalf("event identity = %#v, want configured id/type/time", event)
	}
	if event.Source != "unit-test" || event.RunID != "run-1" || event.Metadata["scope"] != "llm" {
		t.Fatalf("event metadata = %#v, want source/run/scope", event)
	}
	var payload map[string]string
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("payload unmarshal returned error: %v", err)
	}
	if payload["task"] != "build" {
		t.Fatalf("payload = %#v, want task build", payload)
	}
}

func TestDispatcherCompletesManualEventIdentity(t *testing.T) {
	registry := newTestRegistry(t, Definition{Type: testEvent})
	stateMachine := &recordingStateMachine{}
	dispatcher := newTestDispatcher(t, registry, stateMachine)
	occurredAt := time.Date(2026, 6, 30, 2, 3, 4, 0, time.UTC)

	result, err := dispatcher.Emit(context.Background(), Event{
		Type:       testEvent,
		OccurredAt: occurredAt,
		Source:     "manual",
	})
	if err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}

	if result.Event.ID == "" || !result.Event.OccurredAt.Equal(occurredAt) || result.Event.Source != "manual" {
		t.Fatalf("event = %#v, want generated id with preserved time/source", result.Event)
	}
	if len(stateMachine.events) != 1 || stateMachine.events[0].ID != result.Event.ID {
		t.Fatalf("state machine events = %#v, want completed event", stateMachine.events)
	}
}

func TestDispatcherRunsHooksByLevelAndStopsPropagation(t *testing.T) {
	registry := newTestRegistry(t, Definition{Type: testEvent})
	stateMachine := &recordingStateMachine{}
	dispatcher := newTestDispatcher(t, registry, stateMachine)
	var calls []string

	registerTestHook(t, dispatcher, HookSpec{
		Name:      "middle",
		EventType: testEvent,
		Level:     HookLevelNormal,
		Handle: func(context.Context, Event) (HookResult, error) {
			calls = append(calls, "middle")
			return Stop("handled by middle"), nil
		},
	})
	registerTestHook(t, dispatcher, HookSpec{
		Name:      "first",
		EventType: testEvent,
		Level:     HookLevelHigh,
		Handle: func(context.Context, Event) (HookResult, error) {
			calls = append(calls, "first")
			return Continue(), nil
		},
	})
	registerTestHook(t, dispatcher, HookSpec{
		Name:      "last",
		EventType: testEvent,
		Level:     HookLevelLow,
		Handle: func(context.Context, Event) (HookResult, error) {
			calls = append(calls, "last")
			return Continue(), nil
		},
	})

	result, err := dispatcher.Emit(context.Background(), mustNewTestEvent(t, registry, testEvent))
	if err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}

	if strings.Join(calls, ",") != "first,middle" {
		t.Fatalf("hook calls = %#v, want first,middle", calls)
	}
	if !result.Stopped || result.StoppedBy != "middle" || result.StopReason != "handled by middle" {
		t.Fatalf("dispatch result = %#v, want stopped by middle", result)
	}
	if result.Delivered || len(stateMachine.events) != 0 {
		t.Fatalf("state machine events = %#v, want none", stateMachine.events)
	}
}

func TestRequiredDeliveryCannotBeIntercepted(t *testing.T) {
	registry := newTestRegistry(t, Definition{
		Type:     testEvent,
		Delivery: DeliveryMustReachStateMachine,
	})
	stateMachine := &recordingStateMachine{}
	dispatcher := newTestDispatcher(t, registry, stateMachine)
	var calls []string

	registerTestHook(t, dispatcher, HookSpec{
		Name:      "stopper",
		EventType: testEvent,
		Level:     HookLevelHigh,
		Handle: func(context.Context, Event) (HookResult, error) {
			calls = append(calls, "stopper")
			return Stop("must not block"), nil
		},
	})
	registerTestHook(t, dispatcher, HookSpec{
		Name:      "observer",
		EventType: testEvent,
		Level:     HookLevelLow,
		Handle: func(context.Context, Event) (HookResult, error) {
			calls = append(calls, "observer")
			return Continue(), nil
		},
	})

	result, err := dispatcher.Emit(context.Background(), mustNewTestEvent(t, registry, testEvent))
	if err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}

	if strings.Join(calls, ",") != "stopper,observer" {
		t.Fatalf("hook calls = %#v, want stopper,observer", calls)
	}
	if !result.Delivered || result.Stopped {
		t.Fatalf("dispatch result = %#v, want delivered and not stopped", result)
	}
	if len(result.HookExecutions) != 2 || !result.HookExecutions[0].StopIgnored {
		t.Fatalf("hook executions = %#v, want ignored stop on first hook", result.HookExecutions)
	}
	if len(stateMachine.events) != 1 || stateMachine.events[0].Type != testEvent {
		t.Fatalf("state machine events = %#v, want one %s", stateMachine.events, testEvent)
	}
}

func TestHookPredicateRunsOnlyWhenNeeded(t *testing.T) {
	registry := newTestRegistry(t, Definition{Type: testEvent})
	stateMachine := &recordingStateMachine{}
	dispatcher := newTestDispatcher(t, registry, stateMachine)
	var calls int

	registerTestHook(t, dispatcher, HookSpec{
		Name:      "llm-only",
		EventType: testEvent,
		Level:     HookLevelNormal,
		When: func(_ context.Context, event Event) bool {
			return event.Metadata["scope"] == "llm"
		},
		Handle: func(context.Context, Event) (HookResult, error) {
			calls++
			return Continue(), nil
		},
	})

	if _, err := dispatcher.Emit(context.Background(), mustNewTestEvent(t, registry, testEvent)); err != nil {
		t.Fatalf("first Emit returned error: %v", err)
	}
	if _, err := dispatcher.Emit(context.Background(), mustNewTestEvent(t, registry, testEvent, WithMetadataValue("scope", "llm"))); err != nil {
		t.Fatalf("second Emit returned error: %v", err)
	}

	if calls != 1 {
		t.Fatalf("hook calls = %d, want 1", calls)
	}
	if len(stateMachine.events) != 2 {
		t.Fatalf("state machine events = %d, want 2", len(stateMachine.events))
	}
}

func TestContextEmitterUsesConfiguredDispatcher(t *testing.T) {
	registry := newTestRegistry(t, Definition{Type: testEvent})
	stateMachine := &recordingStateMachine{}
	dispatcher := newTestDispatcher(t, registry, stateMachine)
	ctx := WithDispatcher(context.Background(), dispatcher)

	result, err := EmitNew(ctx, testEvent, map[string]string{"ok": "true"})
	if err != nil {
		t.Fatalf("EmitNew returned error: %v", err)
	}

	if !result.Delivered || len(stateMachine.events) != 1 {
		t.Fatalf("delivered = %v events = %#v, want one delivered event", result.Delivered, stateMachine.events)
	}
}

func TestHookErrorOnRequiredDeliveryStillReachesStateMachine(t *testing.T) {
	registry := newTestRegistry(t, Definition{
		Type:     testEvent,
		Delivery: DeliveryMustReachStateMachine,
	})
	stateMachine := &recordingStateMachine{}
	dispatcher := newTestDispatcher(t, registry, stateMachine)

	registerTestHook(t, dispatcher, HookSpec{
		Name:      "failing-observer",
		EventType: testEvent,
		Level:     HookLevelNormal,
		Handle: func(context.Context, Event) (HookResult, error) {
			return Continue(), errors.New("boom")
		},
	})

	result, err := dispatcher.Emit(context.Background(), mustNewTestEvent(t, registry, testEvent))
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Emit error = %v, want hook error", err)
	}
	if !result.Delivered || len(stateMachine.events) != 1 {
		t.Fatalf("dispatch result = %#v events = %#v, want delivered despite hook error", result, stateMachine.events)
	}
}

func newTestRegistry(t *testing.T, definitions ...Definition) *Registry {
	t.Helper()

	registry, err := NewRegistry(definitions...)
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	return registry
}

func newTestDispatcher(t *testing.T, registry *Registry, stateMachine StateMachine) *Dispatcher {
	t.Helper()

	dispatcher, err := NewDispatcher(registry, stateMachine)
	if err != nil {
		t.Fatalf("NewDispatcher returned error: %v", err)
	}
	return dispatcher
}

func registerTestHook(t *testing.T, dispatcher *Dispatcher, spec HookSpec) {
	t.Helper()

	if err := dispatcher.RegisterHook(spec); err != nil {
		t.Fatalf("RegisterHook(%s) returned error: %v", spec.Name, err)
	}
}

func mustNewTestEvent(t *testing.T, registry *Registry, eventType Type, options ...EventOption) Event {
	t.Helper()

	event, err := registry.NewEvent(eventType, nil, options...)
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	return event
}

type recordingStateMachine struct {
	events []Event
}

func (s *recordingStateMachine) HandleEvent(_ context.Context, event Event) error {
	s.events = append(s.events, event.Clone())
	return nil
}

package state

import (
	"context"
	"testing"

	runtimeevent "agent/internal/event"
)

func TestMachineAdvanceStoresEventUpdatesStateAndPersistsEffect(t *testing.T) {
	ctx := context.Background()
	states := NewMemoryStateStore()
	events := NewMemoryEventStore()
	effects := NewMemoryEffectStore()
	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})

	initial := NewRunState("run_1", 2)
	if err := states.Save(ctx, initial); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	machine := NewMachine(states, events, effects, registry)
	started := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)
	result, err := machine.Advance(ctx, started)
	if err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}
	if result.RunID != "run_1" || len(result.Effects) != 1 || result.Effects[0].Type != EffectCallModel {
		t.Fatalf("advance result = %#v, want one model.call effect", result)
	}

	storedState, err := states.Load(ctx, "run_1")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if storedState.Phase != PhaseWaiting || storedState.Waiting == nil || storedState.Waiting.Reason != "model_result" {
		t.Fatalf("state = %#v, want waiting model_result", storedState)
	}
	if storedState.LastEventID != started.ID {
		t.Fatalf("LastEventID = %q, want %q", storedState.LastEventID, started.ID)
	}

	storedEvents, err := events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if !hasEvent(storedEvents, started.ID, runtimeevent.EventRunStarted) ||
		!hasEventType(storedEvents, runtimeevent.EventStateChanged) ||
		!hasEventType(storedEvents, runtimeevent.EventContextPersisted) {
		t.Fatalf("events = %#v, want started, StateChanged, and ContextPersisted", storedEvents)
	}

	storedEffects, err := effects.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(storedEffects) != 1 || storedEffects[0].Effect.Type != EffectCallModel {
		t.Fatalf("effects = %#v, want one model.call effect", storedEffects)
	}
}

func TestMachineDispatchOnlyAdvancesAndPersistsEffects(t *testing.T) {
	ctx := context.Background()
	states := NewMemoryStateStore()
	events := NewMemoryEventStore()
	effects := NewMemoryEffectStore()
	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})

	if err := states.Save(ctx, NewRunState("run_1", 2)); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	machine := NewMachine(states, events, effects, registry)
	if err := machine.Dispatch(ctx, mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}

	storedState, err := states.Load(ctx, "run_1")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if storedState.Phase != PhaseWaiting || storedState.IsTerminal() {
		t.Fatalf("state = %#v, want non-terminal waiting", storedState)
	}

	storedEvents, err := events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if !hasEventType(storedEvents, runtimeevent.EventRunStarted) ||
		!hasEventType(storedEvents, runtimeevent.EventStateChanged) ||
		!hasEventType(storedEvents, runtimeevent.EventContextPersisted) {
		t.Fatalf("events = %#v, want original and observability events", storedEvents)
	}
	storedEffects, err := effects.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(storedEffects) != 1 || storedEffects[0].Status != EffectStatusPending {
		t.Fatalf("effects = %#v, want one pending effect", storedEffects)
	}
}

func TestMachineAdvanceSkipsAlreadyStoredEvent(t *testing.T) {
	ctx := context.Background()
	states := NewMemoryStateStore()
	events := NewMemoryEventStore()
	effects := NewMemoryEffectStore()
	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})

	if err := states.Save(ctx, NewRunState("run_1", 2)); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	machine := NewMachine(states, events, effects, registry)
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)

	first, err := machine.Advance(ctx, event)
	if err != nil {
		t.Fatalf("first Advance returned error: %v", err)
	}
	if len(first.Effects) != 1 || first.Effects[0].Type != EffectCallModel {
		t.Fatalf("first effects = %#v, want model.call", first.Effects)
	}

	second, err := machine.Advance(ctx, event)
	if err != nil {
		t.Fatalf("second Advance returned error: %v", err)
	}
	if len(second.Effects) != 0 {
		t.Fatalf("second effects = %#v, want none for duplicate event", second.Effects)
	}

	storedEvents, err := events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if !hasEvent(storedEvents, event.ID, runtimeevent.EventRunStarted) ||
		!hasEventType(storedEvents, runtimeevent.EventStateChanged) ||
		!hasEventType(storedEvents, runtimeevent.EventContextPersisted) {
		t.Fatalf("events = %#v, want one driving event with observability events", storedEvents)
	}
	storedEffects, err := effects.ListPending(ctx, "run_1")
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	if len(storedEffects) != 1 {
		t.Fatalf("effects = %d, want one stored effect", len(storedEffects))
	}
}

func hasEvent(events []runtimeevent.Event, id string, eventType runtimeevent.Type) bool {
	for _, event := range events {
		if event.ID == id && event.Type == eventType {
			return true
		}
	}
	return false
}

func hasEventType(events []runtimeevent.Event, eventType runtimeevent.Type) bool {
	for _, event := range events {
		if event.Type == eventType {
			return true
		}
	}
	return false
}

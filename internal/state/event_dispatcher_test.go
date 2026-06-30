package state

import (
	"context"
	"testing"

	runtimeevent "agent/internal/event"
)

func TestMachineConsumesRuntimeEventDispatcherEvents(t *testing.T) {
	ctx := context.Background()
	states := NewMemoryStateStore()
	events := NewMemoryEventStore()
	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})
	executor := &fakeExecutor{}

	initial := NewRunState("run_1", 2)
	if err := states.Save(ctx, initial); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	machine := NewMachine(states, events, registry, executor)
	dispatcher, err := runtimeevent.NewDispatcher(runtimeevent.DefaultRegistry(), machine)
	if err != nil {
		t.Fatalf("NewDispatcher returned error: %v", err)
	}

	event, err := dispatcher.NewEvent(runtimeevent.EventRunStarted, nil, runtimeevent.WithRunID("run_1"))
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	result, err := dispatcher.Emit(ctx, event)
	if err != nil {
		t.Fatalf("Emit returned error: %v", err)
	}

	if !result.Delivered {
		t.Fatalf("Delivered = false, want true")
	}
	storedState, err := states.Load(ctx, "run_1")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if storedState.Phase != PhaseRunning {
		t.Fatalf("Phase = %q, want %q", storedState.Phase, PhaseRunning)
	}
	if storedState.LastEventID != event.ID {
		t.Fatalf("LastEventID = %q, want %q", storedState.LastEventID, event.ID)
	}
	if len(executor.effects) != 1 || executor.effects[0].Type != EffectCallModel {
		t.Fatalf("effects = %#v, want one model.call effect", executor.effects)
	}
}

func TestCoreReducerConsumesRuntimeEventTypesDirectly(t *testing.T) {
	reducer := CoreRunReducer{}
	st := NewRunState("run_1", 2)
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)

	if !reducer.Match(context.Background(), st, event) {
		t.Fatal("CoreRunReducer did not match llm RunStarted event")
	}
}

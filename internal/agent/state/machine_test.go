package state

import (
	"context"
	"errors"
	"testing"

	runtimeevent "agent/internal/event"
)

func TestMachineDispatchStoresEventUpdatesStateAndExecutesEffect(t *testing.T) {
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
	started := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)
	if err := machine.Dispatch(ctx, started); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}

	storedState, err := states.Load(ctx, "run_1")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if storedState.Phase != PhaseRunning {
		t.Fatalf("Phase = %q, want %q", storedState.Phase, PhaseRunning)
	}
	if storedState.LastEventID != started.ID {
		t.Fatalf("LastEventID = %q, want %q", storedState.LastEventID, started.ID)
	}

	storedEvents, err := events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(storedEvents) != 1 || storedEvents[0].ID != started.ID {
		t.Fatalf("events = %#v, want started event", storedEvents)
	}

	if len(executor.effects) != 1 || executor.effects[0].Type != EffectCallModel {
		t.Fatalf("effects = %#v, want one model.call effect", executor.effects)
	}
}

func TestMachineDispatchUsesExecutorEvents(t *testing.T) {
	ctx := context.Background()
	states := NewMemoryStateStore()
	events := NewMemoryEventStore()
	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})
	executor := &fakeExecutor{
		nextEvents: []runtimeevent.Event{
			mustRuntimeEvent(t, "run_1", runtimeevent.EventRunCompleted, nil),
		},
	}

	if err := states.Save(ctx, NewRunState("run_1", 2)); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	machine := NewMachine(states, events, registry, executor)
	if err := machine.Dispatch(ctx, mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}

	storedState, err := states.Load(ctx, "run_1")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if storedState.Phase != PhaseCompleted {
		t.Fatalf("Phase = %q, want %q", storedState.Phase, PhaseCompleted)
	}

	storedEvents, err := events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(storedEvents) != 2 {
		t.Fatalf("events = %d, want 2: %#v", len(storedEvents), storedEvents)
	}
}

func TestMachineDispatchRecordsEffectFailureEvent(t *testing.T) {
	ctx := context.Background()
	states := NewMemoryStateStore()
	events := NewMemoryEventStore()
	registry := NewReducerRegistry()
	registry.Register(CoreRunReducer{})
	executor := &fakeExecutor{err: errors.New("executor failed")}

	if err := states.Save(ctx, NewRunState("run_1", 2)); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	machine := NewMachine(states, events, registry, executor)
	if err := machine.Dispatch(ctx, mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}

	storedEvents, err := events.List(ctx, "run_1")
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}
	if len(storedEvents) != 2 {
		t.Fatalf("events = %d, want 2: %#v", len(storedEvents), storedEvents)
	}
	if storedEvents[1].Type != runtimeevent.EventEffectFailed {
		t.Fatalf("second event type = %q, want %q", storedEvents[1].Type, runtimeevent.EventEffectFailed)
	}
}

type fakeExecutor struct {
	effects    []Effect
	nextEvents []runtimeevent.Event
	err        error
}

func (f *fakeExecutor) Execute(ctx context.Context, effect Effect) ([]runtimeevent.Event, error) {
	f.effects = append(f.effects, effect)
	if f.err != nil {
		return nil, f.err
	}
	return append([]runtimeevent.Event(nil), f.nextEvents...), nil
}

package state

import (
	"context"
	"testing"

	runtimeevent "agent/internal/event"
)

func TestReducerRegistryUsesRegisteredReducer(t *testing.T) {
	registry := NewReducerRegistry()
	registry.Register(testReducer{
		match: runtimeevent.Type("TestMatched"),
		reduce: func(s RunState, event runtimeevent.Event) (RunState, []Effect, error) {
			s.Phase = PhaseRunning
			return s, []Effect{NewEffect(s.RunID, EffectNoop)}, nil
		},
	})

	st := NewRunState("run_1", 1)
	event := runtimeevent.Event{ID: "event_1", RunID: "run_1", Type: runtimeevent.Type("TestMatched")}

	next, effects, err := registry.Reduce(context.Background(), st, event)
	if err != nil {
		t.Fatalf("Reduce returned error: %v", err)
	}
	if next.Phase != PhaseRunning {
		t.Fatalf("Phase = %q, want %q", next.Phase, PhaseRunning)
	}
	if next.LastEventID != event.ID {
		t.Fatalf("LastEventID = %q, want %q", next.LastEventID, event.ID)
	}
	if len(effects) != 1 || effects[0].Type != EffectNoop {
		t.Fatalf("effects = %#v, want one noop effect", effects)
	}
}

func TestReducerRegistryIgnoresUnknownEvent(t *testing.T) {
	registry := NewReducerRegistry()
	st := NewRunState("run_1", 1)
	event := runtimeevent.Event{ID: "event_1", RunID: "run_1", Type: runtimeevent.Type("UnknownEvent")}

	next, effects, err := registry.Reduce(context.Background(), st, event)
	if err != nil {
		t.Fatalf("Reduce returned error: %v", err)
	}
	if next.Phase != st.Phase {
		t.Fatalf("Phase = %q, want %q", next.Phase, st.Phase)
	}
	if next.LastEventID != "" {
		t.Fatalf("LastEventID = %q, want empty", next.LastEventID)
	}
	if len(effects) != 0 {
		t.Fatalf("effects = %#v, want none", effects)
	}
}

type testReducer struct {
	match  runtimeevent.Type
	reduce func(RunState, runtimeevent.Event) (RunState, []Effect, error)
}

func (r testReducer) Match(ctx context.Context, st RunState, event runtimeevent.Event) bool {
	return event.Type == r.match
}

func (r testReducer) Reduce(ctx context.Context, st RunState, event runtimeevent.Event) (RunState, []Effect, error) {
	return r.reduce(st, event)
}

package state

import (
	"encoding/json"
	"testing"

	runtimeevent "agent/internal/event"
)

func TestNewRunStateDefaults(t *testing.T) {
	st := NewRunState("run_1", 5)

	if st.SchemaVersion != CurrentSchemaVersion {
		t.Fatalf("SchemaVersion = %d, want %d", st.SchemaVersion, CurrentSchemaVersion)
	}
	if st.RunID != "run_1" {
		t.Fatalf("RunID = %q, want run_1", st.RunID)
	}
	if st.Phase != PhaseIdle {
		t.Fatalf("Phase = %q, want %q", st.Phase, PhaseIdle)
	}
	if st.MaxSteps != 5 {
		t.Fatalf("MaxSteps = %d, want 5", st.MaxSteps)
	}
	if st.Extensions == nil {
		t.Fatal("Extensions = nil, want initialized map")
	}
}

func TestRunStateIsTerminal(t *testing.T) {
	tests := []struct {
		name string
		st   RunState
		want bool
	}{
		{name: "idle", st: RunState{Phase: PhaseIdle}, want: false},
		{name: "running", st: RunState{Phase: PhaseRunning}, want: false},
		{name: "waiting", st: RunState{Phase: PhaseWaiting}, want: false},
		{name: "completed", st: RunState{Phase: PhaseCompleted}, want: true},
		{name: "failed", st: RunState{Phase: PhaseFailed}, want: true},
		{name: "cancelled", st: RunState{Phase: PhaseCancelled}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.st.IsTerminal(); got != tt.want {
				t.Fatalf("IsTerminal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStateEffectAndRuntimeEventMarshalJSON(t *testing.T) {
	st := NewRunState("run_1", 3)
	event := mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil)
	effect := NewEffect("run_1", EffectCallModel)

	for name, value := range map[string]interface{}{
		"state":  st,
		"event":  event,
		"effect": effect,
	} {
		if _, err := json.Marshal(value); err != nil {
			t.Fatalf("Marshal(%s) returned error: %v", name, err)
		}
	}
}

func mustRuntimeEvent(t *testing.T, runID string, eventType runtimeevent.Type, payload any) runtimeevent.Event {
	t.Helper()

	event, err := runtimeevent.New(eventType, payload, runtimeevent.WithRunID(runID))
	if err != nil {
		t.Fatalf("New(%s) returned error: %v", eventType, err)
	}
	return event
}

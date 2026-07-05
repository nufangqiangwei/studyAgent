package persistence

import (
	"agent/internal/runtime/agents"
	"agent/internal/runtime/agents/builtinagents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
	statemachine2 "agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestFileStorePersistsRuntimeStateSnapshotsAndEvents(t *testing.T) {
	ctx := context.Background()
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}

	state := statemachine2.NewTaskState("task_1", time.Now().UTC())
	state.Phase = statemachine2.PhaseWaitingUserInput
	state.LastEventID = "event_2"
	if err := store.TaskStates().Save(ctx, state); err != nil {
		t.Fatalf("Save task state returned error: %v", err)
	}
	loadedState, ok, err := store.TaskStates().Load(ctx, "task_1")
	if err != nil || !ok {
		t.Fatalf("Load task state ok=%v err=%v, want state", ok, err)
	}
	if loadedState.Phase != statemachine2.PhaseWaitingUserInput || loadedState.LastEventID != "event_2" {
		t.Fatalf("loaded state = %#v, want waiting state", loadedState)
	}

	snapshot := agents.AgentSnapshot{
		TaskID:    "task_1",
		Agent:     builtinagents.AnalyzeAgentName,
		Phase:     agents.BusinessPhaseWaitingUser,
		Input:     "input",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := store.AgentSnapshots().Save(ctx, snapshot); err != nil {
		t.Fatalf("Save snapshot returned error: %v", err)
	}
	loadedSnapshot, ok, err := store.AgentSnapshots().Load(ctx, builtinagents.AnalyzeAgentName, "task_1")
	if err != nil || !ok {
		t.Fatalf("Load snapshot ok=%v err=%v, want snapshot", ok, err)
	}
	if loadedSnapshot.Phase != agents.BusinessPhaseWaitingUser {
		t.Fatalf("loaded snapshot = %#v, want waiting user", loadedSnapshot)
	}

	runtimeSnapshot := NewRuntimeSnapshot(reactor.TaskRuntime{
		TaskID:               "task_1",
		Agent:                builtinagents.AnalyzeAgentName,
		MaxConcurrentEffects: 2,
		Metadata:             map[string]string{"scope": "test"},
	}, time.Now().UTC())
	if err := store.Runtimes().Save(ctx, runtimeSnapshot); err != nil {
		t.Fatalf("Save runtime returned error: %v", err)
	}
	loadedRuntime, ok, err := store.Runtimes().Load(ctx, "task_1")
	if err != nil || !ok {
		t.Fatalf("Load runtime ok=%v err=%v, want runtime", ok, err)
	}
	if loadedRuntime.Agent != builtinagents.AnalyzeAgentName || loadedRuntime.Metadata["scope"] != "test" {
		t.Fatalf("loaded runtime = %#v, want analyze runtime", loadedRuntime)
	}

	event, err := eventbus.NewEvent(statemachine2.TopicTask, statemachine2.EventAgentUserInputRequested, statemachine2.UserInputPayload{
		RequestID: "input_1",
		Prompt:    "continue?",
	}, eventbus.WithEventID("event_2"), eventbus.WithTaskID("task_1"))
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	appended, err := store.Events().Append(ctx, event)
	if err != nil || !appended {
		t.Fatalf("Append event appended=%v err=%v, want append", appended, err)
	}
	appended, err = store.Events().Append(ctx, event)
	if err != nil || appended {
		t.Fatalf("Append duplicate appended=%v err=%v, want no-op", appended, err)
	}
	last, ok, err := store.Events().Last(ctx, "task_1")
	if err != nil || !ok {
		t.Fatalf("Last event ok=%v err=%v, want event", ok, err)
	}
	if last.ID != "event_2" || last.Type != statemachine2.EventAgentUserInputRequested {
		t.Fatalf("last event = %#v, want event_2", last)
	}

	var payload statemachine2.UserInputPayload
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.RequestID != "input_1" {
		t.Fatalf("payload = %#v, want input_1", payload)
	}
}

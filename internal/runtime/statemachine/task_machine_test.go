package statemachine

import (
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTaskStateMachineStartsTaskAndInitializesAgentFlow(t *testing.T) {
	machine, store := newTestMachine(t)

	result, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventTaskStartRequested, "task_1", TaskStartPayload{
		Agent: CodeAgentName,
		Input: "fix tests",
	}))
	if err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}

	state := mustState(t, store, "task_1")
	if state.Phase != PhaseRunning || state.Agent.Name != CodeAgentName || state.Agent.Phase != CodePhaseInspectRepo {
		t.Fatalf("state = %#v, want Running code InspectRepo", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectAgentStart {
		t.Fatalf("effects = %#v, want agent.start", result.Effects)
	}
	if len(state.Lifecycle) != 1 || state.Lifecycle[0].PreviousPhase != PhaseCreated || state.Lifecycle[0].NextPhase != PhaseRunning {
		t.Fatalf("lifecycle = %#v, want Created -> Running", state.Lifecycle)
	}
}

func TestTaskStateMachineToolRequestAndCompletion(t *testing.T) {
	machine, store := newTestMachine(t)
	startTask(t, machine, "task_1")

	request := ToolCallPayload{
		ToolCallID: "tool_1",
		ToolName:   "shell",
		Arguments:  json.RawMessage(`{"cmd":"go test ./..."}`),
	}
	result, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventAgentToolRequested, "task_1", request))
	if err != nil {
		t.Fatalf("tool request returned error: %v", err)
	}
	state := mustState(t, store, "task_1")
	if state.Phase != PhaseWaitingTool || state.PendingTool == nil || state.PendingTool.ToolCallID != "tool_1" {
		t.Fatalf("state = %#v, want WaitingTool with pending tool_1", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectToolDispatch {
		t.Fatalf("effects = %#v, want tool.dispatch", result.Effects)
	}

	result, err = machine.HandleEvent(context.Background(), mustTaskEvent(t, EventToolCompleted, "task_1", ToolCallPayload{
		ToolCallID: "tool_1",
		ToolName:   "shell",
		Result:     json.RawMessage(`{"exit_code":0}`),
	}))
	if err != nil {
		t.Fatalf("tool completed returned error: %v", err)
	}
	state = mustState(t, store, "task_1")
	if state.Phase != PhaseRunning || state.PendingTool != nil {
		t.Fatalf("state = %#v, want Running with no pending tool", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectAgentResume {
		t.Fatalf("effects = %#v, want agent.resume", result.Effects)
	}
}

func TestTaskStateMachineModelRequestAndResponse(t *testing.T) {
	machine, store := newTestMachine(t)
	startTask(t, machine, "task_1")

	request := ModelCallPayload{
		ModelCallID: "model_1",
		Agent:       CodeAgentName,
		Request:     json.RawMessage(`{"task_id":"task_1","agent":"code"}`),
	}
	result, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventAgentModelRequested, "task_1", request))
	if err != nil {
		t.Fatalf("model request returned error: %v", err)
	}
	state := mustState(t, store, "task_1")
	if state.Phase != PhaseWaitingModel || state.PendingModel == nil || state.PendingModel.ModelCallID != "model_1" {
		t.Fatalf("state = %#v, want WaitingModel with pending model_1", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectModelCall {
		t.Fatalf("effects = %#v, want model.call", result.Effects)
	}

	result, err = machine.HandleEvent(context.Background(), mustTaskEvent(t, EventModelResponseReceived, "task_1", ModelCallPayload{
		ModelCallID: "model_1",
		Response:    json.RawMessage(`{"content":"{}"}`),
	}))
	if err != nil {
		t.Fatalf("model response returned error: %v", err)
	}
	state = mustState(t, store, "task_1")
	if state.Phase != PhaseRunning || state.PendingModel != nil {
		t.Fatalf("state = %#v, want Running with no pending model", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectAgentResume {
		t.Fatalf("effects = %#v, want agent.resume", result.Effects)
	}
}

func TestTaskStateMachineWaitsForUserInputAndSubAgent(t *testing.T) {
	machine, store := newTestMachine(t)
	startTask(t, machine, "task_1")

	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventAgentUserInputRequested, "task_1", UserInputPayload{
		RequestID: "input_1",
		Prompt:    "continue?",
	})); err != nil {
		t.Fatalf("user input request returned error: %v", err)
	}
	state := mustState(t, store, "task_1")
	if state.Phase != PhaseWaitingUserInput || state.PendingUserInput == nil || state.PendingUserInput.RequestID != "input_1" {
		t.Fatalf("state = %#v, want WaitingUserInput input_1", state)
	}
	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventUserInputReceived, "task_1", UserInputPayload{
		RequestID: "input_1",
		Answer:    "yes",
	})); err != nil {
		t.Fatalf("user input received returned error: %v", err)
	}

	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventAgentSubAgentRequested, "task_1", SubAgentPayload{
		SubTaskID: "sub_1",
		Agent:     "review",
		Input:     "check change",
	})); err != nil {
		t.Fatalf("sub-agent request returned error: %v", err)
	}
	state = mustState(t, store, "task_1")
	if state.Phase != PhaseWaitingSubAgent || state.PendingSubAgent == nil || state.PendingSubAgent.SubTaskID != "sub_1" {
		t.Fatalf("state = %#v, want WaitingSubAgent sub_1", state)
	}
	result, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventSubAgentCompleted, "task_1", SubAgentPayload{
		SubTaskID: "sub_1",
		Result:    json.RawMessage(`{"ok":true}`),
	}))
	if err != nil {
		t.Fatalf("sub-agent completed returned error: %v", err)
	}
	state = mustState(t, store, "task_1")
	if state.Phase != PhaseRunning || state.PendingSubAgent != nil {
		t.Fatalf("state = %#v, want Running with no pending sub-agent", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectAgentResume {
		t.Fatalf("effects = %#v, want agent.resume", result.Effects)
	}
}

func TestTaskStateMachineAcceptsUserGuidanceWhileRunning(t *testing.T) {
	machine, store := newTestMachine(t)
	startTask(t, machine, "task_1")

	result, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventUserInputReceived, "task_1", UserInputPayload{
		RequestID: "cli_1",
		Answer:    "focus on tests",
	}))
	if err != nil {
		t.Fatalf("user guidance returned error: %v", err)
	}
	state := mustState(t, store, "task_1")
	if state.Phase != PhaseRunning || state.PendingUserInput != nil {
		t.Fatalf("state = %#v, want Running with no pending user input", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectAgentResume {
		t.Fatalf("effects = %#v, want agent.resume", result.Effects)
	}
}

func TestTaskStateMachineCompletesAndRejectsLaterEvents(t *testing.T) {
	machine, store := newTestMachine(t)
	startTask(t, machine, "task_1")

	result, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventAgentCompleted, "task_1", AgentCompletedPayload{
		Result: json.RawMessage(`{"summary":"done"}`),
	}))
	if err != nil {
		t.Fatalf("agent completed returned error: %v", err)
	}
	state := mustState(t, store, "task_1")
	if state.Phase != PhaseCompleted || state.CompletedAt == nil || string(state.Result) != `{"summary":"done"}` {
		t.Fatalf("state = %#v, want Completed with result", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != EffectEmitTaskCompleted {
		t.Fatalf("effects = %#v, want task.completed.emit", result.Effects)
	}

	_, err = machine.HandleEvent(context.Background(), mustTaskEvent(t, EventToolCompleted, "task_1", ToolCallPayload{ToolCallID: "late"}))
	var illegal *IllegalEventError
	if !errors.As(err, &illegal) {
		t.Fatalf("late event error = %v, want IllegalEventError", err)
	}
	unchanged := mustState(t, store, "task_1")
	if unchanged.Phase != PhaseCompleted || unchanged.LastEventID != state.LastEventID {
		t.Fatalf("state changed after illegal event: %#v", unchanged)
	}
}

func TestTaskStateMachineRunsCodeAgentFlowByTransitionTable(t *testing.T) {
	machine, store := newTestMachine(t)
	startTask(t, machine, "task_1")

	result, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventCodeInspectionCompleted, "task_1", nil))
	if err != nil {
		t.Fatalf("inspection completed returned error: %v", err)
	}
	state := mustState(t, store, "task_1")
	if state.Agent.Phase != CodePhaseEditCode {
		t.Fatalf("agent phase = %s, want EditCode", state.Agent.Phase)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectAgentResume {
		t.Fatalf("effects = %#v, want agent.resume", result.Effects)
	}

	_, err = machine.HandleEvent(context.Background(), mustTaskEvent(t, EventCodeTestsFailed, "task_1", nil))
	var illegal *IllegalEventError
	if !errors.As(err, &illegal) {
		t.Fatalf("tests failed from EditCode error = %v, want IllegalEventError", err)
	}

	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventCodeEditCompleted, "task_1", nil)); err != nil {
		t.Fatalf("edit completed returned error: %v", err)
	}
	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventCodeTestsFailed, "task_1", nil)); err != nil {
		t.Fatalf("tests failed from RunTests returned error: %v", err)
	}
	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventCodeFixCompleted, "task_1", nil)); err != nil {
		t.Fatalf("fix completed returned error: %v", err)
	}
	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventCodeTestsPassed, "task_1", nil)); err != nil {
		t.Fatalf("tests passed returned error: %v", err)
	}
	state = mustState(t, store, "task_1")
	if state.Agent.Phase != CodePhaseReport {
		t.Fatalf("agent phase = %s, want Report", state.Agent.Phase)
	}
}

func TestTaskStateMachineFailureThresholdFailsTask(t *testing.T) {
	machine, store := newTestMachineWithOptions(t, WithMaxFailures(1))
	startTask(t, machine, "task_1")
	requestTool(t, machine, "task_1", "tool_1")

	result, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventToolFailed, "task_1", ToolCallPayload{
		ToolCallID: "tool_1",
		Error:      "first failure",
	}))
	if err != nil {
		t.Fatalf("first tool failure returned error: %v", err)
	}
	state := mustState(t, store, "task_1")
	if state.Phase != PhaseRunning || state.FailureCount != 1 || state.LastError == nil {
		t.Fatalf("state = %#v, want first failure recover to Running", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != reactor.EffectAgentResume {
		t.Fatalf("effects = %#v, want agent.resume after recoverable failure", result.Effects)
	}

	requestTool(t, machine, "task_1", "tool_2")
	result, err = machine.HandleEvent(context.Background(), mustTaskEvent(t, EventToolFailed, "task_1", ToolCallPayload{
		ToolCallID: "tool_2",
		Error:      "second failure",
	}))
	if err != nil {
		t.Fatalf("second tool failure returned error: %v", err)
	}
	state = mustState(t, store, "task_1")
	if state.Phase != PhaseFailed || state.FailureCount != 2 || state.CompletedAt == nil {
		t.Fatalf("state = %#v, want Failed after threshold exceeded", state)
	}
	if len(result.Effects) != 1 || result.Effects[0].Type != EffectEmitTaskFailed {
		t.Fatalf("effects = %#v, want task.failed.emit", result.Effects)
	}
}

func newTestMachine(t *testing.T) (*TaskStateMachine, *MemoryStateStore) {
	t.Helper()
	return newTestMachineWithOptions(t)
}

func newTestMachineWithOptions(t *testing.T, options ...TaskStateMachineOption) (*TaskStateMachine, *MemoryStateStore) {
	t.Helper()
	store := NewMemoryStateStore()
	flows := NewAgentFlowRegistry()
	codeFlow, err := NewCodeAgentFlow()
	if err != nil {
		t.Fatalf("NewCodeAgentFlow returned error: %v", err)
	}
	if err := flows.Register(CodeAgentName, codeFlow); err != nil {
		t.Fatalf("Register code flow returned error: %v", err)
	}
	base := []TaskStateMachineOption{
		WithStateStore(store),
		WithAgentFlows(flows),
		WithClock(fixedClock(time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC))),
	}
	base = append(base, options...)
	machine, err := NewTaskStateMachine(base...)
	if err != nil {
		t.Fatalf("NewTaskStateMachine returned error: %v", err)
	}
	return machine, store
}

func fixedClock(now time.Time) Clock {
	return func() time.Time {
		return now
	}
}

func startTask(t *testing.T, machine *TaskStateMachine, taskID string) {
	t.Helper()
	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventTaskStartRequested, taskID, TaskStartPayload{Agent: CodeAgentName})); err != nil {
		t.Fatalf("start task returned error: %v", err)
	}
}

func requestTool(t *testing.T, machine *TaskStateMachine, taskID string, toolCallID string) {
	t.Helper()
	if _, err := machine.HandleEvent(context.Background(), mustTaskEvent(t, EventAgentToolRequested, taskID, ToolCallPayload{
		ToolCallID: toolCallID,
		ToolName:   "shell",
	})); err != nil {
		t.Fatalf("request tool returned error: %v", err)
	}
}

func mustTaskEvent(t *testing.T, eventType eventbus.EventType, taskID string, payload any) eventbus.Event {
	t.Helper()
	event, err := eventbus.NewEvent(TopicTask, eventType, payload, eventbus.WithTaskID(taskID))
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	return event
}

func mustState(t *testing.T, store *MemoryStateStore, taskID string) TaskState {
	t.Helper()
	state, ok, err := store.Load(context.Background(), taskID)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !ok {
		t.Fatalf("state %s not found", taskID)
	}
	return state
}

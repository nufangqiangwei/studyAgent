package task

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/agent"
	"context"
	"encoding/json"
	"testing"
	"time"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }

func TestTaskLifecycleDelegatesExecutionAndCompletes(t *testing.T) {
	now := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	clock := &testClock{now: now}
	svc := &taskService{address: "task.42", instanceID: "task-42", clock: clock}
	state := mustInitialState(t, svc)

	create := CreateRequest{TaskID: "42", GoalID: "goal-1", Title: "Implement task", Input: "finish the work"}
	decision := mustHandle(t, svc, state, ownerMessage(CreateMessageType, "create-1", create))
	if len(decision.Events) != 1 || decision.Events[0].Type != taskCreatedEvent || decision.Reply == nil {
		t.Fatalf("create decision=%#v", decision)
	}
	state = mustApply(t, svc, state, decision)

	clock.now = now.Add(time.Minute)
	decision = mustHandle(t, svc, state, ownerMessage(MarkReadyMessageType, "ready-1", struct{}{}))
	state = mustApply(t, svc, state, decision)

	clock.now = now.Add(2 * time.Minute)
	decision = mustHandle(t, svc, state, ownerMessage(AssignMessageType, "assign-1", AssignRequest{AgentAddress: "agent.main"}))
	state = mustApply(t, svc, state, decision)

	clock.now = now.Add(3 * time.Minute)
	decision = mustHandle(t, svc, state, ownerMessage(StartMessageType, "start-1", struct{}{}))
	if len(decision.Events) != 1 || len(decision.Outgoing) != 1 || decision.Reply == nil {
		t.Fatalf("start decision=%#v", decision)
	}
	execute := decision.Outgoing[0]
	if execute.Kind != contract.MessageCommand || execute.Type != agent.ExecuteMessageType || execute.To != "agent.main" || execute.ReplyTo != "task.42" {
		t.Fatalf("execute=%#v", execute)
	}
	var request agent.ExecuteRequest
	if err := json.Unmarshal(execute.Payload, &request); err != nil || request.RunID != "42/attempt/1" || request.Input != "finish the work" {
		t.Fatalf("execute request=%#v err=%v", request, err)
	}
	state = mustApply(t, svc, state, decision)
	started := mustDecodeTask(t, state)
	if started.Phase != PhaseRunning || started.Attempt != 1 || started.ActiveRunID != request.RunID {
		t.Fatalf("started=%#v", started)
	}

	clock.now = now.Add(4 * time.Minute)
	waitingMessage := contract.Message{
		ID: "waiting-1", Kind: contract.MessageEvent, Type: ExecutionWaitingMessageType, Version: ProtocolVersion,
		From: "agent.main", To: "task.42", CorrelationID: request.RunID,
		Payload: mustJSON(t, ExecutionWaiting{TaskID: "42", RunID: request.RunID, Kind: WaitCapability, References: []string{"call-1"}}),
	}
	decision = mustHandle(t, svc, state, waitingMessage)
	if len(decision.Events) != 1 || decision.Events[0].Type != taskWaitingEvent || len(decision.Outgoing) != 1 {
		t.Fatalf("waiting decision=%#v", decision)
	}
	state = mustApply(t, svc, state, decision)
	if waiting := mustDecodeTask(t, state); waiting.Phase != PhaseWaiting || waiting.Wait == nil || waiting.Wait.References[0] != "call-1" {
		t.Fatalf("waiting=%#v", waiting)
	}

	clock.now = now.Add(5 * time.Minute)
	resumedMessage := contract.Message{
		ID: "resumed-1", Kind: contract.MessageEvent, Type: ExecutionResumedMessageType, Version: ProtocolVersion,
		From: "agent.main", To: "task.42", CorrelationID: request.RunID,
		Payload: mustJSON(t, ExecutionResumed{TaskID: "42", RunID: request.RunID}),
	}
	decision = mustHandle(t, svc, state, resumedMessage)
	state = mustApply(t, svc, state, decision)
	if resumed := mustDecodeTask(t, state); resumed.Phase != PhaseRunning || resumed.Wait != nil {
		t.Fatalf("resumed=%#v", resumed)
	}

	clock.now = now.Add(6 * time.Minute)
	output := contract.ArtifactRef{Store: "test", Key: "tasks/42/result.txt", ContentType: "text/plain", Size: 4}
	completedMessage := contract.Message{
		ID: "agent-completed-1", Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion,
		From: "agent.main", To: "task.42", CorrelationID: request.RunID,
		Payload: mustJSON(t, agent.ExecuteResult{RunID: request.RunID, Phase: agent.PhaseCompleted, Output: &output, Turns: 2}),
	}
	decision = mustHandle(t, svc, state, completedMessage)
	if len(decision.Events) != 1 || decision.Events[0].Type != taskCompletedEvent || len(decision.Outgoing) != 1 || decision.Outgoing[0].Type != CompletedEventType {
		t.Fatalf("completion decision=%#v", decision)
	}
	state = mustApply(t, svc, state, decision)
	completed := mustDecodeTask(t, state)
	if completed.Phase != PhaseCompleted || completed.ResultRef == nil || completed.ResultRef.Key != output.Key || completed.CompletedAt == nil {
		t.Fatalf("completed=%#v", completed)
	}
}

func TestTaskFailureRetryRejectsLateRunAndStartsNewAttempt(t *testing.T) {
	now := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	clock := &testClock{now: now}
	svc := &taskService{address: "task.retry", instanceID: "task-retry", clock: clock}
	state := readyAssignedTask(t, svc, "retry", "agent.main")

	decision := mustHandle(t, svc, state, ownerMessage(StartMessageType, "start-1", struct{}{}))
	state = mustApply(t, svc, state, decision)
	firstRun := mustDecodeTask(t, state).ActiveRunID

	clock.now = now.Add(time.Minute)
	failed := contract.Message{
		ID: "failed-1", Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion,
		From: "agent.main", CorrelationID: firstRun,
		Payload: mustJSON(t, agent.ExecuteResult{RunID: firstRun, Phase: agent.PhaseFailed, ErrorCode: "agent_model_failed", ErrorMessage: "model failed"}),
	}
	decision = mustHandle(t, svc, state, failed)
	state = mustApply(t, svc, state, decision)
	failedTask := mustDecodeTask(t, state)
	if failedTask.Phase != PhaseFailed || failedTask.FailureCount != 1 || failedTask.LastError == nil {
		t.Fatalf("failed task=%#v", failedTask)
	}

	clock.now = now.Add(2 * time.Minute)
	decision = mustHandle(t, svc, state, ownerMessage(RetryMessageType, "retry-1", struct{}{}))
	state = mustApply(t, svc, state, decision)
	decision = mustHandle(t, svc, state, ownerMessage(StartMessageType, "start-2", struct{}{}))
	state = mustApply(t, svc, state, decision)
	secondRun := mustDecodeTask(t, state).ActiveRunID
	if secondRun == firstRun || secondRun != "retry/attempt/2" {
		t.Fatalf("second run=%q first=%q", secondRun, firstRun)
	}

	lateOutput := contract.ArtifactRef{Store: "test", Key: "tasks/retry/late.txt"}
	late := contract.Message{
		ID: "late-1", Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion,
		From: "agent.main", CorrelationID: firstRun,
		Payload: mustJSON(t, agent.ExecuteResult{RunID: firstRun, Phase: agent.PhaseCompleted, Output: &lateOutput}),
	}
	lateDecision := mustHandle(t, svc, state, late)
	if len(lateDecision.Events) != 0 || len(lateDecision.Outgoing) != 0 {
		t.Fatalf("late decision=%#v", lateDecision)
	}
}

func TestRunningTaskCancellationWaitsForAgentTerminalReply(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	clock := &testClock{now: now}
	svc := &taskService{address: "task.cancel", instanceID: "task-cancel", clock: clock}
	state := readyAssignedTask(t, svc, "cancel", "agent.main")
	decision := mustHandle(t, svc, state, ownerMessage(StartMessageType, "start-1", struct{}{}))
	state = mustApply(t, svc, state, decision)
	runID := mustDecodeTask(t, state).ActiveRunID

	clock.now = now.Add(time.Minute)
	decision = mustHandle(t, svc, state, ownerMessage(CancelMessageType, "cancel-1", CancelRequest{ReasonCode: "user_cancelled"}))
	if len(decision.Events) != 1 || decision.Events[0].Type != taskCancelRequestedEvent || len(decision.Outgoing) != 1 || decision.Outgoing[0].Type != agent.CancelMessageType {
		t.Fatalf("cancel decision=%#v", decision)
	}
	state = mustApply(t, svc, state, decision)
	pending := mustDecodeTask(t, state)
	if pending.Phase != PhaseRunning || pending.Cancellation == nil {
		t.Fatalf("pending cancellation=%#v", pending)
	}

	clock.now = now.Add(2 * time.Minute)
	completed := contract.Message{
		ID: "agent-cancelled-1", Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion,
		From: "agent.main", CorrelationID: runID,
		Payload: mustJSON(t, agent.ExecuteResult{RunID: runID, Phase: agent.PhaseCancelled, ErrorCode: "agent_cancelled"}),
	}
	decision = mustHandle(t, svc, state, completed)
	state = mustApply(t, svc, state, decision)
	cancelled := mustDecodeTask(t, state)
	if cancelled.Phase != PhaseCancelled || cancelled.CompletedAt == nil || cancelled.LastError == nil || cancelled.LastError.Code != "agent_cancelled" {
		t.Fatalf("cancelled=%#v", cancelled)
	}
}

func TestReadyTaskCanSuspendResumeAndCancelWithoutStartingAgent(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 30, 0, 0, time.UTC)
	clock := &testClock{now: now}
	svc := &taskService{address: "task.suspend", instanceID: "task-suspend", clock: clock}
	state := readyAssignedTask(t, svc, "suspend", "agent.main")

	decision := mustHandle(t, svc, state, ownerMessage(SuspendMessageType, "suspend-1", SuspendRequest{Reason: "waiting for a maintenance window"}))
	state = mustApply(t, svc, state, decision)
	suspended := mustDecodeTask(t, state)
	if suspended.Phase != PhaseSuspended || suspended.Suspension == nil {
		t.Fatalf("suspended=%#v", suspended)
	}

	clock.now = now.Add(time.Minute)
	decision = mustHandle(t, svc, state, ownerMessage(ResumeMessageType, "resume-1", struct{}{}))
	state = mustApply(t, svc, state, decision)
	resumed := mustDecodeTask(t, state)
	if resumed.Phase != PhaseReady || resumed.Suspension != nil {
		t.Fatalf("resumed=%#v", resumed)
	}

	clock.now = now.Add(2 * time.Minute)
	decision = mustHandle(t, svc, state, ownerMessage(CancelMessageType, "cancel-ready-1", CancelRequest{ReasonCode: "no_longer_needed"}))
	if len(decision.Events) != 1 || decision.Events[0].Type != taskCancelledEvent || len(decision.Outgoing) != 1 || decision.Outgoing[0].Type != CancelledEventType {
		t.Fatalf("direct cancellation decision=%#v", decision)
	}
	state = mustApply(t, svc, state, decision)
	cancelled := mustDecodeTask(t, state)
	if cancelled.Phase != PhaseCancelled || cancelled.Attempt != 0 || cancelled.LastError == nil || cancelled.LastError.Code != "no_longer_needed" {
		t.Fatalf("cancelled=%#v", cancelled)
	}
}

func TestGetIsReadOnlyAndApplyRejectsUnknownEvent(t *testing.T) {
	now := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)
	svc := &taskService{address: "task.query", instanceID: "task-query", clock: &testClock{now: now}}
	state := readyAssignedTask(t, svc, "query", "agent.main")
	decision := mustHandle(t, svc, state, contract.Message{
		ID: "get-1", Kind: contract.MessageQuery, Type: GetMessageType, Version: ProtocolVersion,
		From: "owner.main", ReplyTo: "owner.main", Payload: mustJSON(t, GetRequest{TaskID: "query"}),
	})
	if decision.Reply == nil || len(decision.Events) != 0 || len(decision.Effects) != 0 {
		t.Fatalf("get decision=%#v", decision)
	}
	if _, err := svc.Apply(state, contract.StoredEvent{EventType: "task.state.unknown", EventVersion: ProtocolVersion}); err == nil {
		t.Fatal("expected unknown event to be rejected")
	}
}

func TestCreateReplayIsDeterministicAndOwnerIsEnforced(t *testing.T) {
	now := time.Date(2026, 7, 22, 13, 30, 0, 0, time.UTC)
	svc := &taskService{address: "task.replay", instanceID: "task-replay", clock: &testClock{now: now}}
	initial := mustInitialState(t, svc)
	decision := mustHandle(t, svc, initial, ownerMessage(CreateMessageType, "create-replay", CreateRequest{
		TaskID: "replay", GoalID: "goal-1", Input: "replay this task",
	}))
	first := mustApply(t, svc, initial, decision)
	second := mustApply(t, svc, initial, decision)
	if string(first.Data) != string(second.Data) {
		t.Fatalf("replay differs:\n%s\n%s", first.Data, second.Data)
	}

	unauthorized := ownerMessage(MarkReadyMessageType, "ready-unauthorized", struct{}{})
	unauthorized.From, unauthorized.ReplyTo = "intruder", "intruder"
	rejected := mustHandle(t, svc, first, unauthorized)
	if rejected.Reply == nil || rejected.Reply.Error == nil || rejected.Reply.Error.Code != errAccessDenied || len(rejected.Events) != 0 {
		t.Fatalf("unauthorized decision=%#v", rejected)
	}
}

func TestDefinitionDeclaresVirtualTaskContracts(t *testing.T) {
	definition := Definition(ServiceFactory{clock: &testClock{now: time.Now().UTC()}})
	if definition.Scope != "virtual" || definition.Component != Component || definition.StateSchema != StateSchema {
		t.Fatalf("definition=%#v", definition)
	}
	if len(definition.EffectExecutors) != 0 || len(definition.Dependencies) != 0 {
		t.Fatalf("task definition unexpectedly owns effects or dependencies: %#v", definition)
	}
	assertContract(t, definition.Consumes, contract.MessageReply, agent.CompletedMessageType, agent.ProtocolVersion)
	assertContract(t, definition.Produces, contract.MessageCommand, agent.ExecuteMessageType, agent.ProtocolVersion)
	assertContract(t, definition.Produces, contract.MessageEvent, CompletedEventType, ProtocolVersion)
}

func readyAssignedTask(t *testing.T, svc *taskService, taskID string, assigned contract.ServiceAddress) service.State {
	t.Helper()
	state := mustInitialState(t, svc)
	state = mustApply(t, svc, state, mustHandle(t, svc, state, ownerMessage(CreateMessageType, "create-"+taskID, CreateRequest{
		TaskID: taskID, GoalID: "goal-1", Input: "do the work",
	})))
	state = mustApply(t, svc, state, mustHandle(t, svc, state, ownerMessage(MarkReadyMessageType, "ready-"+taskID, struct{}{})))
	state = mustApply(t, svc, state, mustHandle(t, svc, state, ownerMessage(AssignMessageType, "assign-"+taskID, AssignRequest{AgentAddress: assigned})))
	return state
}

func ownerMessage(messageType contract.MessageType, id string, payload any) contract.Message {
	return contract.Message{
		ID: id, Kind: contract.MessageCommand, Type: messageType, Version: ProtocolVersion,
		From: "owner.main", To: "task.test", ReplyTo: "owner.main", UserID: "user-1", GoalID: "goal-1",
		Payload: mustJSONWithoutTest(payload),
	}
}

func mustInitialState(t *testing.T, svc *taskService) service.State {
	t.Helper()
	state, err := svc.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func mustHandle(t *testing.T, svc *taskService, state service.State, message contract.Message) service.Decision {
	t.Helper()
	decision, err := svc.Handle(context.Background(), state, message)
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func mustApply(t *testing.T, svc *taskService, state service.State, decision service.Decision) service.State {
	t.Helper()
	if len(decision.Events) != 1 {
		t.Fatalf("expected one event, got %#v", decision.Events)
	}
	event := decision.Events[0]
	next, err := svc.Apply(state, contract.StoredEvent{EventType: event.Type, EventVersion: event.Version, Payload: event.Payload})
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func mustDecodeTask(t *testing.T, state service.State) State {
	t.Helper()
	decoded, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Task == nil {
		t.Fatal("task is missing")
	}
	return decoded.Task.Clone()
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mustJSONWithoutTest(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func assertContract(t *testing.T, contracts []building.MessageContract, kind contract.MessageKind, messageType contract.MessageType, version int) {
	t.Helper()
	for _, candidate := range contracts {
		if candidate.Kind == kind && candidate.Type == messageType && candidate.Version == version {
			return
		}
	}
	t.Fatalf("missing contract %s %s v%d in %#v", kind, messageType, version, contracts)
}

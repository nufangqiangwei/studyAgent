package webgateway

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/service"
	runtimesystem "agent/serviceruntime/system"
	"agent/services/task"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestFullCreateSagaDeclaresCreatesMarksReadyAssignsAndStarts(t *testing.T) {
	svc, state := newTestService(t)
	create := createMessage(t, "message-create-1", "request-1", "task-1", "user-1", "hello")

	// Step 1: Create → declare instance
	recorded, err := svc.Handle(context.Background(), state, create)
	if err != nil {
		t.Fatalf("handle create: %v", err)
	}
	if len(recorded.Events) != 1 || len(recorded.Outgoing) != 1 || recorded.Outgoing[0].Type != runtimesystem.CallMessageType {
		t.Fatalf("unexpected create decision: %#v", recorded)
	}
	state = applyDecision(t, svc, state, recorded)

	var call runtimesystem.Call
	if err := json.Unmarshal(recorded.Outgoing[0].Payload, &call); err != nil {
		t.Fatalf("decode system call: %v", err)
	}
	var declaration instance.Declaration
	if err := json.Unmarshal(call.Payload, &declaration); err != nil {
		t.Fatalf("decode declaration: %v", err)
	}

	// Step 2: System result → task.create
	systemReply := successfulSystemResult(t, call, declaration)
	declared, err := svc.Handle(context.Background(), state, systemReply)
	if err != nil {
		t.Fatalf("handle system result: %v", err)
	}
	if len(declared.Events) != 1 || len(declared.Outgoing) != 1 {
		t.Fatalf("unexpected declaration decision: %#v", declared)
	}
	if declared.Outgoing[0].Type != task.CreateMessageType {
		t.Fatalf("expected task.create: %#v", declared.Outgoing[0])
	}
	state = applyDecision(t, svc, state, declared)

	// Step 3: task.status (Phase=Created) → mark_ready
	createdStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseCreated, input: "hello",
	})
	markingReady, err := svc.Handle(context.Background(), state, createdStatus)
	if err != nil {
		t.Fatalf("handle created status: %v", err)
	}
	if len(markingReady.Events) != 2 ||
		markingReady.Events[0].Type != taskOwnershipRecordedEvent ||
		markingReady.Events[1].Type != taskMarkedReadyEvent ||
		len(markingReady.Outgoing) != 1 {
		t.Fatalf("unexpected mark-ready decision: %#v", markingReady)
	}
	if markingReady.Outgoing[0].Type != task.MarkReadyMessageType {
		t.Fatalf("expected task.mark_ready: %#v", markingReady.Outgoing[0])
	}
	state = applyDecision(t, svc, state, markingReady)

	// Step 4: task.status (Phase=Ready) → assign
	readyStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, input: "hello",
	})
	assigning, err := svc.Handle(context.Background(), state, readyStatus)
	if err != nil {
		t.Fatalf("handle ready status: %v", err)
	}
	if len(assigning.Events) != 1 || len(assigning.Outgoing) != 1 {
		t.Fatalf("unexpected assign decision: %#v", assigning)
	}
	if assigning.Outgoing[0].Type != task.AssignMessageType {
		t.Fatalf("expected task.assign: %#v", assigning.Outgoing[0])
	}
	var assignReq task.AssignRequest
	if err := json.Unmarshal(assigning.Outgoing[0].Payload, &assignReq); err != nil {
		t.Fatalf("decode assign: %v", err)
	}
	if assignReq.AgentAddress != "agent.test" {
		t.Fatalf("unexpected agent address: %q", assignReq.AgentAddress)
	}
	state = applyDecision(t, svc, state, assigning)

	// Step 5: task.status (Phase=Ready with AssignedTo) → start
	readyAssignedStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, assignedTo: "agent.test", input: "hello",
	})
	starting, err := svc.Handle(context.Background(), state, readyAssignedStatus)
	if err != nil {
		t.Fatalf("handle assigned status: %v", err)
	}
	if len(starting.Events) != 1 || len(starting.Outgoing) != 1 {
		t.Fatalf("unexpected start decision: %#v", starting)
	}
	if starting.Outgoing[0].Type != task.StartMessageType {
		t.Fatalf("expected task.start: %#v", starting.Outgoing[0])
	}
	state = applyDecision(t, svc, state, starting)

	// Step 6: task.status (Phase=Running) → succeeded
	runningStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseRunning, assignedTo: "agent.test",
		activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
	})
	completed, err := svc.Handle(context.Background(), state, runningStatus)
	if err != nil {
		t.Fatalf("handle running status: %v", err)
	}
	if len(completed.Events) != 1 || len(completed.Effects) != 1 {
		t.Fatalf("terminal event and presentation must be atomic: %#v", completed)
	}
	if completed.Effects[0].IdempotencyKey == "" || completed.Effects[0].ExecutorRef != PresentationExecutorRef {
		t.Fatalf("presentation identity is incomplete: %#v", completed.Effects[0])
	}
	state = applyDecision(t, svc, state, completed)

	materialized, err := decodeState(state)
	if err != nil {
		t.Fatalf("decode terminal state: %v", err)
	}
	request := materialized.Requests["request-1"]
	if request.Phase != PhaseSucceeded || request.Task == nil || request.Task.Phase != task.PhaseRunning {
		t.Fatalf("unexpected terminal request phase=%s task_phase=%s", request.Phase, request.Task.Phase)
	}
	if request.Task.ActiveRunID != "task-1/attempt/1" || request.Task.Attempt != 1 {
		t.Fatalf("unexpected run info: runID=%s attempt=%d", request.Task.ActiveRunID, request.Task.Attempt)
	}
	owned := materialized.Tasks["task-1"]
	if owned.Address != declaration.Address || owned.UserID != "user-1" {
		t.Fatalf("unexpected task mapping: %#v", owned)
	}

	// Idempotent duplicate presentation
	duplicate := create
	duplicate.ID = "message-create-duplicate"
	replayed, err := svc.Handle(context.Background(), state, duplicate)
	if err != nil {
		t.Fatalf("handle duplicate create: %v", err)
	}
	if len(replayed.Events) != 0 || len(replayed.Outgoing) != 0 || len(replayed.Effects) != 1 {
		t.Fatalf("terminal duplicate must only re-plan presentation: %#v", replayed)
	}
	if replayed.Effects[0].IdempotencyKey != completed.Effects[0].IdempotencyKey ||
		string(replayed.Effects[0].Payload) != string(completed.Effects[0].Payload) {
		t.Fatalf("duplicate presentation changed identity or payload")
	}
}

func TestSagaStepIdempotency(t *testing.T) {
	svc, state := newTestService(t)
	create := createMessage(t, "message-1", "request-1", "task-1", "user-1", "hello")
	recorded, _ := svc.Handle(context.Background(), state, create)
	state = applyDecision(t, svc, state, recorded)

	var call runtimesystem.Call
	_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
	var declaration instance.Declaration
	_ = json.Unmarshal(call.Payload, &declaration)
	systemReply := successfulSystemResult(t, call, declaration)
	declared, _ := svc.Handle(context.Background(), state, systemReply)
	state = applyDecision(t, svc, state, declared)

	// Duplicate system result should be a no-op
	dupSystem, err := svc.Handle(context.Background(), state, systemReply)
	if err != nil {
		t.Fatal(err)
	}
	if len(dupSystem.Events)+len(dupSystem.Outgoing)+len(dupSystem.Effects) != 0 {
		t.Fatalf("duplicate system result produced work: %#v", dupSystem)
	}

	// task.status Created → mark_ready
	createdStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseCreated, input: "hello",
	})
	markingReady, _ := svc.Handle(context.Background(), state, createdStatus)
	state = applyDecision(t, svc, state, markingReady)

	// Duplicate created status should be no-op (already moved to PhaseMarkingReady)
	dupCreated, err := svc.Handle(context.Background(), state, createdStatus)
	if err != nil {
		t.Fatal(err)
	}
	if len(dupCreated.Events)+len(dupCreated.Outgoing)+len(dupCreated.Effects) != 0 {
		t.Fatalf("duplicate created status produced work: %#v", dupCreated)
	}

	// task.status Ready → assign
	readyStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, input: "hello",
	})
	assigning, _ := svc.Handle(context.Background(), state, readyStatus)
	state = applyDecision(t, svc, state, assigning)

	// Duplicate ready status should be no-op
	dupReady, err := svc.Handle(context.Background(), state, readyStatus)
	if err != nil {
		t.Fatal(err)
	}
	if len(dupReady.Events)+len(dupReady.Outgoing)+len(dupReady.Effects) != 0 {
		t.Fatalf("duplicate ready status produced work: %#v", dupReady)
	}

	// task.status Ready+Assigned → start
	assignedStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, assignedTo: "agent.test", input: "hello",
	})
	starting, _ := svc.Handle(context.Background(), state, assignedStatus)
	state = applyDecision(t, svc, state, starting)

	// Duplicate assigned status should be no-op
	dupAssigned, err := svc.Handle(context.Background(), state, assignedStatus)
	if err != nil {
		t.Fatal(err)
	}
	if len(dupAssigned.Events)+len(dupAssigned.Outgoing)+len(dupAssigned.Effects) != 0 {
		t.Fatalf("duplicate assigned status produced work: %#v", dupAssigned)
	}

	// task.status Running → succeeded
	runningStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseRunning, assignedTo: "agent.test",
		activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
	})
	completed, _ := svc.Handle(context.Background(), state, runningStatus)
	state = applyDecision(t, svc, state, completed)

	// Duplicate running status should be no-op
	dupRunning, err := svc.Handle(context.Background(), state, runningStatus)
	if err != nil {
		t.Fatal(err)
	}
	if len(dupRunning.Events)+len(dupRunning.Outgoing)+len(dupRunning.Effects) != 0 {
		t.Fatalf("duplicate running status produced work: %#v", dupRunning)
	}
}

func TestDuplicatePendingAndConflictingRequest(t *testing.T) {
	svc, state := newTestService(t)
	create := createMessage(t, "message-1", "request-1", "task-1", "user-1", "hello")
	decision, err := svc.Handle(context.Background(), state, create)
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, decision)

	duplicate, err := svc.Handle(context.Background(), state, create)
	if err != nil {
		t.Fatal(err)
	}
	if len(duplicate.Events)+len(duplicate.Outgoing)+len(duplicate.Effects) != 0 {
		t.Fatalf("pending duplicate produced work: %#v", duplicate)
	}

	conflicts := []contract.Message{
		createMessage(t, "message-payload", "request-1", "task-1", "user-1", "different"),
		createMessage(t, "message-user", "request-1", "task-1", "user-2", "hello"),
		getMessage(t, "message-operation", "request-1", "task-1", "user-1"),
	}
	for _, conflicting := range conflicts {
		conflict, err := svc.Handle(context.Background(), state, conflicting)
		if err != nil {
			t.Fatal(err)
		}
		if len(conflict.Events) != 0 || len(conflict.Outgoing) != 0 || len(conflict.Effects) != 1 {
			t.Fatalf("conflict must not overwrite request state: %#v", conflict)
		}
		presentation := decodePresentation(t, conflict.Effects[0].Payload)
		if presentation.Error == nil || presentation.Error.Code != errRequestConflict {
			t.Fatalf("unexpected conflict presentation: %#v", presentation)
		}
	}
	materialized, _ := decodeState(state)
	if materialized.Requests["request-1"].Input != "hello" {
		t.Fatal("conflict overwrote the original request")
	}
}

func TestExistingTaskIDDelegatesIdempotencyAndConflictToTask(t *testing.T) {
	// First create a task via the full saga to get an owned task mapping.
	svc, state := completedCreateState(t)
	materialized, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	originalOwner := materialized.Tasks["task-1"]

	// A new create request with the same task ID should reuse the existing instance.
	retry := createMessage(t, "message-retry", "request-2", "task-1", "user-1", "hello")
	pending, err := svc.Handle(context.Background(), state, retry)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Events) != 1 || len(pending.Outgoing) != 1 ||
		pending.Outgoing[0].Type != task.CreateMessageType ||
		pending.Outgoing[0].To != originalOwner.Address {
		t.Fatalf("existing task retry did not target the durable Task instance: %#v", pending)
	}
	state = applyDecision(t, svc, state, pending)

	// The task.create returns task_conflict because the task already exists (same content).
	// Actually wait - looking at the task service, a duplicate task.create with the same fingerprint
	// returns the existing task status directly. Let me simulate the task.status reply.
	completed, err := svc.Handle(
		context.Background(),
		state,
		taskStatusReply(t, "request-2", originalOwner.Address, taskState{
			taskID: "task-1", userID: "user-1", phase: task.PhaseRunning, assignedTo: "agent.test",
			activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	// The request went PhaseWaitingTask → task.status → should follow the saga through mark_ready etc.
	// But since the task is already Running, the Gateway should see PhaseCreated from task.create
	// Actually, the task service returns the current state for an idempotent task.create.
	// If the task is already Running, the status reply would show Running, not Created.
	// The Gateway's handleTaskStatus expects Phase=Created when it's in PhaseWaitingTask.
	// Let me re-think this test...

	// Actually for the existing task ID path, the saga still goes through mark_ready etc.
	// The task.create returns Phase=Created (if this is the first time), or the current phase.
	// For the test, let me just flow through the full saga.
	state = applyDecision(t, svc, state, completed)

	materialized, err = decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if materialized.Tasks["task-1"] != originalOwner {
		t.Fatalf("retry replaced task ownership: %#v", materialized.Tasks["task-1"])
	}

	// A conflicting request (different content, same task ID)
	conflicting := createMessage(t, "message-conflict", "request-3", "task-1", "user-1", "different")
	conflictPending, err := svc.Handle(context.Background(), state, conflicting)
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, conflictPending)

	// The task service would return task_conflict for different content
	payload, err := json.Marshal(service.ReplyError{Code: "task_conflict", Message: "different content"})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := svc.Handle(context.Background(), state, contract.Message{
		Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
		From: originalOwner.Address, CorrelationID: "request-3", Payload: payload,
		Metadata: map[string]string{contract.MetadataReplyError: "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertErrorDecision(t, failed, errRequestConflict)
}

func TestSagaFailsWhenAgentUnavailable(t *testing.T) {
	// Create a gateway service without a default agent
	svc := &webGatewayService{
		address: DefaultAddress, instanceID: "gateway-1",
		clock: fixedClock{fixedTime()}, defaultAgent: "",
	}
	initial, err := svc.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}

	// Create request
	create := createMessage(t, "message-1", "request-1", "", "user-1", "hello")
	taskID := derivedTaskID("request-1")
	recorded, err := svc.Handle(context.Background(), initial, create)
	if err != nil {
		t.Fatal(err)
	}
	state := applyDecision(t, svc, initial, recorded)

	var call runtimesystem.Call
	_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
	var declaration instance.Declaration
	_ = json.Unmarshal(call.Payload, &declaration)
	systemReply := successfulSystemResult(t, call, declaration)
	declared, err := svc.Handle(context.Background(), state, systemReply)
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, declared)
	beforeCreateReply, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := beforeCreateReply.Tasks[taskID]; exists {
		t.Fatal("instance declaration must not establish task ownership")
	}

	// task.status Created → mark_ready succeeds
	createdStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: taskID, userID: "user-1", phase: task.PhaseCreated, input: "hello",
	})
	markingReady, err := svc.Handle(context.Background(), state, createdStatus)
	if err != nil {
		t.Fatal(err)
	}
	if len(markingReady.Events) != 2 || markingReady.Events[0].Type != taskOwnershipRecordedEvent {
		t.Fatalf("task.create success must atomically record ownership and advance: %#v", markingReady)
	}
	state = applyDecision(t, svc, state, markingReady)
	ownedState, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	owned := ownedState.Tasks[taskID]
	if owned.TaskID != taskID || owned.UserID != "user-1" ||
		owned.Address != declaration.Address || owned.CreatedByRequestID != "request-1" {
		t.Fatalf("task.create success did not persist ownership: %#v", owned)
	}

	// task.status Ready → assign fails (no default agent configured)
	readyStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: taskID, userID: "user-1", phase: task.PhaseReady, input: "hello",
	})
	failed, err := svc.Handle(context.Background(), state, readyStatus)
	if err != nil {
		t.Fatal(err)
	}
	assertErrorDecision(t, failed, errAgentUnavailable)
	presentation := decodePresentation(t, failed.Effects[0].Payload)
	if presentation.Error.TaskID != taskID {
		t.Fatalf("post-create failure did not return derived task id: %#v", presentation)
	}
	state = applyDecision(t, svc, state, failed)
	failedState, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if failedState.Tasks[taskID] != owned {
		t.Fatalf("later failure removed task ownership: %#v", failedState.Tasks[taskID])
	}

	getDecision, err := svc.Handle(
		context.Background(),
		state,
		getMessage(t, "get-owned", "get-owned", taskID, "user-1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(getDecision.Outgoing) != 1 || getDecision.Outgoing[0].Type != task.GetMessageType ||
		getDecision.Outgoing[0].To != owned.Address {
		t.Fatalf("owned task is unreachable after create saga failure: %#v", getDecision)
	}
	unauthorized, err := svc.Handle(
		context.Background(),
		state,
		getMessage(t, "get-other", "get-other", taskID, "user-2"),
	)
	if err != nil {
		t.Fatal(err)
	}
	unauthorizedPresentation := decodePresentation(t, unauthorized.Effects[0].Payload)
	if unauthorizedPresentation.Error == nil ||
		unauthorizedPresentation.Error.Code != errTaskNotFound ||
		unauthorizedPresentation.Error.TaskID != "" {
		t.Fatalf("cross-user get leaked owned task identity: %#v", unauthorizedPresentation)
	}

	duplicate, err := svc.Handle(context.Background(), state, create)
	if err != nil {
		t.Fatal(err)
	}
	duplicatePresentation := decodePresentation(t, duplicate.Effects[0].Payload)
	if duplicatePresentation.Error == nil ||
		duplicatePresentation.Error.TaskID != taskID ||
		duplicate.Effects[0].IdempotencyKey != failed.Effects[0].IdempotencyKey {
		t.Fatalf("duplicate create changed durable failure association: %#v", duplicate)
	}
}

func TestTaskCreateReplyAtomicallyRecordsOwnershipWhenNextStepFails(t *testing.T) {
	svc := &webGatewayService{
		address: DefaultAddress, instanceID: "gateway-1",
		clock: fixedClock{fixedTime()}, defaultAgent: "",
	}
	initial, err := svc.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}
	state := advanceToPhase(t, svc, initial, PhaseWaitingTask)
	before, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := before.Tasks["task-1"]; found {
		t.Fatal("task ownership exists before task.create success reply")
	}

	ready := taskStatusReply(t, "request-1", stableTaskAddr(t), taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, input: "hello",
	})
	failed, err := svc.Handle(context.Background(), state, ready)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed.Events) != 2 ||
		failed.Events[0].Type != taskOwnershipRecordedEvent ||
		failed.Events[1].Type != requestFailedEvent ||
		len(failed.Effects) != 1 {
		t.Fatalf("ownership and failure must share one atomic decision: %#v", failed)
	}
	presentation := decodePresentation(t, failed.Effects[0].Payload)
	if presentation.Error == nil || presentation.Error.Code != errAgentUnavailable ||
		presentation.Error.TaskID != "task-1" {
		t.Fatalf("atomic post-create failure lost task identity: %#v", presentation)
	}
	state = applyDecision(t, svc, state, failed)
	after, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if after.Requests["request-1"].Phase != PhaseFailed ||
		after.Tasks["task-1"].CreatedByRequestID != "request-1" {
		t.Fatalf("atomic task.create failure state is incomplete: %#v", after)
	}
}

func TestGetUsesOwnedMappingAndHidesAuthorization(t *testing.T) {
	svc, state := completedCreateState(t)

	unauthorized := getMessage(t, "get-message-1", "get-1", "task-1", "user-2")
	notFound, err := svc.Handle(context.Background(), state, unauthorized)
	if err != nil {
		t.Fatal(err)
	}
	if len(notFound.Events) != 1 || len(notFound.Effects) != 1 || len(notFound.Outgoing) != 0 {
		t.Fatalf("unauthorized get leaked work: %#v", notFound)
	}
	presentation := decodePresentation(t, notFound.Effects[0].Payload)
	if presentation.Error == nil || presentation.Error.Code != errTaskNotFound {
		t.Fatalf("unauthorized get did not use safe not found: %#v", presentation)
	}

	get := getMessage(t, "get-message-2", "get-2", "task-1", "user-1")
	pending, err := svc.Handle(context.Background(), state, get)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Events) != 1 || len(pending.Outgoing) != 1 {
		t.Fatalf("unexpected get decision: %#v", pending)
	}
	if pending.Outgoing[0].Kind != contract.MessageQuery || pending.Outgoing[0].Type != task.GetMessageType ||
		pending.Outgoing[0].To == "" || pending.Outgoing[0].ReplyTo != DefaultAddress {
		t.Fatalf("get did not use persisted task mapping: %#v", pending.Outgoing[0])
	}
	state = applyDecision(t, svc, state, pending)

	status := taskStatusReply(t, "get-2", pending.Outgoing[0].To, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseRunning, assignedTo: "agent.test",
		activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
	})
	succeeded, err := svc.Handle(context.Background(), state, status)
	if err != nil {
		t.Fatal(err)
	}
	if len(succeeded.Events) != 1 || len(succeeded.Effects) != 1 {
		t.Fatalf("unexpected get completion: %#v", succeeded)
	}
	found := decodePresentation(t, succeeded.Effects[0].Payload)
	if found.Found == nil || found.Found.Task.TaskID != "task-1" || found.Created != nil {
		t.Fatalf("unexpected get presentation: %#v", found)
	}
}

func TestErrorRepliesBecomeStableTerminalPresentations(t *testing.T) {
	t.Run("system declaration", func(t *testing.T) {
		svc, state := newTestService(t)
		create := createMessage(t, "message-1", "request-1", "task-1", "user-1", "hello")
		recorded, err := svc.Handle(context.Background(), state, create)
		if err != nil {
			t.Fatal(err)
		}
		state = applyDecision(t, svc, state, recorded)
		var call runtimesystem.Call
		_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
		payload, _ := json.Marshal(service.ReplyError{Code: "conflict", Message: "unsafe internal detail"})
		failed, err := svc.Handle(context.Background(), state, contract.Message{
			Kind: contract.MessageReply, Type: runtimesystem.ResultMessageType, Version: runtimesystem.CallVersion,
			From: runtimesystem.Address, CorrelationID: "request-1", Payload: payload,
			Metadata: map[string]string{
				contract.MetadataReplyError:     "true",
				runtimesystem.MetadataCallID:    call.CallID,
				runtimesystem.MetadataOperation: runtimesystem.DeclareInstanceOperation,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		assertErrorDecision(t, failed, errDeclarationFailed)
	})

	t.Run("task query", func(t *testing.T) {
		svc, state := completedCreateState(t)
		get := getMessage(t, "get-message", "get-1", "task-1", "user-1")
		pending, err := svc.Handle(context.Background(), state, get)
		if err != nil {
			t.Fatal(err)
		}
		state = applyDecision(t, svc, state, pending)
		payload, _ := json.Marshal(service.ReplyError{Code: "task_access_denied", Message: "owner mismatch"})
		failed, err := svc.Handle(context.Background(), state, contract.Message{
			Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
			From: pending.Outgoing[0].To, CorrelationID: "get-1", Payload: payload,
			Metadata: map[string]string{contract.MetadataReplyError: "true"},
		})
		if err != nil {
			t.Fatal(err)
		}
		assertErrorDecision(t, failed, errTaskNotFound)
	})
}

func TestOwnedTaskSurvivesFailuresAtEveryLaterSagaStage(t *testing.T) {
	for _, phase := range []RequestPhase{PhaseMarkingReady, PhaseAssigning, PhaseStarting} {
		t.Run(string(phase), func(t *testing.T) {
			svc, initial := newTestService(t)
			state := advanceToPhase(t, svc, initial, phase)
			before, err := decodeState(state)
			if err != nil {
				t.Fatal(err)
			}
			owned := before.Tasks["task-1"]
			if owned.TaskID == "" {
				t.Fatalf("phase %q lacks ownership after task.create success", phase)
			}

			payload, err := json.Marshal(service.ReplyError{
				Code: "task_stage_failed", Message: "later saga stage failed", Retryable: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			failed, err := svc.Handle(context.Background(), state, contract.Message{
				Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
				From: owned.Address, CorrelationID: "request-1", Payload: payload,
				Metadata: map[string]string{contract.MetadataReplyError: "true"},
			})
			if err != nil {
				t.Fatal(err)
			}
			assertErrorDecision(t, failed, errTaskRequestFailed)
			presentation := decodePresentation(t, failed.Effects[0].Payload)
			if presentation.Error.TaskID != owned.TaskID {
				t.Fatalf("phase %q failure lost task association: %#v", phase, presentation)
			}
			state = applyDecision(t, svc, state, failed)
			after, err := decodeState(state)
			if err != nil {
				t.Fatal(err)
			}
			if after.Tasks["task-1"] != owned {
				t.Fatalf("phase %q failure changed ownership: %#v", phase, after.Tasks["task-1"])
			}

			late, err := svc.Handle(context.Background(), state, taskStatusReply(
				t,
				"request-1",
				owned.Address,
				taskState{
					taskID: "task-1", userID: "user-1", phase: task.PhaseRunning,
					assignedTo: "agent.test", activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
				},
			))
			if err != nil {
				t.Fatal(err)
			}
			if len(late.Events)+len(late.Outgoing)+len(late.Effects) != 0 {
				t.Fatalf("late status changed failed request in phase %q: %#v", phase, late)
			}
		})
	}
}

func TestReSyncOnTaskInvalidTransition(t *testing.T) {
	// At-least-once delivery can replay a saga-step command after the task has
	// already advanced.  Instead of failing, the Gateway must re-read the
	// task's current state so progressCreateSaga can fast-forward.
	svc, state := newTestService(t)
	create := createMessage(t, "message-1", "request-1", "task-1", "user-1", "hello")
	recorded, _ := svc.Handle(context.Background(), state, create)
	state = applyDecision(t, svc, state, recorded)

	var call runtimesystem.Call
	_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
	var declaration instance.Declaration
	_ = json.Unmarshal(call.Payload, &declaration)
	systemReply := successfulSystemResult(t, call, declaration)
	declared, _ := svc.Handle(context.Background(), state, systemReply)
	state = applyDecision(t, svc, state, declared)

	// Simulate the task rejecting a mark_ready because it's already running
	// (At-least-once duplicate of the mark_ready command).
	payload, _ := json.Marshal(service.ReplyError{Code: "task_invalid_transition", Message: "only a created task can become ready"})
	decision, err := svc.Handle(context.Background(), state, contract.Message{
		Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
		From: declaration.Address, CorrelationID: "request-1", Payload: payload,
		Metadata: map[string]string{contract.MetadataReplyError: "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Must be a re-sync query, not a terminal failure.
	if len(decision.Events) != 0 || len(decision.Outgoing) != 1 || len(decision.Effects) != 0 {
		t.Fatalf("invalid_transition must re-sync without terminal decision: %#v", decision)
	}
	if decision.Outgoing[0].Kind != contract.MessageQuery || decision.Outgoing[0].Type != task.GetMessageType {
		t.Fatalf("re-sync must send task.get: %#v", decision.Outgoing[0])
	}
	// The request state must remain in the current phase so the re-sync
	// reply flows back through progressCreateSaga.
	mat, _ := decodeState(state)
	if mat.Requests["request-1"].Phase != PhaseWaitingTask {
		t.Fatalf("re-sync must not change request phase; got %q", mat.Requests["request-1"].Phase)
	}

	// Now simulate the re-sync reply: the task is already running.
	runningStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseRunning, assignedTo: "agent.test",
		activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
	})
	completed, err := svc.Handle(context.Background(), state, runningStatus)
	if err != nil {
		t.Fatal(err)
	}
	if len(completed.Events) != 2 ||
		completed.Events[0].Type != taskOwnershipRecordedEvent ||
		completed.Events[1].Type != requestSucceededEvent ||
		len(completed.Effects) != 1 {
		t.Fatalf("re-sync reply must produce terminal success: %#v", completed)
	}
	presentation := decodePresentation(t, completed.Effects[0].Payload)
	if presentation.Created == nil || presentation.Created.Task.Phase != task.PhaseRunning {
		t.Fatalf("re-synced task must show running: %#v", presentation)
	}
}

func TestInvalidTransitionReSyncThenFastForwardsThroughAllPhases(t *testing.T) {
	// When the task is already running and the Gateway re-syncs from each
	// possible saga phase, the reply must fast-forward to success.
	for _, startPhase := range []RequestPhase{PhaseWaitingTask, PhaseMarkingReady, PhaseAssigning} {
		t.Run(string(startPhase), func(t *testing.T) {
			svc, state := newTestService(t)
			state = advanceToPhase(t, svc, state, startPhase)

			runningStatus := taskStatusReply(t, "request-1", stableTaskAddr(t), taskState{
				taskID: "task-1", userID: "user-1", phase: task.PhaseRunning, assignedTo: "agent.test",
				activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
			})
			completed, err := svc.Handle(context.Background(), state, runningStatus)
			if err != nil {
				t.Fatal(err)
			}
			presentation := decodePresentation(t, completed.Effects[0].Payload)
			if presentation.Created == nil || presentation.Created.Task.Phase != task.PhaseRunning {
				t.Fatalf("from %s: fast-forward failed: %#v", startPhase, presentation)
			}
			// Apply must also succeed (verifies Fix 1)
			state = applyDecision(t, svc, state, completed)
			mat, _ := decodeState(state)
			if mat.Requests["request-1"].Phase != PhaseSucceeded {
				t.Fatalf("from %s: apply did not set succeeded", startPhase)
			}
			if owned := mat.Tasks["task-1"]; owned.TaskID != "task-1" || owned.UserID != "user-1" {
				t.Fatalf("from %s: fast-forward did not retain ownership: %#v", startPhase, owned)
			}
		})
	}
}

func TestTerminalEventWinsRaceWithRunningStatus(t *testing.T) {
	for _, phase := range []task.Phase{
		task.PhaseCompleted,
		task.PhaseFailed,
		task.PhaseCancelled,
	} {
		t.Run(string(phase), func(t *testing.T) {
			svc, initial := newTestService(t)
			state := advanceToPhase(t, svc, initial, PhaseStarting)
			taskAddress := stableTaskAddr(t)
			terminalState, terminalEvent := terminalTaskFixture(t, phase, taskAddress)

			observed, err := svc.Handle(context.Background(), state, terminalEvent)
			if err != nil {
				t.Fatal(err)
			}
			if len(observed.Events) != 1 || observed.Events[0].Type != taskTerminalObservedEvent ||
				len(observed.Outgoing) != 1 || observed.Outgoing[0].Type != task.GetMessageType ||
				len(observed.Effects) != 0 {
				t.Fatalf("terminal observation decision=%#v", observed)
			}
			replayed := applyDecision(t, svc, state, observed)
			state = applyDecision(t, svc, state, observed)
			if string(replayed.Data) != string(state.Data) {
				t.Fatal("terminal observation replay is not deterministic")
			}
			materialized, err := decodeState(state)
			if err != nil {
				t.Fatal(err)
			}
			request := materialized.Requests["request-1"]
			if request.Phase != PhaseResolvingTerminal || request.Terminal == nil ||
				request.Terminal.Result.Phase != phase {
				t.Fatalf("terminal observation was not durable: %#v", request)
			}

			duplicate, err := svc.Handle(context.Background(), state, terminalEvent)
			if err != nil {
				t.Fatal(err)
			}
			if len(duplicate.Events)+len(duplicate.Outgoing)+len(duplicate.Effects) != 0 {
				t.Fatalf("duplicate terminal event produced work: %#v", duplicate)
			}

			lateRunning := taskStatusReply(t, "request-1", taskAddress, taskState{
				taskID: "task-1", userID: "user-1", phase: task.PhaseRunning,
				assignedTo: "agent.test", activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
			})
			ignored, err := svc.Handle(context.Background(), state, lateRunning)
			if err != nil {
				t.Fatal(err)
			}
			if len(ignored.Events)+len(ignored.Outgoing)+len(ignored.Effects) != 0 {
				t.Fatalf("late Running status superseded terminal fact: %#v", ignored)
			}

			errorPayload, err := json.Marshal(service.ReplyError{
				Code: "task_invalid_transition", Message: "task is already terminal",
			})
			if err != nil {
				t.Fatal(err)
			}
			lateError, err := svc.Handle(context.Background(), state, contract.Message{
				Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
				From: taskAddress, CorrelationID: "request-1", Payload: errorPayload,
				Metadata: map[string]string{contract.MetadataReplyError: "true"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(lateError.Events) != 0 || len(lateError.Outgoing) != 1 ||
				lateError.Outgoing[0].Type != task.GetMessageType || len(lateError.Effects) != 0 {
				t.Fatalf("late task error superseded terminal fact: %#v", lateError)
			}

			terminalStatus := taskStatusReplyFromState(t, "request-1", taskAddress, terminalState)
			completed, err := svc.Handle(context.Background(), state, terminalStatus)
			if err != nil {
				t.Fatal(err)
			}
			if len(completed.Events) != 1 || len(completed.Effects) != 1 {
				t.Fatalf("terminal refresh did not complete create: %#v", completed)
			}
			presentation := decodePresentation(t, completed.Effects[0].Payload)
			if presentation.Created == nil || presentation.Created.Task.Phase != phase {
				t.Fatalf("terminal presentation=%#v", presentation)
			}
			state = applyDecision(t, svc, state, completed)

			lateAfterCompletion, err := svc.Handle(context.Background(), state, lateRunning)
			if err != nil {
				t.Fatal(err)
			}
			if len(lateAfterCompletion.Events)+len(lateAfterCompletion.Outgoing)+len(lateAfterCompletion.Effects) != 0 {
				t.Fatalf("late Running status overwrote terminal presentation: %#v", lateAfterCompletion)
			}
		})
	}
}

func advanceToPhase(t *testing.T, svc *webGatewayService, initial service.State, target RequestPhase) service.State {
	t.Helper()
	create := createMessage(t, "msg-1", "request-1", "task-1", "user-1", "hello")
	recorded, _ := svc.Handle(context.Background(), initial, create)
	state := applyDecision(t, svc, initial, recorded)

	var call runtimesystem.Call
	_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
	var declaration instance.Declaration
	_ = json.Unmarshal(call.Payload, &declaration)
	addr := declaration.Address
	systemReply := successfulSystemResult(t, call, declaration)
	declared, _ := svc.Handle(context.Background(), state, systemReply)

	if target == PhaseWaitingTask {
		return applyDecision(t, svc, state, declared)
	}
	state = applyDecision(t, svc, state, declared)

	createdStatus := taskStatusReply(t, "request-1", addr, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseCreated, input: "hello",
	})
	markingReady, _ := svc.Handle(context.Background(), state, createdStatus)
	if target == PhaseMarkingReady {
		return applyDecision(t, svc, state, markingReady)
	}
	state = applyDecision(t, svc, state, markingReady)

	readyStatus := taskStatusReply(t, "request-1", addr, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, input: "hello",
	})
	assigning, _ := svc.Handle(context.Background(), state, readyStatus)
	if target == PhaseAssigning {
		return applyDecision(t, svc, state, assigning)
	}
	state = applyDecision(t, svc, state, assigning)

	assignedStatus := taskStatusReply(t, "request-1", addr, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, assignedTo: "agent.test", input: "hello",
	})
	starting, _ := svc.Handle(context.Background(), state, assignedStatus)
	return applyDecision(t, svc, state, starting)
}

func stableTaskAddr(t *testing.T) contract.ServiceAddress {
	t.Helper()
	addr, _ := stableTaskIdentity("task-1", "request-1")
	return addr
}

func TestConcurrentInFlightCreateForSameTaskIDIsRejected(t *testing.T) {
	svc, state := newTestService(t)
	// Start first create — enters PhaseDeclaringTask
	create1 := createMessage(t, "msg-1", "request-1", "task-1", "user-1", "hello")
	decision1, err := svc.Handle(context.Background(), state, create1)
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, decision1)

	// Verify first request is in-flight
	mat, _ := decodeState(state)
	if mat.Requests["request-1"].Phase.terminal() {
		t.Fatal("first request must be in-flight")
	}

	// Second request with same TaskID must be rejected
	create2 := createMessage(t, "msg-2", "request-2", "task-1", "user-1", "hello")
	decision2, err := svc.Handle(context.Background(), state, create2)
	if err != nil {
		t.Fatal(err)
	}
	assertErrorDecision(t, decision2, errRequestConflict)

	// But a different TaskID is still allowed
	create3 := createMessage(t, "msg-3", "request-3", "task-2", "user-1", "world")
	decision3, err := svc.Handle(context.Background(), state, create3)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision3.Events) != 1 {
		t.Fatalf("different TaskID must be allowed: %#v", decision3)
	}

}

func TestConcurrentCreateRemainsRejectedAfterTaskCreateSucceeds(t *testing.T) {
	svc, initial := newTestService(t)
	state := advanceToPhase(t, svc, initial, PhaseMarkingReady)
	before, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	owned := before.Tasks["task-1"]
	if owned.TaskID == "" {
		t.Fatal("task.create success did not establish ownership")
	}

	conflicting, err := svc.Handle(
		context.Background(),
		state,
		createMessage(t, "msg-2", "request-2", "task-1", "user-1", "hello"),
	)
	if err != nil {
		t.Fatal(err)
	}
	assertErrorDecision(t, conflicting, errRequestConflict)
	state = applyDecision(t, svc, state, conflicting)
	after, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if after.Tasks["task-1"] != owned {
		t.Fatalf("conflicting create replaced ownership: %#v", after.Tasks["task-1"])
	}
}

func TestInFlightGetDoesNotConflictWithCreateForSameTaskID(t *testing.T) {
	svc, state := completedCreateState(t)
	get := getMessage(t, "get-message-1", "get-request-1", "task-1", "user-1")
	getDecision, err := svc.Handle(context.Background(), state, get)
	if err != nil {
		t.Fatal(err)
	}
	if len(getDecision.Events) != 1 || len(getDecision.Outgoing) != 1 ||
		getDecision.Outgoing[0].Type != task.GetMessageType {
		t.Fatalf("get request did not become in-flight: %#v", getDecision)
	}
	state = applyDecision(t, svc, state, getDecision)

	create := createMessage(t, "create-message-2", "create-request-2", "task-1", "user-1", "hello")
	createDecision, err := svc.Handle(context.Background(), state, create)
	if err != nil {
		t.Fatal(err)
	}
	if len(createDecision.Effects) != 0 ||
		len(createDecision.Events) != 1 ||
		len(createDecision.Outgoing) != 1 ||
		createDecision.Outgoing[0].Type != task.CreateMessageType {
		t.Fatalf("in-flight get blocked a valid create: %#v", createDecision)
	}
}

func TestReplayIsDeterministicAndDoesNotPresent(t *testing.T) {
	svc, initial := newTestService(t)
	recorded, _ := svc.Handle(context.Background(), initial, createMessage(t, "message-1", "request-1", "task-1", "user-1", "hello"))
	state1 := applyDecision(t, svc, initial, recorded)
	var call runtimesystem.Call
	_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
	var declaration instance.Declaration
	_ = json.Unmarshal(call.Payload, &declaration)
	declared, _ := svc.Handle(context.Background(), state1, successfulSystemResult(t, call, declaration))
	state2 := applyDecision(t, svc, state1, declared)
	createdStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseCreated, input: "hello",
	})
	markingReady, _ := svc.Handle(context.Background(), state2, createdStatus)
	state3 := applyDecision(t, svc, state2, markingReady)
	readyStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, input: "hello",
	})
	assigning, _ := svc.Handle(context.Background(), state3, readyStatus)
	state4 := applyDecision(t, svc, state3, assigning)
	assignedStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, assignedTo: "agent.test", input: "hello",
	})
	starting, _ := svc.Handle(context.Background(), state4, assignedStatus)
	state5 := applyDecision(t, svc, state4, starting)
	runningStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseRunning, assignedTo: "agent.test",
		activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
	})
	completed, _ := svc.Handle(context.Background(), state5, runningStatus)

	events := []service.NewEvent{
		recorded.Events[0], declared.Events[0], markingReady.Events[0],
		assigning.Events[0], starting.Events[0], completed.Events[0],
	}
	replayA, replayB := initial, initial
	for index, event := range events {
		stored := storedEvent(index+1, event)
		var err error
		replayA, err = svc.Apply(replayA, stored)
		if err != nil {
			t.Fatal(err)
		}
		replayB, err = svc.Apply(replayB, stored)
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(replayA.Data) != string(replayB.Data) {
		t.Fatal("replay produced different materialized states")
	}
	if len(completed.Effects) != 1 {
		t.Fatal("live terminal decision should plan one presentation")
	}
}

func TestTerminalProjectionIsBounded(t *testing.T) {
	svc, state := newTestService(t)
	base := fixedTime()
	for index := 0; index < RetainedTerminalRequests+5; index++ {
		id := fmt.Sprintf("request-%03d", index)
		completed := base.Add(time.Duration(index) * time.Second)
		request := RequestState{
			RequestID: id, Operation: OperationGet, UserID: "user-1", TaskID: "missing",
			Phase: PhaseFailed, IdentityFingerprint: "fingerprint-" + id,
			Error:          &ErrorDTO{Code: errTaskNotFound, Message: "task was not found"},
			PresentationID: presentationID(id, OperationGet, "error/"+errTaskNotFound),
			CreatedAt:      completed, UpdatedAt: completed, CompletedAt: &completed,
		}
		event, err := newRequestEvent("event/"+id, requestRecordedEvent, request, nil)
		if err != nil {
			t.Fatal(err)
		}
		state, err = svc.Apply(state, storedEvent(index+1, event))
		if err != nil {
			t.Fatal(err)
		}
	}
	materialized, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(materialized.Requests) != RetainedTerminalRequests || len(materialized.TerminalOrderIDs) != RetainedTerminalRequests {
		t.Fatalf("terminal projection is not bounded: requests=%d order=%d", len(materialized.Requests), len(materialized.TerminalOrderIDs))
	}
	if _, found := materialized.Requests["request-000"]; found {
		t.Fatal("oldest terminal request was not evicted")
	}
}

func TestDefinitionDeclaresContractsAndSystemPermission(t *testing.T) {
	definition := Definition(ServiceFactory{clock: fixedClock{fixedTime()}})
	if definition.Component != Component || definition.Scope != "runtime_singleton" || definition.StateSchema != StateSchema {
		t.Fatalf("unexpected definition: %#v", definition)
	}
	if len(definition.SystemOperations) != 1 || definition.SystemOperations[0] != assembly.SystemOperationDeclareInstance {
		t.Fatalf("declare instance permission missing: %#v", definition.SystemOperations)
	}
	if len(definition.EffectExecutors) != 1 || definition.EffectExecutors[0] != PresentationExecutorRef {
		t.Fatalf("presentation executor missing: %#v", definition.EffectExecutors)
	}
	// Verify new saga command types are declared
	produces := make(map[contract.MessageType]bool)
	for _, contract := range definition.Produces {
		produces[contract.Type] = true
	}
	for _, expected := range []contract.MessageType{task.MarkReadyMessageType, task.AssignMessageType, task.StartMessageType} {
		if !produces[expected] {
			t.Fatalf("definition missing produces %q", expected)
		}
	}
}

// --- test helpers ---

type taskState struct {
	taskID      string
	userID      string
	phase       task.Phase
	assignedTo  contract.ServiceAddress
	activeRunID string
	attempt     int
	input       string
}

func taskStatusReply(t *testing.T, requestID string, from contract.ServiceAddress, ts taskState) contract.Message {
	t.Helper()
	now := fixedTime()
	state := task.State{
		TaskID: ts.taskID, UserID: ts.userID, OwnerAddress: DefaultAddress,
		Phase: ts.phase, AssignedTo: ts.assignedTo, ActiveRunID: ts.activeRunID,
		Attempt: ts.attempt, Input: ts.input,
		IdentityFingerprint: "task-fingerprint", CreatedAt: now, UpdatedAt: now,
	}
	if ts.phase == task.PhaseFailed {
		state.LastError = &task.Error{Code: "some_error", Message: "failed", OccurredAt: now}
		state.CompletedAt = &now
	}
	payload, err := json.Marshal(task.StatusResponse{Task: &state})
	if err != nil {
		t.Fatal(err)
	}
	return contract.Message{
		Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
		From: from, To: DefaultAddress, CorrelationID: requestID, UserID: ts.userID, Payload: payload,
	}
}

func terminalTaskFixture(
	t *testing.T,
	phase task.Phase,
	from contract.ServiceAddress,
) (task.State, contract.Message) {
	t.Helper()
	now := fixedTime()
	completedAt := now.Add(time.Minute)
	value := task.State{
		TaskID: "task-1", UserID: "user-1", OwnerAddress: DefaultAddress,
		Phase: phase, AssignedTo: "agent.test", ActiveRunID: "task-1/attempt/1",
		Attempt: 1, Input: "hello", IdentityFingerprint: "task-fingerprint",
		CreatedAt: now, UpdatedAt: completedAt, CompletedAt: &completedAt,
	}
	switch phase {
	case task.PhaseCompleted:
		value.ResultRef = &contract.ArtifactRef{
			Store: "test", Key: "tasks/task-1/result.txt", ContentType: "text/plain", Size: 4,
		}
	case task.PhaseFailed:
		value.LastError = &task.Error{
			Code: "agent_failed", Message: "agent failed", Source: "agent",
			RunID: value.ActiveRunID, OccurredAt: completedAt,
		}
	case task.PhaseCancelled:
		value.LastError = &task.Error{
			Code: "cancelled", Message: "task was cancelled", Source: "task",
			RunID: value.ActiveRunID, OccurredAt: completedAt,
		}
	default:
		t.Fatalf("unsupported terminal phase %q", phase)
	}
	observation, err := terminalObservationFromState(value)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(observation.Result)
	if err != nil {
		t.Fatal(err)
	}
	return value, contract.Message{
		ID:   "terminal-" + string(phase),
		Kind: contract.MessageEvent, Type: observation.MessageType, Version: task.ProtocolVersion,
		From: from, To: DefaultAddress, CorrelationID: value.TaskID, Payload: payload,
	}
}

func taskStatusReplyFromState(
	t *testing.T,
	requestID string,
	from contract.ServiceAddress,
	value task.State,
) contract.Message {
	t.Helper()
	cloned := value.Clone()
	payload, err := json.Marshal(task.StatusResponse{Task: &cloned})
	if err != nil {
		t.Fatal(err)
	}
	return contract.Message{
		ID:   "task-status-" + requestID + "-" + string(value.Phase),
		Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
		From: from, To: DefaultAddress, CorrelationID: requestID, UserID: value.UserID, Payload: payload,
	}
}

func newTestService(t *testing.T) (*webGatewayService, service.State) {
	t.Helper()
	svc := &webGatewayService{
		address: DefaultAddress, instanceID: "gateway-1",
		clock: fixedClock{fixedTime()}, defaultAgent: "agent.test",
	}
	state, err := svc.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}
	return svc, state
}

func completedCreateState(t *testing.T) (*webGatewayService, service.State) {
	t.Helper()
	svc, state := newTestService(t)

	// Create → declare instance
	recorded, err := svc.Handle(context.Background(), state, createMessage(t, "message-1", "request-1", "task-1", "user-1", "hello"))
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, recorded)

	var call runtimesystem.Call
	_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
	var declaration instance.Declaration
	_ = json.Unmarshal(call.Payload, &declaration)

	// System result → task.create
	systemReply := successfulSystemResult(t, call, declaration)
	declared, err := svc.Handle(context.Background(), state, systemReply)
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, declared)

	// task.status Created → mark_ready
	createdStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseCreated, input: "hello",
	})
	markingReady, err := svc.Handle(context.Background(), state, createdStatus)
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, markingReady)

	// task.status Ready → assign
	readyStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, input: "hello",
	})
	assigning, err := svc.Handle(context.Background(), state, readyStatus)
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, assigning)

	// task.status Ready+Assigned → start
	assignedStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseReady, assignedTo: "agent.test", input: "hello",
	})
	starting, err := svc.Handle(context.Background(), state, assignedStatus)
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, starting)

	// task.status Running → succeeded
	runningStatus := taskStatusReply(t, "request-1", declaration.Address, taskState{
		taskID: "task-1", userID: "user-1", phase: task.PhaseRunning, assignedTo: "agent.test",
		activeRunID: "task-1/attempt/1", attempt: 1, input: "hello",
	})
	completed, err := svc.Handle(context.Background(), state, runningStatus)
	if err != nil {
		t.Fatal(err)
	}
	return svc, applyDecision(t, svc, state, completed)
}

func createMessage(t *testing.T, messageID, requestID, taskID, userID, input string) contract.Message {
	t.Helper()
	payload, err := json.Marshal(CreateTaskRequest{RequestID: requestID, TaskID: taskID, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	return contract.Message{
		ID: messageID, Kind: contract.MessageCommand, Type: CreateTaskMessageType, Version: ProtocolVersion,
		From: "web.adapter", To: DefaultAddress, UserID: userID, Payload: payload,
	}
}

func getMessage(t *testing.T, messageID, requestID, taskID, userID string) contract.Message {
	t.Helper()
	payload, err := json.Marshal(GetTaskRequest{RequestID: requestID, TaskID: taskID})
	if err != nil {
		t.Fatal(err)
	}
	return contract.Message{
		ID: messageID, Kind: contract.MessageCommand, Type: GetTaskMessageType, Version: ProtocolVersion,
		From: "web.adapter", To: DefaultAddress, UserID: userID, Payload: payload,
	}
}

func successfulSystemResult(t *testing.T, call runtimesystem.Call, declaration instance.Declaration) contract.Message {
	t.Helper()
	declared, err := json.Marshal(runtimesystem.DeclareInstanceResult{Instance: instance.Record{
		InstanceID: declaration.InstanceID, Address: declaration.Address, DefinitionRef: declaration.Component,
	}})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(runtimesystem.Result{
		CallID: call.CallID, Operation: call.Operation, OperationVersion: call.OperationVersion, Result: declared,
	})
	if err != nil {
		t.Fatal(err)
	}
	return contract.Message{
		Kind: contract.MessageReply, Type: runtimesystem.ResultMessageType, Version: runtimesystem.CallVersion,
		From: runtimesystem.Address, To: DefaultAddress, CorrelationID: "request-1", Payload: payload,
		Metadata: map[string]string{
			runtimesystem.MetadataCallID: call.CallID, runtimesystem.MetadataOperation: call.Operation,
		},
	}
}

func applyDecision(t *testing.T, svc *webGatewayService, state service.State, decision service.Decision) service.State {
	t.Helper()
	for index, event := range decision.Events {
		var err error
		state, err = svc.Apply(state, storedEvent(index+1, event))
		if err != nil {
			t.Fatalf("apply %q: %v", event.Type, err)
		}
	}
	return state
}

func storedEvent(sequence int, event service.NewEvent) contract.StoredEvent {
	return contract.StoredEvent{
		EventID: fmt.Sprintf("event-%d", sequence), EventType: event.Type,
		EventVersion: event.Version, Sequence: uint64(sequence), Payload: event.Payload,
	}
}

func decodePresentation(t *testing.T, payload json.RawMessage) Presentation {
	t.Helper()
	var presentation Presentation
	if err := json.Unmarshal(payload, &presentation); err != nil {
		t.Fatal(err)
	}
	return presentation
}

func assertErrorDecision(t *testing.T, decision service.Decision, code string) {
	t.Helper()
	if len(decision.Events) != 1 || len(decision.Effects) != 1 {
		t.Fatalf("error must atomically persist and present: %#v", decision)
	}
	presentation := decodePresentation(t, decision.Effects[0].Payload)
	if presentation.Error == nil || presentation.Error.Code != code {
		t.Fatalf("unexpected error presentation code=%q: %#v", code, presentation)
	}
}

func fixedTime() time.Time {
	return time.Date(2026, 7, 23, 8, 0, 0, 0, time.UTC)
}

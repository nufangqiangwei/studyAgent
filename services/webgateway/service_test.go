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

func TestCreateStateMachineAndIdempotentPresentation(t *testing.T) {
	svc, state := newTestService(t)
	create := createMessage(t, "message-create-1", "request-1", "task-1", "user-1", "hello")

	recorded, err := svc.Handle(context.Background(), state, create)
	if err != nil {
		t.Fatalf("handle create: %v", err)
	}
	if len(recorded.Events) != 1 || len(recorded.Outgoing) != 1 || recorded.Outgoing[0].Type != runtimesystem.CallMessageType {
		t.Fatalf("unexpected create decision: %#v", recorded)
	}
	var call runtimesystem.Call
	if err := json.Unmarshal(recorded.Outgoing[0].Payload, &call); err != nil {
		t.Fatalf("decode system call: %v", err)
	}
	if call.Operation != assembly.SystemOperationDeclareInstance || recorded.Outgoing[0].ReplyTo != DefaultAddress {
		t.Fatalf("unexpected system call: %#v", recorded.Outgoing[0])
	}
	var declaration instance.Declaration
	if err := json.Unmarshal(call.Payload, &declaration); err != nil {
		t.Fatalf("decode declaration: %v", err)
	}
	if declaration.Component != task.Component || declaration.InstanceID == "" || declaration.Address == "" || declaration.ParentID != "gateway-1" {
		t.Fatalf("unexpected declaration: %#v", declaration)
	}
	state = applyDecision(t, svc, state, recorded)

	systemReply := successfulSystemResult(t, call, declaration)
	declared, err := svc.Handle(context.Background(), state, systemReply)
	if err != nil {
		t.Fatalf("handle system result: %v", err)
	}
	if len(declared.Events) != 1 || len(declared.Outgoing) != 1 {
		t.Fatalf("unexpected declaration decision: %#v", declared)
	}
	outgoing := declared.Outgoing[0]
	if outgoing.Kind != contract.MessageCommand || outgoing.Type != task.CreateMessageType ||
		outgoing.To != declaration.Address || outgoing.ReplyTo != DefaultAddress || outgoing.CorrelationID != "request-1" {
		t.Fatalf("unexpected task create: %#v", outgoing)
	}
	state = applyDecision(t, svc, state, declared)

	status := createdTaskStatus(t, "request-1", declaration.Address, "task-1", "user-1")
	completed, err := svc.Handle(context.Background(), state, status)
	if err != nil {
		t.Fatalf("handle task status: %v", err)
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
	if request.Phase != PhaseSucceeded || request.Task == nil || request.Task.Phase != task.PhaseCreated {
		t.Fatalf("unexpected terminal request: %#v", request)
	}
	owned := materialized.Tasks["task-1"]
	if owned.Address != declaration.Address || owned.UserID != "user-1" {
		t.Fatalf("unexpected task mapping: %#v", owned)
	}

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

	status := createdTaskStatus(t, "get-2", pending.Outgoing[0].To, "task-1", "user-1")
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

func TestDuplicateRepliesDoNotResendBusinessWork(t *testing.T) {
	svc, state := newTestService(t)
	recorded, _ := svc.Handle(context.Background(), state, createMessage(t, "message-1", "request-1", "task-1", "user-1", "hello"))
	state = applyDecision(t, svc, state, recorded)
	var call runtimesystem.Call
	_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
	var declaration instance.Declaration
	_ = json.Unmarshal(call.Payload, &declaration)
	systemReply := successfulSystemResult(t, call, declaration)
	declared, _ := svc.Handle(context.Background(), state, systemReply)
	state = applyDecision(t, svc, state, declared)

	duplicateSystem, err := svc.Handle(context.Background(), state, systemReply)
	if err != nil {
		t.Fatal(err)
	}
	if len(duplicateSystem.Events)+len(duplicateSystem.Outgoing)+len(duplicateSystem.Effects) != 0 {
		t.Fatalf("duplicate system result sent a second task.create: %#v", duplicateSystem)
	}

	status := createdTaskStatus(t, "request-1", declaration.Address, "task-1", "user-1")
	completed, _ := svc.Handle(context.Background(), state, status)
	state = applyDecision(t, svc, state, completed)
	duplicateStatus, err := svc.Handle(context.Background(), state, status)
	if err != nil {
		t.Fatal(err)
	}
	if len(duplicateStatus.Events)+len(duplicateStatus.Outgoing)+len(duplicateStatus.Effects) != 0 {
		t.Fatalf("duplicate task status repeated terminal work: %#v", duplicateStatus)
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
	completed, _ := svc.Handle(context.Background(), state2, createdTaskStatus(t, "request-1", declaration.Address, "task-1", "user-1"))

	events := []service.NewEvent{recorded.Events[0], declared.Events[0], completed.Events[0]}
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
}

func newTestService(t *testing.T) (*webGatewayService, service.State) {
	t.Helper()
	svc := &webGatewayService{address: DefaultAddress, instanceID: "gateway-1", clock: fixedClock{fixedTime()}}
	state, err := svc.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}
	return svc, state
}

func completedCreateState(t *testing.T) (*webGatewayService, service.State) {
	t.Helper()
	svc, state := newTestService(t)
	recorded, err := svc.Handle(context.Background(), state, createMessage(t, "message-1", "request-1", "task-1", "user-1", "hello"))
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, recorded)
	var call runtimesystem.Call
	_ = json.Unmarshal(recorded.Outgoing[0].Payload, &call)
	var declaration instance.Declaration
	_ = json.Unmarshal(call.Payload, &declaration)
	declared, err := svc.Handle(context.Background(), state, successfulSystemResult(t, call, declaration))
	if err != nil {
		t.Fatal(err)
	}
	state = applyDecision(t, svc, state, declared)
	completed, err := svc.Handle(context.Background(), state, createdTaskStatus(t, "request-1", declaration.Address, "task-1", "user-1"))
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

func createdTaskStatus(t *testing.T, requestID string, from contract.ServiceAddress, taskID, userID string) contract.Message {
	t.Helper()
	now := fixedTime()
	payload, err := json.Marshal(task.StatusResponse{Task: &task.State{
		TaskID: taskID, UserID: userID, OwnerAddress: DefaultAddress, Phase: task.PhaseCreated,
		Input: "hello", IdentityFingerprint: "task-fingerprint", CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return contract.Message{
		Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
		From: from, To: DefaultAddress, CorrelationID: requestID, UserID: userID, Payload: payload,
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
		t.Fatalf("unexpected error presentation: %#v", presentation)
	}
}

func fixedTime() time.Time {
	return time.Date(2026, 7, 23, 8, 0, 0, 0, time.UTC)
}

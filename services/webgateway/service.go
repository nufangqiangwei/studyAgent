package webgateway

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/service"
	runtimesystem "agent/serviceruntime/system"
	"agent/services/task"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	errInvalidRequest    = "web_task_invalid_request"
	errRequestConflict   = "web_task_conflict"
	errTaskNotFound      = "web_task_not_found"
	errDeclarationFailed = "web_task_declaration_failed"
	errTaskRequestFailed = "web_task_request_failed"
	errDeadlineExpired   = "web_task_deadline_expired"
	errAgentUnavailable  = "web_task_agent_unavailable"
)

type webGatewayService struct {
	address      contract.ServiceAddress
	instanceID   contract.ServiceInstanceID
	clock        contract.Clock
	defaultAgent contract.ServiceAddress
}

func (*webGatewayService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: Component, StateSchema: StateSchema}
}

func (*webGatewayService) InitialState(context.Context, service.Init) (service.State, error) {
	return encodeState(initialState())
}

func (s *webGatewayService) Handle(_ context.Context, raw service.State, message contract.Message) (service.Decision, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.Decision{}, err
	}
	switch {
	case message.Kind == contract.MessageCommand && message.Type == CreateTaskMessageType && message.Version == ProtocolVersion:
		return s.handleCreate(state, message)
	case message.Kind == contract.MessageCommand && message.Type == GetTaskMessageType && message.Version == ProtocolVersion:
		return s.handleGet(state, message)
	case message.Kind == contract.MessageReply && message.Type == runtimesystem.ResultMessageType && message.Version == runtimesystem.CallVersion:
		return s.handleSystemResult(state, message)
	case message.Kind == contract.MessageReply && message.Type == task.StatusMessageType && message.Version == task.ProtocolVersion:
		return s.handleTaskStatus(state, message)
	case message.Kind == contract.MessageEvent && message.Type == task.StatusChangedEventType && message.Version == task.ProtocolVersion:
		return s.handleTaskEvent(state, message)
	case message.Kind == contract.MessageEvent && message.Type == task.CompletedEventType && message.Version == task.ProtocolVersion:
		return s.handleTaskEvent(state, message)
	case message.Kind == contract.MessageEvent && message.Type == task.FailedEventType && message.Version == task.ProtocolVersion:
		return s.handleTaskEvent(state, message)
	case message.Kind == contract.MessageEvent && message.Type == task.CancelledEventType && message.Version == task.ProtocolVersion:
		return s.handleTaskEvent(state, message)
	default:
		return service.Decision{}, fmt.Errorf("unsupported web gateway message %s %q v%d", message.Kind, message.Type, message.Version)
	}
}

func (s *webGatewayService) handleCreate(state State, message contract.Message) (service.Decision, error) {
	var input CreateTaskRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return s.invalidPresentation(OperationCreate, message.ID, "task create payload is invalid")
	}
	input.RequestID = strings.TrimSpace(input.RequestID)
	input.TaskID = strings.TrimSpace(input.TaskID)
	input.GoalID = strings.TrimSpace(input.GoalID)
	input.Title = strings.TrimSpace(input.Title)
	input.Input = strings.TrimSpace(input.Input)
	userID := strings.TrimSpace(message.UserID)
	if input.RequestID == "" || userID == "" {
		return s.invalidPresentation(OperationCreate, stableFallbackID(input.RequestID, message.ID), "request id and user id are required")
	}
	if input.GoalID == "" {
		input.GoalID = strings.TrimSpace(message.GoalID)
	} else if message.GoalID != "" && input.GoalID != strings.TrimSpace(message.GoalID) {
		return s.invalidPresentation(OperationCreate, input.RequestID, "task goal does not match message context")
	}
	if (input.Input == "") == (input.InputArtifact == nil) {
		return s.invalidPresentation(OperationCreate, input.RequestID, "exactly one task input is required")
	}
	if len(input.Input) > maxInlineTaskInputBytes {
		return s.invalidPresentation(OperationCreate, input.RequestID, "inline task input is too large; use an input artifact")
	}
	if input.InputArtifact != nil {
		if err := artifact.ValidateRef(*input.InputArtifact); err != nil {
			return s.invalidPresentation(OperationCreate, input.RequestID, "task input artifact is invalid")
		}
	}
	if input.Deadline != nil {
		input.Deadline = cloneTime(input.Deadline)
		if !input.Deadline.After(s.now()) {
			return s.invalidPresentation(OperationCreate, input.RequestID, "task deadline has expired")
		}
	}
	if input.TaskID == "" {
		input.TaskID = derivedTaskID(input.RequestID)
	}
	taskAddress, taskInstanceID := stableTaskIdentity(input.TaskID, input.RequestID)
	fingerprint, err := createFingerprint(userID, input)
	if err != nil {
		return service.Decision{}, err
	}
	if existing, found := state.Requests[input.RequestID]; found {
		return s.duplicateDecision(existing, OperationCreate, fingerprint)
	}
	now := s.now()
	request := RequestState{
		RequestID: input.RequestID, Operation: OperationCreate, UserID: userID,
		TaskID: input.TaskID, TaskAddress: taskAddress, TaskInstanceID: taskInstanceID,
		Phase: PhaseDeclaringTask, IdentityFingerprint: fingerprint,
		DeclarationCallID: "web-task-declare/" + digest(input.RequestID+"\x00"+fingerprint),
		GoalID:            input.GoalID, Title: input.Title, Input: input.Input,
		InputArtifact: cloneArtifact(input.InputArtifact), Deadline: cloneTime(input.Deadline),
		CreatedAt: now, UpdatedAt: now,
	}
	// Reject concurrent create requests for the same TaskID that is still
	// in-flight.  OwnedTask is only written when the Saga succeeds, so
	// without this check two concurrent requests to the same TaskID could
	// both enter PhaseDeclaringTask and race through the entire Saga.
	for _, existing := range state.Requests {
		if existing.Operation == OperationCreate &&
			existing.TaskID == request.TaskID &&
			!existing.Phase.terminal() &&
			existing.RequestID != request.RequestID {
			return s.recordNewFailure(request, errRequestConflict,
				"a request for this task id is already in progress")
		}
	}

	if owned, exists := state.Tasks[request.TaskID]; exists {

		if owned.UserID != request.UserID {
			return s.recordNewFailure(request, errRequestConflict, "task id is already bound to an existing task")
		}
		request.TaskAddress = owned.Address
		request.TaskInstanceID = owned.InstanceID
		request.DeclarationCallID = ""
		request.Phase = PhaseWaitingTask
		event, err := newRequestEvent("web-task-request-recorded/"+request.RequestID, requestRecordedEvent, request, nil)
		if err != nil {
			return service.Decision{}, err
		}
		taskPayload, err := json.Marshal(task.CreateRequest{
			TaskID: request.TaskID, GoalID: request.GoalID, Title: request.Title,
			Input: request.Input, InputArtifact: cloneArtifact(request.InputArtifact), Deadline: cloneTime(request.Deadline),
		})
		if err != nil {
			return service.Decision{}, err
		}
		return service.Decision{
			Events: []service.NewEvent{event},
			Outgoing: []service.OutgoingMessage{{
				Key:  "web-task-create/" + request.RequestID,
				Kind: contract.MessageCommand, Type: task.CreateMessageType, Version: task.ProtocolVersion,
				To: request.TaskAddress, ReplyTo: s.address, CorrelationID: request.RequestID,
				Deadline: cloneTime(request.Deadline), Payload: taskPayload,
			}},
		}, nil
	}
	declarationPayload, err := json.Marshal(instance.Declaration{
		InstanceID: request.TaskInstanceID,
		Address:    request.TaskAddress,
		Component:  task.Component,
		ParentID:   s.instanceID,
		Metadata: map[string]string{
			"task_id": request.TaskID, "request_id": request.RequestID, "user_id": request.UserID,
		},
	})
	if err != nil {
		return service.Decision{}, err
	}
	callPayload, err := json.Marshal(runtimesystem.Call{
		CallID: request.DeclarationCallID, Operation: runtimesystem.DeclareInstanceOperation,
		OperationVersion: 1, Payload: declarationPayload,
	})
	if err != nil {
		return service.Decision{}, err
	}
	event, err := newRequestEvent("web-task-request-recorded/"+request.RequestID, requestRecordedEvent, request, nil)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{event},
		Outgoing: []service.OutgoingMessage{{
			Key:  "web-task-declare/" + request.RequestID,
			Kind: contract.MessageCommand, Type: runtimesystem.CallMessageType, Version: runtimesystem.CallVersion,
			To: runtimesystem.Address, ReplyTo: s.address, CorrelationID: request.RequestID, Payload: callPayload,
		}},
	}, nil
}

func (s *webGatewayService) handleGet(state State, message contract.Message) (service.Decision, error) {
	var input GetTaskRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return s.invalidPresentation(OperationGet, message.ID, "task get payload is invalid")
	}
	input.RequestID = strings.TrimSpace(input.RequestID)
	input.TaskID = strings.TrimSpace(input.TaskID)
	userID := strings.TrimSpace(message.UserID)
	if input.RequestID == "" || input.TaskID == "" || userID == "" {
		return s.invalidPresentation(OperationGet, stableFallbackID(input.RequestID, message.ID), "request id, task id, and user id are required")
	}
	fingerprint, err := getFingerprint(userID, input)
	if err != nil {
		return service.Decision{}, err
	}
	if existing, found := state.Requests[input.RequestID]; found {
		return s.duplicateDecision(existing, OperationGet, fingerprint)
	}
	now := s.now()
	request := RequestState{
		RequestID: input.RequestID, Operation: OperationGet, UserID: userID, TaskID: input.TaskID,
		Phase: PhaseWaitingTask, IdentityFingerprint: fingerprint, CreatedAt: now, UpdatedAt: now,
	}
	owned, found := state.Tasks[input.TaskID]
	if !found || owned.UserID != userID {
		return s.recordNewFailure(request, errTaskNotFound, "task was not found")
	}
	request.TaskAddress, request.TaskInstanceID = owned.Address, owned.InstanceID
	payload, err := json.Marshal(task.GetRequest{TaskID: request.TaskID})
	if err != nil {
		return service.Decision{}, err
	}
	event, err := newRequestEvent("web-task-request-recorded/"+request.RequestID, requestRecordedEvent, request, nil)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{event},
		Outgoing: []service.OutgoingMessage{{
			Key:  "web-task-get/" + request.RequestID,
			Kind: contract.MessageQuery, Type: task.GetMessageType, Version: task.ProtocolVersion,
			To: request.TaskAddress, ReplyTo: s.address, CorrelationID: request.RequestID, Payload: payload,
		}},
	}, nil
}

func (s *webGatewayService) handleSystemResult(state State, message contract.Message) (service.Decision, error) {
	if message.From != runtimesystem.Address {
		return service.Decision{}, fmt.Errorf("system result came from unexpected address %q", message.From)
	}
	callID := strings.TrimSpace(message.Metadata[runtimesystem.MetadataCallID])
	request, found := findDeclarationRequest(state, callID)
	if !found {
		return service.Decision{}, nil
	}
	if request.Phase != PhaseDeclaringTask {
		return service.Decision{}, nil
	}
	if isReplyError(message) {
		return s.failPending(request, errDeclarationFailed, "task instance declaration failed", false)
	}
	var result runtimesystem.Result
	if err := json.Unmarshal(message.Payload, &result); err != nil {
		return service.Decision{}, fmt.Errorf("decode system result: %w", err)
	}
	if result.CallID != request.DeclarationCallID || result.Operation != runtimesystem.DeclareInstanceOperation || result.OperationVersion != 1 {
		return service.Decision{}, fmt.Errorf("system result does not match task declaration %q", request.DeclarationCallID)
	}
	var declared runtimesystem.DeclareInstanceResult
	if err := json.Unmarshal(result.Result, &declared); err != nil {
		return service.Decision{}, fmt.Errorf("decode declared task instance: %w", err)
	}
	if declared.Instance.InstanceID != request.TaskInstanceID ||
		declared.Instance.Address != request.TaskAddress ||
		declared.Instance.DefinitionRef != task.Component {
		return service.Decision{}, fmt.Errorf("declared task instance does not match request %q", request.RequestID)
	}
	if request.Deadline != nil && !request.Deadline.After(s.now()) {
		return s.failPending(request, errDeadlineExpired, "task deadline has expired", false)
	}
	next := request.clone()
	next.Phase, next.UpdatedAt = PhaseWaitingTask, s.now()
	event, err := newRequestEvent("web-task-declaration-completed/"+request.RequestID, taskDeclarationCompletedEvent, next, nil)
	if err != nil {
		return service.Decision{}, err
	}
	taskPayload, err := json.Marshal(task.CreateRequest{
		TaskID: request.TaskID, GoalID: request.GoalID, Title: request.Title,
		Input: request.Input, InputArtifact: cloneArtifact(request.InputArtifact), Deadline: cloneTime(request.Deadline),
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{event},
		Outgoing: []service.OutgoingMessage{{
			Key:  "web-task-create/" + request.RequestID,
			Kind: contract.MessageCommand, Type: task.CreateMessageType, Version: task.ProtocolVersion,
			To: request.TaskAddress, ReplyTo: s.address, CorrelationID: request.RequestID,
			Deadline: cloneTime(request.Deadline), Payload: taskPayload,
		}},
	}, nil
}

func (s *webGatewayService) handleTaskStatus(state State, message contract.Message) (service.Decision, error) {
	requestID := strings.TrimSpace(message.CorrelationID)
	request, found := state.Requests[requestID]
	if !found {
		return service.Decision{}, nil
	}
	if message.From != request.TaskAddress {
		return service.Decision{}, fmt.Errorf("task status for request %q came from unexpected address %q", requestID, message.From)
	}
	if isReplyError(message) {
		return s.handleTaskStatusError(request, message)
	}
	var status task.StatusResponse
	if err := json.Unmarshal(message.Payload, &status); err != nil {
		return service.Decision{}, fmt.Errorf("decode task status: %w", err)
	}
	if status.Task == nil || status.Task.TaskID != request.TaskID ||
		status.Task.OwnerAddress != s.address || status.Task.UserID != request.UserID {
		return service.Decision{}, fmt.Errorf("task status does not match gateway request %q", request.RequestID)
	}

	// For get operations, the original behavior: success when task status is received.
	if request.Operation == OperationGet {
		if request.Phase != PhaseWaitingTask {
			return service.Decision{}, nil
		}
		result := taskDTOFromState(status.Task.Clone())
		if err := result.validate(); err != nil {
			return service.Decision{}, fmt.Errorf("validate task status: %w", err)
		}
		return s.succeedPending(state, request, result)
	}

	// For create operations, drive the saga based on current task phase and gateway phase.
	if request.Operation != OperationCreate {
		return service.Decision{}, nil
	}
	return s.progressCreateSaga(state, request, status)
}

func (s *webGatewayService) handleTaskStatusError(request RequestState, message contract.Message) (service.Decision, error) {
	var replyError service.ReplyError
	if err := json.Unmarshal(message.Payload, &replyError); err != nil {
		return service.Decision{}, fmt.Errorf("decode task error reply: %w", err)
	}
	if request.Operation == OperationCreate && request.Phase == PhaseResolvingTerminal {
		// No late command/query error can supersede a terminal owner fact that
		// already committed. Re-read the authoritative Task state instead.
		return s.reSyncTaskState(request)
	}
	if request.Operation == OperationGet && (replyError.Code == "task_not_found" || replyError.Code == "task_access_denied") {
		return s.failPending(request, errTaskNotFound, "task was not found", false)
	}
	if request.Operation == OperationCreate && replyError.Code == "task_conflict" {
		return s.failPending(request, errRequestConflict, "task id is already bound to different content", false)
	}
	// At-least-once delivery may replay a saga-step command after the task has
	// already advanced.  Instead of failing, re-read the task's current state
	// and let progressCreateSaga fast-forward.
	if request.Operation == OperationCreate && replyError.Code == "task_invalid_transition" {
		return s.reSyncTaskState(request)
	}
	return s.failPending(request, errTaskRequestFailed, "task service rejected the request", replyError.Retryable)
}

// handleTaskEvent acknowledges Task owner events. Status-change events remain
// outside Gateway state (the full Projection belongs to issue #37). A terminal
// event that races ahead of task.start's Running reply is different: the Saga
// records only that minimum terminal fact and atomically requests task.get so
// it can finish from Task Service's authoritative full State.
func (s *webGatewayService) handleTaskEvent(state State, message contract.Message) (service.Decision, error) {
	taskID := strings.TrimSpace(message.CorrelationID)
	if taskID == "" {
		return service.Decision{}, nil
	}
	request, pending, err := taskEventRequest(state, taskID, message.From)
	if err != nil {
		return service.Decision{}, err
	}
	if message.Type == task.StatusChangedEventType || !pending {
		return service.Decision{}, nil
	}

	var result task.Result
	if err := json.Unmarshal(message.Payload, &result); err != nil {
		return service.Decision{}, fmt.Errorf("decode task terminal event: %w", err)
	}
	observation := terminalObservation{MessageType: message.Type, Result: result}
	if err := observation.validate(request); err != nil {
		return service.Decision{}, fmt.Errorf("validate task terminal event: %w", err)
	}
	if request.Phase == PhaseResolvingTerminal {
		if !sameTerminalObservation(request.Terminal, &observation) {
			return service.Decision{}, fmt.Errorf("task terminal event conflicts with the fact already recorded for request %q", request.RequestID)
		}
		return service.Decision{}, nil
	}

	now := s.now()
	next := request.clone()
	next.Phase, next.Terminal, next.UpdatedAt = PhaseResolvingTerminal, cloneTerminalObservation(&observation), now
	event, err := newRequestEvent(
		"web-task-terminal-observed/"+request.RequestID+"/"+string(result.Phase),
		taskTerminalObservedEvent,
		next,
		nil,
	)
	if err != nil {
		return service.Decision{}, err
	}
	query, err := s.taskStateQuery(next, "web-task-terminal-refresh/"+request.RequestID)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Outgoing: []service.OutgoingMessage{query}}, nil
}

// reSyncTaskState sends a task.get query to re-read the task's current phase
// after a task_invalid_transition error from an At-least-once duplicate command.
// The reply flows through handleTaskStatus → progressCreateSaga, which will
// fast-forward based on the actual task phase.
func (s *webGatewayService) reSyncTaskState(request RequestState) (service.Decision, error) {
	query, err := s.taskStateQuery(request, "web-task-resync/"+request.RequestID)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Outgoing: []service.OutgoingMessage{query}}, nil
}

func (s *webGatewayService) taskStateQuery(request RequestState, key string) (service.OutgoingMessage, error) {
	payload, err := json.Marshal(task.GetRequest{TaskID: request.TaskID})
	if err != nil {
		return service.OutgoingMessage{}, err
	}
	return service.OutgoingMessage{
		Key:  key,
		Kind: contract.MessageQuery, Type: task.GetMessageType, Version: task.ProtocolVersion,
		To: request.TaskAddress, ReplyTo: s.address, CorrelationID: request.RequestID, Payload: payload,
	}, nil
}

// progressCreateSaga advances the task creation saga from created through mark_ready, assign, start to running.
// It handles late/duplicate replies by ignoring task phases that belong to previous steps,
// and handles tasks that are already ahead of the current saga step by skipping forward.
func (s *webGatewayService) progressCreateSaga(state State, request RequestState, status task.StatusResponse) (service.Decision, error) {
	taskState := status.Task.Clone()

	switch request.Phase {
	case PhaseWaitingTask:
		// After task.create, the task should be in Created or beyond.
		switch {
		case taskState.Phase == task.PhaseCreated:
			return s.advanceToMarkReady(request)
		case taskState.Phase == task.PhaseReady && taskState.AssignedTo != "":
			// Task is already ready and assigned; skip mark_ready and assign.
			return s.advanceToStart(request)
		case taskState.Phase == task.PhaseReady:
			// Task is already ready; skip mark_ready.
			return s.advanceToAssign(request)
		case taskState.Phase == task.PhaseRunning || taskState.Phase.Terminal():
			// Task is already executing or done; succeed directly.
			return s.fastForwardSuccess(state, request, taskState)
		default:
			return service.Decision{}, fmt.Errorf("task create request %q returned unexpected phase %q", request.RequestID, taskState.Phase)
		}

	case PhaseMarkingReady:
		// After task.mark_ready, the task should be in Ready or beyond.
		switch {
		case taskState.Phase == task.PhaseReady && taskState.AssignedTo != "":
			// Already assigned; skip assign step.
			return s.advanceToStart(request)
		case taskState.Phase == task.PhaseReady:
			return s.advanceToAssign(request)
		case taskState.Phase == task.PhaseRunning || taskState.Phase.Terminal():
			return s.fastForwardSuccess(state, request, taskState)
		case taskState.Phase == task.PhaseCreated:
			// Duplicate/late reply for task.create; idempotently ignore.
			return service.Decision{}, nil
		default:
			return service.Decision{}, fmt.Errorf("task mark ready request %q returned unexpected phase %q", request.RequestID, taskState.Phase)
		}

	case PhaseAssigning:
		// After task.assign, the task should have AssignedTo set or be Running.
		switch {
		case taskState.AssignedTo != "" && (taskState.Phase == task.PhaseReady || taskState.Phase == task.PhaseCreated):
			return s.advanceToStart(request)
		case taskState.Phase == task.PhaseRunning || taskState.Phase.Terminal():
			return s.fastForwardSuccess(state, request, taskState)
		case taskState.Phase == task.PhaseReady && taskState.AssignedTo == "":
			// Duplicate/late reply for mark_ready; idempotently ignore.
			return service.Decision{}, nil
		case taskState.Phase == task.PhaseCreated:
			// Duplicate/late reply for task.create; idempotently ignore.
			return service.Decision{}, nil
		default:
			return service.Decision{}, fmt.Errorf("task assign request %q returned unexpected state phase=%q assigned=%q", request.RequestID, taskState.Phase, taskState.AssignedTo)
		}

	case PhaseStarting:
		// After task.start, the task should be in Running.
		switch {
		case taskState.Phase == task.PhaseRunning:
			if taskState.ActiveRunID == "" {
				return service.Decision{}, fmt.Errorf("task start request %q did not set active run id", request.RequestID)
			}
			return s.fastForwardSuccess(state, request, taskState)
		case taskState.Phase.Terminal():
			return s.fastForwardSuccess(state, request, taskState)
		default:
			// Duplicate/late reply for a previous step; idempotently ignore.
			return service.Decision{}, nil
		}
	case PhaseResolvingTerminal:
		// task.start's Running reply may have been queued before the terminal
		// event. It cannot supersede the durable terminal observation.
		if !taskState.Phase.Terminal() {
			return service.Decision{}, nil
		}
		actual, err := terminalObservationFromState(taskState)
		if err != nil {
			return service.Decision{}, err
		}
		if !sameTerminalObservation(request.Terminal, &actual) {
			return service.Decision{}, fmt.Errorf(
				"authoritative task state conflicts with terminal fact for request %q",
				request.RequestID,
			)
		}
		return s.fastForwardSuccess(state, request, taskState)
	default:
		// Request already in a non-creating phase; idempotently ignore.
		return service.Decision{}, nil
	}
}

func (s *webGatewayService) fastForwardSuccess(state State, request RequestState, taskState task.State) (service.Decision, error) {
	result := taskDTOFromState(taskState)
	if err := result.validate(); err != nil {
		return service.Decision{}, fmt.Errorf("validate task status: %w", err)
	}
	return s.succeedPending(state, request, result)
}

func (s *webGatewayService) advanceToMarkReady(request RequestState) (service.Decision, error) {
	if request.Deadline != nil && !request.Deadline.After(s.now()) {
		return s.failPending(request, errDeadlineExpired, "task deadline has expired", false)
	}
	now := s.now()
	next := request.clone()
	next.Phase, next.UpdatedAt = PhaseMarkingReady, now
	event, err := newRequestEvent("web-task-mark-ready-event/"+request.RequestID, taskMarkedReadyEvent, next, nil)
	if err != nil {
		return service.Decision{}, err
	}
	payload, err := json.Marshal(json.RawMessage("{}"))
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{event},
		Outgoing: []service.OutgoingMessage{{
			Key:  "web-task-mark-ready/" + request.RequestID,
			Kind: contract.MessageCommand, Type: task.MarkReadyMessageType, Version: task.ProtocolVersion,
			To: request.TaskAddress, ReplyTo: s.address, CorrelationID: request.RequestID,
			Deadline: cloneTime(request.Deadline), Payload: payload,
		}},
	}, nil
}

func (s *webGatewayService) advanceToAssign(request RequestState) (service.Decision, error) {
	if request.Deadline != nil && !request.Deadline.After(s.now()) {
		return s.failPending(request, errDeadlineExpired, "task deadline has expired", false)
	}
	if s.defaultAgent == "" {
		return s.failPending(request, errAgentUnavailable, "no default agent is configured for the web gateway", false)
	}
	now := s.now()
	next := request.clone()
	next.Phase, next.UpdatedAt = PhaseAssigning, now
	event, err := newRequestEvent("web-task-assign-event/"+request.RequestID, taskAssignedEvent, next, nil)
	if err != nil {
		return service.Decision{}, err
	}
	assignPayload, err := json.Marshal(task.AssignRequest{AgentAddress: s.defaultAgent})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{event},
		Outgoing: []service.OutgoingMessage{{
			Key:  "web-task-assign/" + request.RequestID,
			Kind: contract.MessageCommand, Type: task.AssignMessageType, Version: task.ProtocolVersion,
			To: request.TaskAddress, ReplyTo: s.address, CorrelationID: request.RequestID,
			Deadline: cloneTime(request.Deadline), Payload: assignPayload,
		}},
	}, nil
}

func (s *webGatewayService) advanceToStart(request RequestState) (service.Decision, error) {
	if request.Deadline != nil && !request.Deadline.After(s.now()) {
		return s.failPending(request, errDeadlineExpired, "task deadline has expired", false)
	}
	now := s.now()
	next := request.clone()
	next.Phase, next.UpdatedAt = PhaseStarting, now
	event, err := newRequestEvent("web-task-start-event/"+request.RequestID, taskStartRequestedEvent, next, nil)
	if err != nil {
		return service.Decision{}, err
	}
	payload, err := json.Marshal(json.RawMessage("{}"))
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{event},
		Outgoing: []service.OutgoingMessage{{
			Key:  "web-task-start/" + request.RequestID,
			Kind: contract.MessageCommand, Type: task.StartMessageType, Version: task.ProtocolVersion,
			To: request.TaskAddress, ReplyTo: s.address, CorrelationID: request.RequestID,
			Deadline: cloneTime(request.Deadline), Payload: payload,
		}},
	}, nil
}

func (s *webGatewayService) duplicateDecision(existing RequestState, incomingOperation Operation, fingerprint string) (service.Decision, error) {
	if existing.IdentityFingerprint != fingerprint {
		return s.conflictPresentation(existing.RequestID, incomingOperation, fingerprint)
	}
	if existing.Phase.terminal() {
		return s.presentationOnly(presentationForRequest(existing))
	}
	return service.Decision{}, nil
}

func (s *webGatewayService) succeedPending(state State, request RequestState, result TaskDTO) (service.Decision, error) {
	now := s.now()
	next := request.clone()
	next.Phase, next.Task, next.Error = PhaseSucceeded, &result, nil
	next.Terminal = nil
	next.UpdatedAt, next.CompletedAt = now, cloneTime(&now)
	next.PresentationID = presentationID(next.RequestID, next.Operation, "success")
	var owned *OwnedTask
	if next.Operation == OperationCreate {
		value, found := state.Tasks[next.TaskID]
		if !found {
			value = OwnedTask{
				TaskID: next.TaskID, UserID: next.UserID, Address: next.TaskAddress,
				InstanceID: next.TaskInstanceID, CreatedByRequestID: next.RequestID,
			}
		}
		owned = &value
	}
	event, err := newRequestEvent("web-task-request-succeeded/"+next.RequestID, requestSucceededEvent, next, owned)
	if err != nil {
		return service.Decision{}, err
	}
	presentation, err := plannedPresentation(presentationForRequest(next))
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Effects: []service.PlannedEffect{presentation}}, nil
}

func (s *webGatewayService) failPending(request RequestState, code, message string, retryable bool) (service.Decision, error) {
	return s.failureDecision(request, requestFailedEvent, code, message, retryable)
}

func (s *webGatewayService) recordNewFailure(request RequestState, code, message string) (service.Decision, error) {
	return s.failureDecision(request, requestRecordedEvent, code, message, false)
}

func (s *webGatewayService) failureDecision(request RequestState, eventType contract.EventType, code, message string, retryable bool) (service.Decision, error) {
	now := s.now()
	next := request.clone()
	next.Phase, next.Task = PhaseFailed, nil
	next.Terminal = nil
	next.Error = &ErrorDTO{Code: code, Message: message, Retryable: retryable}
	next.UpdatedAt, next.CompletedAt = now, cloneTime(&now)
	next.PresentationID = presentationID(next.RequestID, next.Operation, "error/"+code)
	event, err := newRequestEvent("web-task-request-failed/"+next.RequestID+"/"+code, eventType, next, nil)
	if err != nil {
		return service.Decision{}, err
	}
	presentation, err := plannedPresentation(presentationForRequest(next))
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Effects: []service.PlannedEffect{presentation}}, nil
}

func (s *webGatewayService) invalidPresentation(operation Operation, requestID, message string) (service.Decision, error) {
	requestID = stableFallbackID(requestID, "invalid")
	return s.presentationOnly(Presentation{
		PresentationID: presentationID(requestID, operation, "error/"+errInvalidRequest),
		RequestID:      requestID, Operation: operation,
		Error: &ErrorDTO{Code: errInvalidRequest, Message: message},
	})
}

func (s *webGatewayService) conflictPresentation(requestID string, operation Operation, fingerprint string) (service.Decision, error) {
	return s.presentationOnly(Presentation{
		PresentationID: presentationID(requestID, operation, "conflict/"+digest(fingerprint)[:16]),
		RequestID:      requestID, Operation: operation,
		Error: &ErrorDTO{Code: errRequestConflict, Message: "request id is already bound to different operation, user, or payload"},
	})
}

func (s *webGatewayService) presentationOnly(presentation Presentation) (service.Decision, error) {
	planned, err := plannedPresentation(presentation)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Effects: []service.PlannedEffect{planned}}, nil
}

func presentationForRequest(request RequestState) Presentation {
	presentation := Presentation{
		PresentationID: request.PresentationID, RequestID: request.RequestID, Operation: request.Operation,
	}
	if request.Phase == PhaseSucceeded && request.Task != nil {
		if request.Operation == OperationCreate {
			presentation.Created = &TaskCreatedPresentation{RequestID: request.RequestID, Task: request.Task.clone()}
		} else {
			presentation.Found = &TaskFoundPresentation{RequestID: request.RequestID, Task: request.Task.clone()}
		}
	} else if request.Error != nil {
		value := *request.Error
		presentation.Error = &value
	}
	return presentation
}

func plannedPresentation(presentation Presentation) (service.PlannedEffect, error) {
	if err := presentation.validate(); err != nil {
		return service.PlannedEffect{}, err
	}
	payload, err := json.Marshal(presentation.clone())
	if err != nil {
		return service.PlannedEffect{}, fmt.Errorf("encode web task presentation: %w", err)
	}
	return service.PlannedEffect{
		Key:  "web-task-presentation/" + presentation.PresentationID,
		Type: PresentationEffectType, Version: ProtocolVersion, ExecutorRef: PresentationExecutorRef,
		IdempotencyKey: "web-task/presentation/" + presentation.PresentationID, Payload: payload,
	}, nil
}

func newRequestEvent(key string, eventType contract.EventType, request RequestState, owned *OwnedTask) (service.NewEvent, error) {
	payload, err := json.Marshal(requestEventPayload{Request: request.clone(), Task: owned})
	if err != nil {
		return service.NewEvent{}, err
	}
	return service.NewEvent{Key: key, Type: eventType, Version: ProtocolVersion, Payload: payload}, nil
}

func findDeclarationRequest(state State, callID string) (RequestState, bool) {
	if callID == "" {
		return RequestState{}, false
	}
	for _, request := range state.Requests {
		if request.DeclarationCallID == callID {
			return request.clone(), true
		}
	}
	return RequestState{}, false
}

func taskEventRequest(
	state State,
	taskID string,
	from contract.ServiceAddress,
) (RequestState, bool, error) {
	if owned, found := state.Tasks[taskID]; found && owned.Address != from {
		return RequestState{}, false, fmt.Errorf(
			"task event for %q came from unexpected address %q", taskID, from,
		)
	}
	var pending RequestState
	foundPending := false
	for _, request := range state.Requests {
		if request.TaskID != taskID || request.Operation != OperationCreate || request.Phase.terminal() {
			continue
		}
		if request.TaskAddress != from {
			return RequestState{}, false, fmt.Errorf(
				"task event for request %q came from unexpected address %q",
				request.RequestID,
				from,
			)
		}
		if foundPending {
			return RequestState{}, false, fmt.Errorf("multiple create requests are in flight for task %q", taskID)
		}
		pending, foundPending = request.clone(), true
	}
	return pending, foundPending, nil
}

func terminalObservationFromState(value task.State) (terminalObservation, error) {
	if !value.Phase.Terminal() || value.CompletedAt == nil {
		return terminalObservation{}, fmt.Errorf("task %q is not terminal", value.TaskID)
	}
	messageType, ok := terminalMessageType(value.Phase)
	if !ok {
		return terminalObservation{}, fmt.Errorf("task %q has unsupported terminal phase %q", value.TaskID, value.Phase)
	}
	result := task.Result{
		TaskID: value.TaskID, GoalID: value.GoalID, Phase: value.Phase, Attempt: value.Attempt,
		ResultRef: cloneArtifact(value.ResultRef), CompletedAt: value.CompletedAt.UTC(),
	}
	if value.LastError != nil {
		errValue := *value.LastError
		result.Error = &errValue
	}
	return terminalObservation{MessageType: messageType, Result: result}, nil
}

func sameTerminalObservation(left, right *terminalObservation) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.MessageType != right.MessageType ||
		left.Result.TaskID != right.Result.TaskID ||
		left.Result.GoalID != right.Result.GoalID ||
		left.Result.Phase != right.Result.Phase ||
		left.Result.Attempt != right.Result.Attempt ||
		!left.Result.CompletedAt.Equal(right.Result.CompletedAt) {
		return false
	}
	if !sameArtifact(left.Result.ResultRef, right.Result.ResultRef) {
		return false
	}
	return sameTaskError(left.Result.Error, right.Result.Error)
}

func sameArtifact(left, right *contract.ArtifactRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameTaskError(left, right *task.Error) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Code == right.Code &&
		left.Message == right.Message &&
		left.Source == right.Source &&
		left.RunID == right.RunID &&
		left.Retryable == right.Retryable &&
		left.OccurredAt.Equal(right.OccurredAt)
}

func createFingerprint(userID string, input CreateTaskRequest) (string, error) {
	value := struct {
		Operation     Operation
		UserID        string
		RequestID     string
		TaskID        string
		GoalID        string
		Title         string
		Input         string
		InputArtifact *contract.ArtifactRef
		Deadline      *time.Time
	}{
		Operation: OperationCreate, UserID: userID, RequestID: input.RequestID, TaskID: input.TaskID,
		GoalID: input.GoalID, Title: input.Title, Input: input.Input,
		InputArtifact: cloneArtifact(input.InputArtifact), Deadline: cloneTime(input.Deadline),
	}
	return fingerprint(value)
}

func getFingerprint(userID string, input GetTaskRequest) (string, error) {
	return fingerprint(struct {
		Operation Operation
		UserID    string
		RequestID string
		TaskID    string
	}{OperationGet, userID, input.RequestID, input.TaskID})
}

func fingerprint(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return digest(string(payload)), nil
}

func stableTaskIdentity(taskID, requestID string) (contract.ServiceAddress, contract.ServiceInstanceID) {
	value := digest("webgateway-task/v1\x00" + taskID + "\x00" + requestID)
	return contract.ServiceAddress("task.web." + value[:32]), contract.ServiceInstanceID("task-web-" + value[:32])
}

func derivedTaskID(requestID string) string {
	return "task-" + digest("webgateway-task-id/v1\x00" + requestID)[:24]
}

func presentationID(requestID string, operation Operation, outcome string) string {
	return "web-task/" + string(operation) + "/" + digest(requestID)[:24] + "/" + outcome
}

func stableFallbackID(requestID, fallback string) string {
	if requestID = strings.TrimSpace(requestID); requestID != "" {
		return requestID
	}
	if fallback = strings.TrimSpace(fallback); fallback != "" {
		return fallback
	}
	return "unknown"
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func isReplyError(message contract.Message) bool {
	return message.Metadata[contract.MetadataReplyError] == "true"
}

func (s *webGatewayService) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock.Now().UTC()
}

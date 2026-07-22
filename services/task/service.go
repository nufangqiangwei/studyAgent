package task

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/agent"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	errInvalidRequest    = "task_invalid_request"
	errNotFound          = "task_not_found"
	errConflict          = "task_conflict"
	errAccessDenied      = "task_access_denied"
	errInvalidTransition = "task_invalid_transition"
	errDeadlineExpired   = "task_deadline_expired"
	errAgentRejected     = "task_agent_rejected"
	errAgentFailed       = "task_agent_failed"
	errCancelled         = "task_cancelled"
)

type taskService struct {
	address    contract.ServiceAddress
	instanceID contract.ServiceInstanceID
	clock      contract.Clock
}

func (s *taskService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: Component, StateSchema: StateSchema}
}

func (*taskService) InitialState(context.Context, service.Init) (service.State, error) {
	return encodeState(initialAggregateState())
}

func (s *taskService) Handle(_ context.Context, raw service.State, message contract.Message) (service.Decision, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.Decision{}, err
	}
	if message.Version != ProtocolVersion {
		return service.Decision{}, fmt.Errorf("unsupported task message version %d", message.Version)
	}
	switch {
	case message.Kind == contract.MessageCommand && message.Type == CreateMessageType:
		return s.handleCreate(state, message)
	case message.Kind == contract.MessageCommand && message.Type == MarkReadyMessageType:
		return s.handleMarkReady(state, message)
	case message.Kind == contract.MessageCommand && message.Type == AssignMessageType:
		return s.handleAssign(state, message)
	case message.Kind == contract.MessageCommand && message.Type == StartMessageType:
		return s.handleStart(state, message)
	case message.Kind == contract.MessageCommand && message.Type == SuspendMessageType:
		return s.handleSuspend(state, message)
	case message.Kind == contract.MessageCommand && message.Type == ResumeMessageType:
		return s.handleResume(state, message)
	case message.Kind == contract.MessageCommand && message.Type == RetryMessageType:
		return s.handleRetry(state, message)
	case message.Kind == contract.MessageCommand && message.Type == CancelMessageType:
		return s.handleCancel(state, message)
	case message.Kind == contract.MessageQuery && message.Type == GetMessageType:
		return s.handleGet(state, message)
	case message.Kind == contract.MessageEvent && message.Type == ExecutionWaitingMessageType:
		return s.handleExecutionWaiting(state, message)
	case message.Kind == contract.MessageEvent && message.Type == ExecutionResumedMessageType:
		return s.handleExecutionResumed(state, message)
	case message.Kind == contract.MessageReply && message.Type == agent.CompletedMessageType:
		return s.handleAgentCompleted(state, message)
	default:
		return service.Decision{}, fmt.Errorf("unsupported task message %s %q v%d", message.Kind, message.Type, message.Version)
	}
}

func (s *taskService) handleCreate(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.ReplyTo == "" || message.From == "" {
		return service.Decision{}, fmt.Errorf("task create requires from and reply_to")
	}
	var input CreateRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return reject(message, errInvalidRequest, "task create payload is invalid")
	}
	input.TaskID = strings.TrimSpace(input.TaskID)
	input.GoalID = strings.TrimSpace(input.GoalID)
	input.Title = strings.TrimSpace(input.Title)
	input.Input = strings.TrimSpace(input.Input)
	if input.GoalID == "" {
		input.GoalID = strings.TrimSpace(message.GoalID)
	} else if message.GoalID != "" && input.GoalID != message.GoalID {
		return reject(message, errInvalidRequest, "task goal does not match message context")
	}
	if input.TaskID == "" || (input.Input == "" && input.InputArtifact == nil) || (input.Input != "" && input.InputArtifact != nil) {
		return reject(message, errInvalidRequest, "task id and exactly one task input are required")
	}
	if len(input.Input) > maxInlineTaskInputBytes {
		return reject(message, errInvalidRequest, "inline task input is too large; use input_artifact")
	}
	if input.InputArtifact != nil {
		if err := artifact.ValidateRef(*input.InputArtifact); err != nil {
			return reject(message, errInvalidRequest, "task input artifact is invalid")
		}
	}
	now := s.now()
	input.Deadline = cloneTime(input.Deadline)
	if input.Deadline != nil && !input.Deadline.After(now) {
		return reject(message, errDeadlineExpired, "task deadline has expired")
	}
	fingerprint, err := taskFingerprint(input, message.From, message.UserID)
	if err != nil {
		return service.Decision{}, err
	}
	if state.Task != nil {
		if state.Task.TaskID == input.TaskID && state.Task.IdentityFingerprint == fingerprint {
			return statusDecision(*state.Task, "task-create-idempotent/"+input.TaskID), nil
		}
		return reject(message, errConflict, "task instance is already bound to another task")
	}
	task := State{
		TaskID: input.TaskID, GoalID: input.GoalID, UserID: strings.TrimSpace(message.UserID),
		OwnerAddress: message.From, Phase: PhaseCreated,
		Title: input.Title, Input: input.Input, InputArtifact: cloneArtifact(input.InputArtifact),
		Deadline: input.Deadline, IdentityFingerprint: fingerprint,
		CreatedAt: now, UpdatedAt: now,
	}
	payload := taskCreatedPayload{Task: task}
	event, next, err := decideEvent(nil, "task-created/"+task.TaskID, taskCreatedEvent, payload)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Reply: statusReply(*next, "task-created-status/"+task.TaskID)}, nil
}

func (s *taskService) handleMarkReady(state aggregateState, message contract.Message) (service.Decision, error) {
	task, decision, err := ownerCommand(state, message)
	if err != nil || decision != nil {
		return dereferenceDecision(decision), err
	}
	if task.Phase == PhaseReady {
		return statusDecision(task, "task-ready-idempotent/"+task.TaskID), nil
	}
	if task.Phase != PhaseCreated {
		return reject(message, errInvalidTransition, "only a created task can become ready")
	}
	event, next, err := decideEvent(&task, "task-ready/"+task.TaskID, taskReadyEvent, taskReadyPayload{ReadyAt: s.now()})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Reply: statusReply(*next, "task-ready-status/"+task.TaskID)}, nil
}

func (s *taskService) handleAssign(state aggregateState, message contract.Message) (service.Decision, error) {
	task, decision, err := ownerCommand(state, message)
	if err != nil || decision != nil {
		return dereferenceDecision(decision), err
	}
	var input AssignRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return reject(message, errInvalidRequest, "task assignment payload is invalid")
	}
	input.AgentAddress = contract.ServiceAddress(strings.TrimSpace(string(input.AgentAddress)))
	if input.AgentAddress == "" {
		return reject(message, errInvalidRequest, "agent address is required")
	}
	if task.AssignedTo == input.AgentAddress && (task.Phase == PhaseCreated || task.Phase == PhaseReady) {
		return statusDecision(task, "task-assign-idempotent/"+task.TaskID), nil
	}
	if task.Phase != PhaseCreated && task.Phase != PhaseReady {
		return reject(message, errInvalidTransition, "task can only be assigned before it starts")
	}
	event, next, err := decideEvent(&task, "task-assigned/"+task.TaskID, taskAssignedEvent, taskAssignedPayload{
		AssignedTo: input.AgentAddress, AssignedAt: s.now(),
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Reply: statusReply(*next, "task-assigned-status/"+task.TaskID)}, nil
}

func (s *taskService) handleStart(state aggregateState, message contract.Message) (service.Decision, error) {
	task, decision, err := ownerCommand(state, message)
	if err != nil || decision != nil {
		return dereferenceDecision(decision), err
	}
	if task.Phase == PhaseRunning || task.Phase == PhaseWaiting {
		return statusDecision(task, "task-start-idempotent/"+task.TaskID), nil
	}
	if task.Phase != PhaseReady {
		return reject(message, errInvalidTransition, "only a ready task can start")
	}
	if task.AssignedTo == "" {
		return reject(message, errInvalidTransition, "task must be assigned before it starts")
	}
	if task.GoalID != "" && message.GoalID != task.GoalID {
		return reject(message, errInvalidRequest, "task start must carry the task goal context")
	}
	if task.UserID != "" && message.UserID != task.UserID {
		return reject(message, errInvalidRequest, "task start must carry the task user context")
	}
	now := s.now()
	if task.Deadline != nil && !task.Deadline.After(now) {
		return reject(message, errDeadlineExpired, "task deadline has expired")
	}
	attempt := task.Attempt + 1
	runID := task.TaskID + "/attempt/" + strconv.Itoa(attempt)
	event, next, err := decideEvent(&task, "task-started/"+runID, taskStartedEvent, taskStartedPayload{
		RunID: runID, Attempt: attempt, StartedAt: now,
	})
	if err != nil {
		return service.Decision{}, err
	}
	requestPayload, err := json.Marshal(agent.ExecuteRequest{
		RunID: runID, Input: task.Input, InputArtifact: cloneArtifact(task.InputArtifact),
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{event},
		Outgoing: []service.OutgoingMessage{{
			Key: "task-agent-execute/" + runID, Kind: contract.MessageCommand,
			Type: agent.ExecuteMessageType, Version: agent.ProtocolVersion,
			To: task.AssignedTo, ReplyTo: s.address, CorrelationID: runID,
			Deadline: cloneTime(task.Deadline), Payload: requestPayload,
		}},
		Reply: statusReply(*next, "task-started-status/"+runID),
	}, nil
}

func (s *taskService) handleSuspend(state aggregateState, message contract.Message) (service.Decision, error) {
	task, decision, err := ownerCommand(state, message)
	if err != nil || decision != nil {
		return dereferenceDecision(decision), err
	}
	if task.Phase == PhaseSuspended {
		return statusDecision(task, "task-suspend-idempotent/"+task.TaskID), nil
	}
	if task.Phase != PhaseReady {
		return reject(message, errInvalidTransition, "the current Agent protocol only permits suspending a ready task")
	}
	var input SuspendRequest
	if len(message.Payload) > 0 {
		if err := json.Unmarshal(message.Payload, &input); err != nil {
			return reject(message, errInvalidRequest, "task suspension payload is invalid")
		}
	}
	event, next, err := decideEvent(&task, "task-suspended/"+task.TaskID, taskSuspendedEvent, taskSuspendedPayload{
		Suspension: Suspension{Reason: strings.TrimSpace(input.Reason), SuspendedAt: s.now()},
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Reply: statusReply(*next, "task-suspended-status/"+task.TaskID)}, nil
}

func (s *taskService) handleResume(state aggregateState, message contract.Message) (service.Decision, error) {
	task, decision, err := ownerCommand(state, message)
	if err != nil || decision != nil {
		return dereferenceDecision(decision), err
	}
	if task.Phase == PhaseReady {
		return statusDecision(task, "task-resume-idempotent/"+task.TaskID), nil
	}
	if task.Phase != PhaseSuspended {
		return reject(message, errInvalidTransition, "only a suspended task can be resumed by its owner")
	}
	event, next, err := decideEvent(&task, "task-unsuspended/"+task.TaskID, taskResumedEvent, taskResumedPayload{ResumedAt: s.now()})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Reply: statusReply(*next, "task-resumed-status/"+task.TaskID)}, nil
}

func (s *taskService) handleRetry(state aggregateState, message contract.Message) (service.Decision, error) {
	task, decision, err := ownerCommand(state, message)
	if err != nil || decision != nil {
		return dereferenceDecision(decision), err
	}
	if task.Phase == PhaseReady && task.Attempt > 0 {
		return statusDecision(task, "task-retry-idempotent/"+task.TaskID), nil
	}
	if task.Phase != PhaseFailed {
		return reject(message, errInvalidTransition, "only a failed task can be retried")
	}
	event, next, err := decideEvent(&task, "task-retry-requested/"+task.TaskID+"/"+strconv.Itoa(task.Attempt), taskRetryRequestedEvent,
		taskRetryRequestedPayload{RequestedAt: s.now()})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Reply: statusReply(*next, "task-retry-status/"+task.TaskID)}, nil
}

func (s *taskService) handleCancel(state aggregateState, message contract.Message) (service.Decision, error) {
	task, decision, err := ownerCommand(state, message)
	if err != nil || decision != nil {
		return dereferenceDecision(decision), err
	}
	if task.Phase.Terminal() {
		return statusDecision(task, "task-cancel-terminal/"+task.TaskID), nil
	}
	var input CancelRequest
	if len(message.Payload) > 0 {
		if err := json.Unmarshal(message.Payload, &input); err != nil {
			return reject(message, errInvalidRequest, "task cancellation payload is invalid")
		}
	}
	input.ReasonCode = strings.TrimSpace(input.ReasonCode)
	if input.ReasonCode == "" {
		input.ReasonCode = errCancelled
	}
	now := s.now()
	if task.Phase == PhaseRunning || task.Phase == PhaseWaiting {
		if task.Cancellation != nil {
			return statusDecision(task, "task-cancel-pending/"+task.TaskID), nil
		}
		event, next, err := decideEvent(&task, "task-cancel-requested/"+task.ActiveRunID, taskCancelRequestedEvent,
			taskCancelRequestedPayload{Cancellation: Cancellation{ReasonCode: input.ReasonCode, RequestedAt: now}})
		if err != nil {
			return service.Decision{}, err
		}
		cancelPayload, err := json.Marshal(agent.CancelRequest{RunID: task.ActiveRunID, ReasonCode: input.ReasonCode})
		if err != nil {
			return service.Decision{}, err
		}
		return service.Decision{
			Events: []service.NewEvent{event},
			Outgoing: []service.OutgoingMessage{{
				Key: "task-agent-cancel/" + task.ActiveRunID, Kind: contract.MessageCommand,
				Type: agent.CancelMessageType, Version: agent.ProtocolVersion,
				To: task.AssignedTo, CorrelationID: task.ActiveRunID + "/cancel", Payload: cancelPayload,
			}},
			Reply: statusReply(*next, "task-cancel-requested-status/"+task.TaskID),
		}, nil
	}
	event, next, err := decideEvent(&task, "task-cancelled/"+task.TaskID, taskCancelledEvent, taskCancelledPayload{
		ReasonCode: input.ReasonCode, CancelledAt: now,
	})
	if err != nil {
		return service.Decision{}, err
	}
	terminal, err := terminalOutgoing(*next)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{event}, Outgoing: []service.OutgoingMessage{terminal},
		Reply: statusReply(*next, "task-cancelled-status/"+task.TaskID),
	}, nil
}

func (s *taskService) handleGet(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, fmt.Errorf("task get requires reply_to")
	}
	if state.Task == nil {
		return reject(message, errNotFound, "task is not created")
	}
	task := state.Task.Clone()
	if message.From != task.OwnerAddress {
		return reject(message, errAccessDenied, "task belongs to another owner")
	}
	var input GetRequest
	if len(message.Payload) > 0 {
		if err := json.Unmarshal(message.Payload, &input); err != nil {
			return reject(message, errInvalidRequest, "task query payload is invalid")
		}
	}
	if input.TaskID != "" && strings.TrimSpace(input.TaskID) != task.TaskID {
		return reject(message, errNotFound, "task was not found in this instance")
	}
	return statusDecision(task, "task-get/"+task.TaskID), nil
}

func (s *taskService) handleExecutionWaiting(state aggregateState, message contract.Message) (service.Decision, error) {
	task, err := executionTask(state, message)
	if err != nil {
		return service.Decision{}, err
	}
	var input ExecutionWaiting
	if err := json.Unmarshal(message.Payload, &input); err != nil || !input.Kind.Valid() {
		return service.Decision{}, fmt.Errorf("task execution waiting payload is invalid")
	}
	if staleExecution(task, input.TaskID, input.RunID, message.CorrelationID) || task.Phase.Terminal() {
		return service.Decision{}, nil
	}
	if task.Phase == PhaseWaiting {
		return service.Decision{}, nil
	}
	if task.Phase != PhaseRunning {
		return service.Decision{}, fmt.Errorf("task cannot wait in phase %q", task.Phase)
	}
	wait := WaitState{
		Kind: input.Kind, References: cleanStrings(input.References), ResumeOn: cleanMessageTypes(input.ResumeOn),
		Deadline: cloneTime(input.Deadline), RequestedAt: s.now(),
	}
	event, next, err := decideEvent(&task, "task-waiting/"+task.ActiveRunID+"/"+string(wait.Kind), taskWaitingEvent,
		taskWaitingPayload{RunID: task.ActiveRunID, Wait: wait})
	if err != nil {
		return service.Decision{}, err
	}
	changed, err := statusChangedOutgoing(*next, "task-status-waiting/"+task.ActiveRunID)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Outgoing: []service.OutgoingMessage{changed}}, nil
}

func (s *taskService) handleExecutionResumed(state aggregateState, message contract.Message) (service.Decision, error) {
	task, err := executionTask(state, message)
	if err != nil {
		return service.Decision{}, err
	}
	var input ExecutionResumed
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, fmt.Errorf("task execution resumed payload is invalid")
	}
	if staleExecution(task, input.TaskID, input.RunID, message.CorrelationID) || task.Phase.Terminal() || task.Phase == PhaseRunning {
		return service.Decision{}, nil
	}
	if task.Phase != PhaseWaiting {
		return service.Decision{}, fmt.Errorf("task cannot resume execution in phase %q", task.Phase)
	}
	event, next, err := decideEvent(&task, "task-execution-resumed/"+task.ActiveRunID, taskResumedEvent,
		taskResumedPayload{RunID: task.ActiveRunID, ResumedAt: s.now()})
	if err != nil {
		return service.Decision{}, err
	}
	changed, err := statusChangedOutgoing(*next, "task-status-running/"+task.ActiveRunID)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Outgoing: []service.OutgoingMessage{changed}}, nil
}

func (s *taskService) handleAgentCompleted(state aggregateState, message contract.Message) (service.Decision, error) {
	task, err := executionTask(state, message)
	if err != nil {
		return service.Decision{}, err
	}
	if task.Phase.Terminal() {
		return service.Decision{}, nil
	}
	if message.CorrelationID != "" && message.CorrelationID != task.ActiveRunID {
		return service.Decision{}, nil
	}
	now := s.now()
	if message.Metadata[contract.MetadataReplyError] == "true" {
		var replyError service.ReplyError
		if err := json.Unmarshal(message.Payload, &replyError); err != nil {
			return service.Decision{}, fmt.Errorf("decode Agent rejection: %w", err)
		}
		code := strings.TrimSpace(replyError.Code)
		if code == "" {
			code = errAgentRejected
		}
		return s.failTask(task, code, safeMessage(replyError.Message, "Agent rejected task execution"), replyError.Retryable, now)
	}
	var result agent.ExecuteResult
	if err := json.Unmarshal(message.Payload, &result); err != nil {
		return service.Decision{}, fmt.Errorf("decode Agent completion: %w", err)
	}
	if result.RunID != task.ActiveRunID {
		return service.Decision{}, nil
	}
	switch result.Phase {
	case agent.PhaseCompleted:
		if result.Output == nil {
			return service.Decision{}, fmt.Errorf("Agent completed run %q without an output artifact", result.RunID)
		}
		if err := artifact.ValidateRef(*result.Output); err != nil {
			return service.Decision{}, fmt.Errorf("Agent completed run %q with an invalid output artifact: %w", result.RunID, err)
		}
		event, next, err := decideEvent(&task, "task-completed/"+task.ActiveRunID, taskCompletedEvent, taskCompletedPayload{
			RunID: task.ActiveRunID, ResultRef: *cloneArtifact(result.Output), CompletedAt: now,
		})
		if err != nil {
			return service.Decision{}, err
		}
		terminal, err := terminalOutgoing(*next)
		if err != nil {
			return service.Decision{}, err
		}
		return service.Decision{Events: []service.NewEvent{event}, Outgoing: []service.OutgoingMessage{terminal}}, nil
	case agent.PhaseFailed:
		code := strings.TrimSpace(result.ErrorCode)
		if code == "" {
			code = errAgentFailed
		}
		return s.failTask(task, code, safeMessage(result.ErrorMessage, "Agent execution failed"), false, now)
	case agent.PhaseCancelled:
		reason := strings.TrimSpace(result.ErrorCode)
		if reason == "" {
			reason = errCancelled
		}
		event, next, err := decideEvent(&task, "task-cancelled/"+task.ActiveRunID, taskCancelledEvent, taskCancelledPayload{
			RunID: task.ActiveRunID, ReasonCode: reason, CancelledAt: now,
		})
		if err != nil {
			return service.Decision{}, err
		}
		terminal, err := terminalOutgoing(*next)
		if err != nil {
			return service.Decision{}, err
		}
		return service.Decision{Events: []service.NewEvent{event}, Outgoing: []service.OutgoingMessage{terminal}}, nil
	default:
		return service.Decision{}, fmt.Errorf("Agent completion run %q has non-terminal phase %q", result.RunID, result.Phase)
	}
}

func (s *taskService) failTask(task State, code, message string, retryable bool, at time.Time) (service.Decision, error) {
	event, next, err := decideEvent(&task, "task-failed/"+task.ActiveRunID+"/"+code, taskFailedEvent, taskFailedPayload{
		RunID: task.ActiveRunID,
		Error: Error{Code: code, Message: message, Source: "agent", RunID: task.ActiveRunID, Retryable: retryable, OccurredAt: at},
	})
	if err != nil {
		return service.Decision{}, err
	}
	terminal, err := terminalOutgoing(*next)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{event}, Outgoing: []service.OutgoingMessage{terminal}}, nil
}

func ownerCommand(state aggregateState, message contract.Message) (State, *service.Decision, error) {
	if message.ReplyTo == "" {
		return State{}, nil, fmt.Errorf("task command %q requires reply_to", message.Type)
	}
	if state.Task == nil {
		decision, err := reject(message, errNotFound, "task is not created")
		return State{}, &decision, err
	}
	task := state.Task.Clone()
	if message.From != task.OwnerAddress {
		decision, err := reject(message, errAccessDenied, "task belongs to another owner")
		return State{}, &decision, err
	}
	return task, nil, nil
}

func executionTask(state aggregateState, message contract.Message) (State, error) {
	if state.Task == nil {
		return State{}, fmt.Errorf("task is not created")
	}
	task := state.Task.Clone()
	if message.From != task.AssignedTo {
		return State{}, fmt.Errorf("task execution report came from unexpected address %q", message.From)
	}
	return task, nil
}

func staleExecution(task State, taskID, runID, correlationID string) bool {
	if strings.TrimSpace(taskID) != task.TaskID || strings.TrimSpace(runID) != task.ActiveRunID {
		return true
	}
	return correlationID != "" && correlationID != task.ActiveRunID
}

func decideEvent(current *State, key string, eventType contract.EventType, payload any) (service.NewEvent, *State, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return service.NewEvent{}, nil, err
	}
	next, err := applyTaskEvent(current, eventType, raw)
	if err != nil {
		return service.NewEvent{}, nil, err
	}
	return service.NewEvent{Key: key, Type: eventType, Version: ProtocolVersion, Payload: raw}, next, nil
}

func statusDecision(task State, key string) service.Decision {
	return service.Decision{Reply: statusReply(task, key)}
}

func statusReply(task State, key string) *service.Reply {
	value := task.Clone()
	payload, _ := json.Marshal(StatusResponse{Task: &value})
	return &service.Reply{Key: key, Type: StatusMessageType, Version: ProtocolVersion, Payload: payload}
}

func statusChangedOutgoing(task State, key string) (service.OutgoingMessage, error) {
	value := task.Clone()
	payload, err := json.Marshal(StatusResponse{Task: &value})
	if err != nil {
		return service.OutgoingMessage{}, err
	}
	return service.OutgoingMessage{
		Key: key, Kind: contract.MessageEvent, Type: StatusChangedEventType, Version: ProtocolVersion,
		To: task.OwnerAddress, CorrelationID: task.TaskID, Payload: payload,
	}, nil
}

func terminalOutgoing(task State) (service.OutgoingMessage, error) {
	if !task.Phase.Terminal() || task.CompletedAt == nil {
		return service.OutgoingMessage{}, fmt.Errorf("task %q is not terminal", task.TaskID)
	}
	result := Result{
		TaskID: task.TaskID, GoalID: task.GoalID, Phase: task.Phase, Attempt: task.Attempt,
		ResultRef: cloneArtifact(task.ResultRef), CompletedAt: task.CompletedAt.UTC(),
	}
	if task.LastError != nil {
		value := *task.LastError
		result.Error = &value
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return service.OutgoingMessage{}, err
	}
	var messageType contract.MessageType
	switch task.Phase {
	case PhaseCompleted:
		messageType = CompletedEventType
	case PhaseFailed:
		messageType = FailedEventType
	case PhaseCancelled:
		messageType = CancelledEventType
	default:
		return service.OutgoingMessage{}, fmt.Errorf("unsupported terminal task phase %q", task.Phase)
	}
	return service.OutgoingMessage{
		Key:  "task-terminal/" + task.TaskID + "/" + string(task.Phase),
		Kind: contract.MessageEvent, Type: messageType, Version: ProtocolVersion,
		To: task.OwnerAddress, CorrelationID: task.TaskID, Payload: payload,
	}, nil
}

func reject(message contract.Message, code, safe string) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, fmt.Errorf("%s: %s", code, safe)
	}
	return service.Decision{Reply: &service.Reply{
		Key: "task-rejected/" + code, Type: StatusMessageType, Version: ProtocolVersion,
		Error: &service.ReplyError{Code: code, Message: safe},
	}}, nil
}

func dereferenceDecision(value *service.Decision) service.Decision {
	if value == nil {
		return service.Decision{}
	}
	return *value
}

func taskFingerprint(input CreateRequest, owner contract.ServiceAddress, userID string) (string, error) {
	identity := struct {
		TaskID        string
		GoalID        string
		UserID        string
		Owner         contract.ServiceAddress
		Title         string
		Input         string
		InputArtifact *contract.ArtifactRef
		Deadline      *time.Time
	}{
		TaskID: input.TaskID, GoalID: input.GoalID, UserID: strings.TrimSpace(userID), Owner: owner,
		Title: input.Title, Input: input.Input, InputArtifact: cloneArtifact(input.InputArtifact), Deadline: cloneTime(input.Deadline),
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func cleanStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, found := seen[value]; found {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func cleanMessageTypes(values []contract.MessageType) []contract.MessageType {
	seen := make(map[contract.MessageType]struct{}, len(values))
	result := make([]contract.MessageType, 0, len(values))
	for _, value := range values {
		value = contract.MessageType(strings.TrimSpace(string(value)))
		if value == "" {
			continue
		}
		if _, found := seen[value]; found {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func safeMessage(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func (s *taskService) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock.Now().UTC()
}

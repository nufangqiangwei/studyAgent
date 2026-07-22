package task

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"encoding/json"
	"fmt"
	"time"
)

type aggregateState struct {
	Task *State `json:"task,omitempty"`
}

type taskCreatedPayload struct {
	Task State `json:"task"`
}

type taskReadyPayload struct {
	ReadyAt time.Time `json:"ready_at"`
}

type taskAssignedPayload struct {
	AssignedTo contract.ServiceAddress `json:"assigned_to"`
	AssignedAt time.Time               `json:"assigned_at"`
}

type taskStartedPayload struct {
	RunID     string    `json:"run_id"`
	Attempt   int       `json:"attempt"`
	StartedAt time.Time `json:"started_at"`
}

type taskWaitingPayload struct {
	RunID string    `json:"run_id"`
	Wait  WaitState `json:"wait"`
}

type taskResumedPayload struct {
	RunID     string    `json:"run_id"`
	ResumedAt time.Time `json:"resumed_at"`
}

type taskSuspendedPayload struct {
	Suspension Suspension `json:"suspension"`
}

type taskRetryRequestedPayload struct {
	RequestedAt time.Time `json:"requested_at"`
}

type taskCancelRequestedPayload struct {
	Cancellation Cancellation `json:"cancellation"`
}

type taskCompletedPayload struct {
	RunID       string               `json:"run_id"`
	ResultRef   contract.ArtifactRef `json:"result_ref"`
	CompletedAt time.Time            `json:"completed_at"`
}

type taskFailedPayload struct {
	RunID string `json:"run_id"`
	Error Error  `json:"error"`
}

type taskCancelledPayload struct {
	RunID       string    `json:"run_id,omitempty"`
	ReasonCode  string    `json:"reason_code,omitempty"`
	CancelledAt time.Time `json:"cancelled_at"`
}

func initialAggregateState() aggregateState { return aggregateState{} }

func encodeState(state aggregateState) (service.State, error) {
	if state.Task != nil {
		cloned := state.Task.Clone()
		if err := cloned.validate(); err != nil {
			return service.State{}, fmt.Errorf("validate task state: %w", err)
		}
		state.Task = &cloned
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return service.State{}, fmt.Errorf("encode task state: %w", err)
	}
	return service.State{SchemaVersion: StateSchema.Version, Data: payload}, nil
}

func decodeState(raw service.State) (aggregateState, error) {
	if raw.SchemaVersion != StateSchema.Version {
		return aggregateState{}, fmt.Errorf("task state schema %d is unsupported", raw.SchemaVersion)
	}
	var state aggregateState
	if err := json.Unmarshal(raw.Data, &state); err != nil {
		return aggregateState{}, fmt.Errorf("decode task state: %w", err)
	}
	if state.Task != nil {
		cloned := state.Task.Clone()
		if err := cloned.validate(); err != nil {
			return aggregateState{}, fmt.Errorf("validate task state: %w", err)
		}
		state.Task = &cloned
	}
	return state, nil
}

func (s *taskService) Apply(raw service.State, event contract.StoredEvent) (service.State, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.State{}, err
	}
	if event.EventVersion != ProtocolVersion {
		return service.State{}, fmt.Errorf("task event %q version %d is unsupported", event.EventType, event.EventVersion)
	}
	next, err := applyTaskEvent(state.Task, event.EventType, event.Payload)
	if err != nil {
		return service.State{}, err
	}
	return encodeState(aggregateState{Task: next})
}

func applyTaskEvent(current *State, eventType contract.EventType, raw json.RawMessage) (*State, error) {
	switch eventType {
	case taskCreatedEvent:
		if current != nil {
			return nil, fmt.Errorf("task is already created")
		}
		var payload taskCreatedPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, fmt.Errorf("decode task created event: %w", err)
		}
		task := payload.Task.Clone()
		if task.Phase != PhaseCreated {
			return nil, fmt.Errorf("created event has phase %q", task.Phase)
		}
		if err := task.validate(); err != nil {
			return nil, err
		}
		return &task, nil
	case taskReadyEvent:
		task, err := requirePhase(current, PhaseCreated)
		if err != nil {
			return nil, err
		}
		var payload taskReadyPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.ReadyAt.IsZero() {
			return nil, fmt.Errorf("task ready event is invalid")
		}
		task.Phase, task.UpdatedAt = PhaseReady, payload.ReadyAt.UTC()
		return &task, nil
	case taskAssignedEvent:
		task, err := requireTask(current)
		if err != nil {
			return nil, err
		}
		if task.Phase != PhaseCreated && task.Phase != PhaseReady {
			return nil, fmt.Errorf("cannot assign task in phase %q", task.Phase)
		}
		var payload taskAssignedPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.AssignedTo == "" || payload.AssignedAt.IsZero() {
			return nil, fmt.Errorf("task assigned event is invalid")
		}
		task.AssignedTo, task.UpdatedAt = payload.AssignedTo, payload.AssignedAt.UTC()
		return &task, nil
	case taskStartedEvent:
		task, err := requirePhase(current, PhaseReady)
		if err != nil {
			return nil, err
		}
		var payload taskStartedPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.RunID == "" || payload.StartedAt.IsZero() || payload.Attempt != task.Attempt+1 {
			return nil, fmt.Errorf("task started event is invalid")
		}
		if task.AssignedTo == "" {
			return nil, fmt.Errorf("unassigned task cannot start")
		}
		task.Phase, task.ActiveRunID, task.Attempt = PhaseRunning, payload.RunID, payload.Attempt
		task.Wait, task.Suspension, task.Cancellation = nil, nil, nil
		task.ResultRef, task.LastError, task.CompletedAt = nil, nil, nil
		task.UpdatedAt = payload.StartedAt.UTC()
		return &task, nil
	case taskWaitingEvent:
		task, err := requirePhase(current, PhaseRunning)
		if err != nil {
			return nil, err
		}
		var payload taskWaitingPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.RunID != task.ActiveRunID || !payload.Wait.Kind.Valid() || payload.Wait.RequestedAt.IsZero() {
			return nil, fmt.Errorf("task waiting event is invalid")
		}
		wait := payload.Wait.clone()
		task.Phase, task.Wait, task.UpdatedAt = PhaseWaiting, &wait, payload.Wait.RequestedAt.UTC()
		return &task, nil
	case taskResumedEvent:
		task, err := requireTask(current)
		if err != nil {
			return nil, err
		}
		var payload taskResumedPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.ResumedAt.IsZero() {
			return nil, fmt.Errorf("task resumed event is invalid")
		}
		switch task.Phase {
		case PhaseWaiting:
			if payload.RunID != task.ActiveRunID {
				return nil, fmt.Errorf("task resumed event run does not match active run")
			}
			task.Phase, task.Wait = PhaseRunning, nil
		case PhaseSuspended:
			if payload.RunID != "" {
				return nil, fmt.Errorf("owner resume event cannot contain a run id")
			}
			task.Phase, task.Suspension = PhaseReady, nil
		default:
			return nil, fmt.Errorf("cannot resume task in phase %q", task.Phase)
		}
		task.UpdatedAt = payload.ResumedAt.UTC()
		return &task, nil
	case taskSuspendedEvent:
		task, err := requirePhase(current, PhaseReady)
		if err != nil {
			return nil, err
		}
		var payload taskSuspendedPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.Suspension.SuspendedAt.IsZero() {
			return nil, fmt.Errorf("task suspended event is invalid")
		}
		suspension := payload.Suspension
		task.Phase, task.Suspension, task.UpdatedAt = PhaseSuspended, &suspension, suspension.SuspendedAt.UTC()
		return &task, nil
	case taskRetryRequestedEvent:
		task, err := requirePhase(current, PhaseFailed)
		if err != nil {
			return nil, err
		}
		var payload taskRetryRequestedPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.RequestedAt.IsZero() {
			return nil, fmt.Errorf("task retry event is invalid")
		}
		task.Phase, task.ActiveRunID = PhaseReady, ""
		task.Wait, task.Suspension, task.Cancellation = nil, nil, nil
		task.ResultRef, task.LastError, task.CompletedAt = nil, nil, nil
		task.UpdatedAt = payload.RequestedAt.UTC()
		return &task, nil
	case taskCancelRequestedEvent:
		task, err := requireTask(current)
		if err != nil {
			return nil, err
		}
		if task.Phase != PhaseRunning && task.Phase != PhaseWaiting {
			return nil, fmt.Errorf("cannot request cancellation in phase %q", task.Phase)
		}
		var payload taskCancelRequestedPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.Cancellation.RequestedAt.IsZero() {
			return nil, fmt.Errorf("task cancellation request event is invalid")
		}
		cancellation := payload.Cancellation
		task.Cancellation, task.UpdatedAt = &cancellation, cancellation.RequestedAt.UTC()
		return &task, nil
	case taskCompletedEvent:
		task, err := requireActiveRun(current)
		if err != nil {
			return nil, err
		}
		var payload taskCompletedPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.RunID != task.ActiveRunID || payload.CompletedAt.IsZero() {
			return nil, fmt.Errorf("task completed event is invalid")
		}
		result := payload.ResultRef
		task.Phase, task.ResultRef, task.LastError = PhaseCompleted, &result, nil
		task.Wait, task.Suspension = nil, nil
		task.UpdatedAt, task.CompletedAt = payload.CompletedAt.UTC(), cloneTime(&payload.CompletedAt)
		return &task, nil
	case taskFailedEvent:
		task, err := requireActiveRun(current)
		if err != nil {
			return nil, err
		}
		var payload taskFailedPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.RunID != task.ActiveRunID || payload.Error.Code == "" || payload.Error.OccurredAt.IsZero() {
			return nil, fmt.Errorf("task failed event is invalid")
		}
		failure := payload.Error
		task.Phase, task.LastError, task.ResultRef = PhaseFailed, &failure, nil
		task.Wait, task.Suspension = nil, nil
		task.FailureCount++
		task.UpdatedAt, task.CompletedAt = failure.OccurredAt.UTC(), cloneTime(&failure.OccurredAt)
		return &task, nil
	case taskCancelledEvent:
		task, err := requireTask(current)
		if err != nil {
			return nil, err
		}
		if task.Phase.Terminal() {
			return nil, fmt.Errorf("terminal task cannot be cancelled")
		}
		var payload taskCancelledPayload
		if err := json.Unmarshal(raw, &payload); err != nil || payload.CancelledAt.IsZero() {
			return nil, fmt.Errorf("task cancelled event is invalid")
		}
		if (task.Phase == PhaseRunning || task.Phase == PhaseWaiting) && payload.RunID != task.ActiveRunID {
			return nil, fmt.Errorf("task cancellation run does not match active run")
		}
		task.Phase, task.ResultRef = PhaseCancelled, nil
		task.Wait, task.Suspension = nil, nil
		reason := payload.ReasonCode
		if reason == "" {
			reason = errCancelled
		}
		failure := Error{Code: reason, Message: "task was cancelled", Source: "task", RunID: payload.RunID, OccurredAt: payload.CancelledAt.UTC()}
		task.LastError = &failure
		task.UpdatedAt, task.CompletedAt = payload.CancelledAt.UTC(), cloneTime(&payload.CancelledAt)
		return &task, nil
	default:
		return nil, fmt.Errorf("unknown task event %q", eventType)
	}
}

func requireTask(current *State) (State, error) {
	if current == nil {
		return State{}, fmt.Errorf("task is not created")
	}
	return current.Clone(), nil
}

func requirePhase(current *State, phase Phase) (State, error) {
	task, err := requireTask(current)
	if err != nil {
		return State{}, err
	}
	if task.Phase != phase {
		return State{}, fmt.Errorf("task phase is %q, want %q", task.Phase, phase)
	}
	return task, nil
}

func requireActiveRun(current *State) (State, error) {
	task, err := requireTask(current)
	if err != nil {
		return State{}, err
	}
	if task.Phase != PhaseRunning && task.Phase != PhaseWaiting {
		return State{}, fmt.Errorf("task phase %q has no active run", task.Phase)
	}
	if task.ActiveRunID == "" {
		return State{}, fmt.Errorf("active task has no run id")
	}
	return task, nil
}

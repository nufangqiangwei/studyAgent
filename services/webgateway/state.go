package webgateway

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/task"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type RequestState struct {
	RequestID           string                     `json:"request_id"`
	Operation           Operation                  `json:"operation"`
	UserID              string                     `json:"user_id"`
	TaskID              string                     `json:"task_id"`
	TaskAddress         contract.ServiceAddress    `json:"task_address,omitempty"`
	TaskInstanceID      contract.ServiceInstanceID `json:"task_instance_id,omitempty"`
	Phase               RequestPhase               `json:"phase"`
	IdentityFingerprint string                     `json:"identity_fingerprint"`
	DeclarationCallID   string                     `json:"declaration_call_id,omitempty"`
	GoalID              string                     `json:"goal_id,omitempty"`
	Title               string                     `json:"title,omitempty"`
	Input               string                     `json:"input,omitempty"`
	InputArtifact       *contract.ArtifactRef      `json:"input_artifact,omitempty"`
	Deadline            *time.Time                 `json:"deadline,omitempty"`
	Task                *TaskDTO                   `json:"task,omitempty"`
	Terminal            *terminalObservation       `json:"terminal,omitempty"`
	Error               *ErrorDTO                  `json:"error,omitempty"`
	PresentationID      string                     `json:"presentation_id,omitempty"`
	CreatedAt           time.Time                  `json:"created_at"`
	UpdatedAt           time.Time                  `json:"updated_at"`
	CompletedAt         *time.Time                 `json:"completed_at,omitempty"`
}

func (r RequestState) clone() RequestState {
	r.InputArtifact = cloneArtifact(r.InputArtifact)
	r.Deadline = cloneTime(r.Deadline)
	if r.Task != nil {
		value := r.Task.clone()
		r.Task = &value
	}
	r.Terminal = cloneTerminalObservation(r.Terminal)
	if r.Error != nil {
		value := *r.Error
		r.Error = &value
	}
	r.CompletedAt = cloneTime(r.CompletedAt)
	return r
}

// terminalObservation is the minimum durable fact needed to bridge the race
// between a Task terminal event and the Task status reply for task.start. It is
// not a Task projection: the Gateway still queries Task Service for the
// authoritative full State before completing the create presentation.
type terminalObservation struct {
	MessageType contract.MessageType `json:"message_type"`
	Result      task.Result          `json:"result"`
}

func cloneTerminalObservation(value *terminalObservation) *terminalObservation {
	if value == nil {
		return nil
	}
	cloned := *value
	cloned.Result.ResultRef = cloneArtifact(value.Result.ResultRef)
	if value.Result.Error != nil {
		errValue := *value.Result.Error
		cloned.Result.Error = &errValue
	}
	return &cloned
}

func (o terminalObservation) validate(request RequestState) error {
	expectedType, ok := terminalMessageType(o.Result.Phase)
	if !ok || o.MessageType != expectedType {
		return fmt.Errorf("terminal message type %q does not match phase %q", o.MessageType, o.Result.Phase)
	}
	if strings.TrimSpace(o.Result.TaskID) == "" || o.Result.TaskID != request.TaskID ||
		o.Result.GoalID != request.GoalID || o.Result.Attempt < 0 || o.Result.CompletedAt.IsZero() {
		return fmt.Errorf("terminal result identity, attempt, or completion time is invalid")
	}
	if o.Result.Phase == task.PhaseCompleted {
		if o.Result.ResultRef == nil {
			return fmt.Errorf("completed terminal result requires an artifact")
		}
		if err := artifact.ValidateRef(*o.Result.ResultRef); err != nil {
			return fmt.Errorf("terminal result artifact is invalid: %w", err)
		}
	}
	if o.Result.Phase == task.PhaseFailed || o.Result.Phase == task.PhaseCancelled {
		if o.Result.Error == nil || strings.TrimSpace(o.Result.Error.Code) == "" || o.Result.Error.OccurredAt.IsZero() {
			return fmt.Errorf("failed or cancelled terminal result requires an error fact")
		}
	}
	return nil
}

func terminalMessageType(phase task.Phase) (contract.MessageType, bool) {
	switch phase {
	case task.PhaseCompleted:
		return task.CompletedEventType, true
	case task.PhaseFailed:
		return task.FailedEventType, true
	case task.PhaseCancelled:
		return task.CancelledEventType, true
	default:
		return "", false
	}
}

func (r RequestState) validate() error {
	if strings.TrimSpace(r.RequestID) == "" || !r.Operation.valid() ||
		strings.TrimSpace(r.UserID) == "" || strings.TrimSpace(r.TaskID) == "" ||
		strings.TrimSpace(r.IdentityFingerprint) == "" || r.CreatedAt.IsZero() || r.UpdatedAt.IsZero() {
		return fmt.Errorf("request identity, user, task, fingerprint, and timestamps are required")
	}
	if r.UpdatedAt.Before(r.CreatedAt) {
		return fmt.Errorf("request update precedes creation")
	}
	if r.Operation == OperationCreate {
		if r.TaskAddress == "" || r.TaskInstanceID == "" {
			return fmt.Errorf("create request requires stable task identity")
		}
		if (strings.TrimSpace(r.Input) == "") == (r.InputArtifact == nil) {
			return fmt.Errorf("create request requires exactly one task input")
		}
		if len(r.Input) > maxInlineTaskInputBytes {
			return fmt.Errorf("create request inline input is too large")
		}
	}
	if r.InputArtifact != nil {
		if err := artifact.ValidateRef(*r.InputArtifact); err != nil {
			return fmt.Errorf("create request input artifact is invalid: %w", err)
		}
	}
	switch r.Phase {
	case PhaseDeclaringTask:
		if r.Operation != OperationCreate || r.DeclarationCallID == "" || r.Task != nil || r.Terminal != nil || r.Error != nil || r.CompletedAt != nil || r.PresentationID != "" {
			return fmt.Errorf("declaring request state is invalid")
		}
	case PhaseWaitingTask:
		if r.TaskAddress == "" || r.TaskInstanceID == "" || r.Task != nil || r.Terminal != nil || r.Error != nil || r.CompletedAt != nil || r.PresentationID != "" {
			return fmt.Errorf("waiting request state is invalid")
		}
	case PhaseMarkingReady:
		if r.Operation != OperationCreate || r.TaskAddress == "" || r.TaskInstanceID == "" || r.Task != nil || r.Terminal != nil || r.Error != nil || r.CompletedAt != nil || r.PresentationID != "" {
			return fmt.Errorf("marking ready request state is invalid")
		}
	case PhaseAssigning:
		if r.Operation != OperationCreate || r.TaskAddress == "" || r.TaskInstanceID == "" || r.Task != nil || r.Terminal != nil || r.Error != nil || r.CompletedAt != nil || r.PresentationID != "" {
			return fmt.Errorf("assigning request state is invalid")
		}
	case PhaseStarting:
		if r.Operation != OperationCreate || r.TaskAddress == "" || r.TaskInstanceID == "" || r.Task != nil || r.Terminal != nil || r.Error != nil || r.CompletedAt != nil || r.PresentationID != "" {
			return fmt.Errorf("starting request state is invalid")
		}
	case PhaseResolvingTerminal:
		if r.Operation != OperationCreate || r.TaskAddress == "" || r.TaskInstanceID == "" ||
			r.Task != nil || r.Terminal == nil || r.Error != nil || r.CompletedAt != nil || r.PresentationID != "" {
			return fmt.Errorf("resolving terminal request state is invalid")
		}
		if err := r.Terminal.validate(r); err != nil {
			return fmt.Errorf("resolving terminal fact is invalid: %w", err)
		}
	case PhaseSucceeded:
		if r.Task == nil || r.Terminal != nil || r.Error != nil || r.CompletedAt == nil || r.CompletedAt.IsZero() || r.PresentationID == "" {
			return fmt.Errorf("succeeded request requires task result, presentation, and completion time")
		}
		if err := r.Task.validate(); err != nil {
			return fmt.Errorf("succeeded request task is invalid: %w", err)
		}
	case PhaseFailed:
		if r.Task != nil || r.Terminal != nil || r.Error == nil || r.CompletedAt == nil || r.CompletedAt.IsZero() || r.PresentationID == "" {
			return fmt.Errorf("failed request requires error, presentation, and completion time")
		}
		if err := r.Error.validate(); err != nil {
			return fmt.Errorf("failed request error is invalid: %w", err)
		}
	default:
		return fmt.Errorf("request phase %q is invalid", r.Phase)
	}
	if r.CompletedAt != nil && r.CompletedAt.Before(r.CreatedAt) {
		return fmt.Errorf("request completion precedes creation")
	}
	return nil
}

type OwnedTask struct {
	TaskID             string                     `json:"task_id"`
	UserID             string                     `json:"user_id"`
	Address            contract.ServiceAddress    `json:"address"`
	InstanceID         contract.ServiceInstanceID `json:"instance_id"`
	CreatedByRequestID string                     `json:"created_by_request_id"`
}

func (t OwnedTask) validate() error {
	if strings.TrimSpace(t.TaskID) == "" || strings.TrimSpace(t.UserID) == "" ||
		t.Address == "" || t.InstanceID == "" || strings.TrimSpace(t.CreatedByRequestID) == "" {
		return fmt.Errorf("owned task identity is incomplete")
	}
	return nil
}

type State struct {
	Requests         map[string]RequestState `json:"requests"`
	Tasks            map[string]OwnedTask    `json:"tasks"`
	TerminalOrderIDs []string                `json:"terminal_order_ids,omitempty"`
}

type requestEventPayload struct {
	Request RequestState `json:"request"`
	Task    *OwnedTask   `json:"task,omitempty"`
}

func initialState() State {
	return State{
		Requests: make(map[string]RequestState),
		Tasks:    make(map[string]OwnedTask),
	}
}

func encodeState(state State) (service.State, error) {
	if state.Requests == nil {
		state.Requests = make(map[string]RequestState)
	}
	if state.Tasks == nil {
		state.Tasks = make(map[string]OwnedTask)
	}
	state.TerminalOrderIDs = append([]string(nil), state.TerminalOrderIDs...)
	payload, err := json.Marshal(state)
	if err != nil {
		return service.State{}, fmt.Errorf("encode web gateway state: %w", err)
	}
	return service.State{SchemaVersion: StateSchema.Version, Data: payload}, nil
}

func decodeState(raw service.State) (State, error) {
	if raw.SchemaVersion != StateSchema.Version {
		return State{}, fmt.Errorf("web gateway state schema %d is unsupported", raw.SchemaVersion)
	}
	var state State
	if err := json.Unmarshal(raw.Data, &state); err != nil {
		return State{}, fmt.Errorf("decode web gateway state: %w", err)
	}
	if state.Requests == nil {
		state.Requests = make(map[string]RequestState)
	}
	if state.Tasks == nil {
		state.Tasks = make(map[string]OwnedTask)
	}
	state.TerminalOrderIDs = append([]string(nil), state.TerminalOrderIDs...)
	for id, request := range state.Requests {
		if id != request.RequestID {
			return State{}, fmt.Errorf("request map key %q does not match request id %q", id, request.RequestID)
		}
		request = request.clone()
		if err := request.validate(); err != nil {
			return State{}, fmt.Errorf("validate request %q: %w", id, err)
		}
		state.Requests[id] = request
	}
	for id, owned := range state.Tasks {
		if id != owned.TaskID {
			return State{}, fmt.Errorf("task map key %q does not match task id %q", id, owned.TaskID)
		}
		if err := owned.validate(); err != nil {
			return State{}, fmt.Errorf("validate owned task %q: %w", id, err)
		}
	}
	if err := validateTerminalProjection(state); err != nil {
		return State{}, err
	}
	return state, nil
}

func (s *webGatewayService) Apply(raw service.State, event contract.StoredEvent) (service.State, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.State{}, err
	}
	if event.EventVersion != ProtocolVersion {
		return service.State{}, fmt.Errorf("web gateway event %q version %d is unsupported", event.EventType, event.EventVersion)
	}
	var payload requestEventPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return service.State{}, fmt.Errorf("decode web gateway event %q: %w", event.EventType, err)
	}
	request := payload.Request.clone()
	if err := request.validate(); err != nil {
		return service.State{}, fmt.Errorf("validate web gateway event %q: %w", event.EventType, err)
	}
	existing, found := state.Requests[request.RequestID]
	switch event.EventType {
	case requestRecordedEvent:
		if found || (request.Phase != PhaseDeclaringTask && request.Phase != PhaseWaitingTask && request.Phase != PhaseFailed) {
			return service.State{}, fmt.Errorf("request %q cannot be recorded in phase %q", request.RequestID, request.Phase)
		}
	case taskDeclarationCompletedEvent:
		if !found || existing.Phase != PhaseDeclaringTask || request.Phase != PhaseWaitingTask || !sameRequestIdentity(existing, request) {
			return service.State{}, fmt.Errorf("request %q cannot complete task declaration", request.RequestID)
		}
	case taskMarkedReadyEvent:
		if !found || existing.Phase != PhaseWaitingTask || request.Phase != PhaseMarkingReady || !sameRequestIdentity(existing, request) || request.Operation != OperationCreate {
			return service.State{}, fmt.Errorf("request %q cannot transition to marking ready", request.RequestID)
		}
	case taskAssignedEvent:
		if !found || existing.Phase != PhaseMarkingReady || request.Phase != PhaseAssigning || !sameRequestIdentity(existing, request) || request.Operation != OperationCreate {
			return service.State{}, fmt.Errorf("request %q cannot transition to assigning", request.RequestID)
		}
	case taskStartRequestedEvent:
		if !found || existing.Phase != PhaseAssigning || request.Phase != PhaseStarting || !sameRequestIdentity(existing, request) || request.Operation != OperationCreate {
			return service.State{}, fmt.Errorf("request %q cannot transition to starting", request.RequestID)
		}
	case taskTerminalObservedEvent:
		if !found || request.Phase != PhaseResolvingTerminal || request.Operation != OperationCreate ||
			!sameRequestIdentity(existing, request) || existing.Phase.terminal() ||
			existing.Phase == PhaseDeclaringTask || existing.Phase == PhaseResolvingTerminal {
			return service.State{}, fmt.Errorf("request %q cannot observe terminal task state from phase %q", request.RequestID, existing.Phase)
		}
	case requestSucceededEvent:
		if !found || (existing.Phase != PhaseWaitingTask && existing.Phase != PhaseMarkingReady &&
			existing.Phase != PhaseAssigning && existing.Phase != PhaseStarting &&
			existing.Phase != PhaseResolvingTerminal) ||
			request.Phase != PhaseSucceeded || !sameRequestIdentity(existing, request) {
			return service.State{}, fmt.Errorf("request %q cannot succeed from phase %q", request.RequestID, existing.Phase)
		}
		if request.Operation == OperationCreate {
			if payload.Task == nil {
				return service.State{}, fmt.Errorf("create request %q success requires an owned task", request.RequestID)
			}
			if err := payload.Task.validate(); err != nil {
				return service.State{}, err
			}
			if owned, exists := state.Tasks[payload.Task.TaskID]; exists && owned != *payload.Task {
				return service.State{}, fmt.Errorf("task %q is already owned by another request", payload.Task.TaskID)
			}
			state.Tasks[payload.Task.TaskID] = *payload.Task
		} else if payload.Task != nil {
			return service.State{}, fmt.Errorf("get request %q cannot create an owned task mapping", request.RequestID)
		}
	case requestFailedEvent:
		if found && (existing.Phase.terminal() || !sameRequestIdentity(existing, request)) {
			return service.State{}, fmt.Errorf("request %q cannot fail from phase %q", request.RequestID, existing.Phase)
		}
		if !found && request.Phase != PhaseFailed {
			return service.State{}, fmt.Errorf("new failed request %q is not terminal", request.RequestID)
		}
		if found && request.Phase != PhaseFailed {
			return service.State{}, fmt.Errorf("request %q failure is not terminal", request.RequestID)
		}
	default:
		return service.State{}, fmt.Errorf("web gateway event type %q is unsupported", event.EventType)
	}
	state.Requests[request.RequestID] = request
	if request.Phase.terminal() {
		retainTerminalRequest(&state, request.RequestID)
	}
	return encodeState(state)
}

func sameRequestIdentity(left, right RequestState) bool {
	return left.RequestID == right.RequestID &&
		left.Operation == right.Operation &&
		left.UserID == right.UserID &&
		left.TaskID == right.TaskID &&
		left.TaskAddress == right.TaskAddress &&
		left.TaskInstanceID == right.TaskInstanceID &&
		left.IdentityFingerprint == right.IdentityFingerprint
}

func retainTerminalRequest(state *State, requestID string) {
	for index, value := range state.TerminalOrderIDs {
		if value == requestID {
			state.TerminalOrderIDs = append(state.TerminalOrderIDs[:index], state.TerminalOrderIDs[index+1:]...)
			break
		}
	}
	state.TerminalOrderIDs = append(state.TerminalOrderIDs, requestID)
	for len(state.TerminalOrderIDs) > RetainedTerminalRequests {
		oldest := state.TerminalOrderIDs[0]
		state.TerminalOrderIDs = state.TerminalOrderIDs[1:]
		delete(state.Requests, oldest)
	}
}

func validateTerminalProjection(state State) error {
	if len(state.TerminalOrderIDs) > RetainedTerminalRequests {
		return fmt.Errorf("state retains %d terminal requests; maximum is %d", len(state.TerminalOrderIDs), RetainedTerminalRequests)
	}
	seen := make(map[string]struct{}, len(state.TerminalOrderIDs))
	for _, requestID := range state.TerminalOrderIDs {
		if _, exists := seen[requestID]; exists {
			return fmt.Errorf("terminal request %q is duplicated", requestID)
		}
		request, found := state.Requests[requestID]
		if !found || !request.Phase.terminal() {
			return fmt.Errorf("terminal request %q is missing or not terminal", requestID)
		}
		seen[requestID] = struct{}{}
	}
	for requestID, request := range state.Requests {
		if request.Phase.terminal() {
			if _, found := seen[requestID]; !found {
				return fmt.Errorf("terminal request %q is absent from retention order", requestID)
			}
		}
	}
	return nil
}

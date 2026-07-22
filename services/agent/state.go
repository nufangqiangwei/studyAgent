package agent

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"encoding/json"
	"fmt"
	"time"
)

type capabilitiesResolvedPayload struct {
	RunID        string               `json:"run_id"`
	Capabilities []ResolvedCapability `json:"capabilities"`
}

type promptRequestedPayload struct {
	RunID string `json:"run_id"`
	Turn  int    `json:"turn"`
}

type promptPreparedPayload struct {
	RunID    string               `json:"run_id"`
	Turn     int                  `json:"turn"`
	Artifact contract.ArtifactRef `json:"artifact"`
}

type modelRequestedPayload struct {
	RunID     string `json:"run_id"`
	Turn      int    `json:"turn"`
	RequestID string `json:"request_id"`
}

type modelRejectedPayload struct {
	RunID       string               `json:"run_id"`
	Turn        int                  `json:"turn"`
	ResponseRef contract.ArtifactRef `json:"response_ref"`
	Feedback    string               `json:"feedback"`
}

type capabilityRequestedPayload struct {
	RunID       string               `json:"run_id"`
	Turn        int                  `json:"turn"`
	ResponseRef contract.ArtifactRef `json:"response_ref"`
	Action      ModelAction          `json:"action"`
	CallID      string               `json:"call_id"`
}

type capabilityObservedPayload struct {
	RunID   string            `json:"run_id"`
	Turn    int               `json:"turn"`
	Outcome CapabilityOutcome `json:"outcome"`
}

type outputRequestedPayload struct {
	RunID       string               `json:"run_id"`
	Turn        int                  `json:"turn"`
	ResponseRef contract.ArtifactRef `json:"response_ref"`
	Action      ModelAction          `json:"action"`
}

type runCompletedPayload struct {
	RunID       string               `json:"run_id"`
	Output      contract.ArtifactRef `json:"output"`
	CompletedAt time.Time            `json:"completed_at"`
}

type runTerminalPayload struct {
	RunID        string    `json:"run_id"`
	ErrorCode    string    `json:"error_code"`
	ErrorMessage string    `json:"error_message"`
	CompletedAt  time.Time `json:"completed_at"`
}

func encodeState(state aggregateState) (service.State, error) {
	if state.Runs == nil {
		state.Runs = make(map[string]RunState)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return service.State{}, fmt.Errorf("encode agent state: %w", err)
	}
	return service.State{SchemaVersion: StateSchema.Version, Data: payload}, nil
}

func decodeState(raw service.State) (aggregateState, error) {
	if raw.SchemaVersion != StateSchema.Version {
		return aggregateState{}, fmt.Errorf("agent state schema %d is unsupported", raw.SchemaVersion)
	}
	var state aggregateState
	if err := json.Unmarshal(raw.Data, &state); err != nil {
		return aggregateState{}, fmt.Errorf("decode agent state: %w", err)
	}
	if state.Runs == nil {
		state.Runs = make(map[string]RunState)
	}
	for id, run := range state.Runs {
		if id == "" || run.RunID != id || run.Phase == "" {
			return aggregateState{}, fmt.Errorf("agent state contains invalid run %q", id)
		}
		state.Runs[id] = run.clone()
	}
	return state, nil
}

func (s *agentService) Apply(raw service.State, event contract.StoredEvent) (service.State, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.State{}, err
	}
	if event.EventVersion != ProtocolVersion {
		return service.State{}, fmt.Errorf("agent event %q version %d is unsupported", event.EventType, event.EventVersion)
	}
	switch event.EventType {
	case runStartedEvent:
		var run RunState
		if err := json.Unmarshal(event.Payload, &run); err != nil {
			return service.State{}, fmt.Errorf("decode run started event: %w", err)
		}
		if run.RunID == "" || run.Phase != PhaseDiscoveringCapabilities {
			return service.State{}, fmt.Errorf("run started event is invalid")
		}
		if _, exists := state.Runs[run.RunID]; exists {
			return service.State{}, fmt.Errorf("run %q already exists", run.RunID)
		}
		state.Runs[run.RunID] = run.clone()
	case capabilitiesResolvedEvent:
		var payload capabilitiesResolvedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, err := requireRun(state, payload.RunID)
		if err != nil {
			return service.State{}, err
		}
		run.Capabilities = cloneCapabilities(payload.Capabilities)
		run.PendingCorrelation = ""
		state.Runs[run.RunID] = run
	case promptRequestedEvent:
		var payload promptRequestedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, err := requireRun(state, payload.RunID)
		if err != nil {
			return service.State{}, err
		}
		if payload.Turn <= 0 || payload.Turn != len(run.Turns)+1 {
			return service.State{}, fmt.Errorf("prompt turn %d is invalid for run %q", payload.Turn, run.RunID)
		}
		run.Turns = append(run.Turns, TurnRecord{Number: payload.Turn})
		run.Phase, run.PendingTurn = PhasePreparingPrompt, payload.Turn
		run.PendingCorrelation, run.PendingCallID = "", ""
		state.Runs[run.RunID] = run
	case promptPreparedEvent:
		var payload promptPreparedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, turn, err := requireTurn(state, payload.RunID, payload.Turn)
		if err != nil {
			return service.State{}, err
		}
		turn.PromptRef = cloneArtifact(&payload.Artifact)
		run.Turns[payload.Turn-1] = turn
		state.Runs[run.RunID] = run
	case modelRequestedEvent:
		var payload modelRequestedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, turn, err := requireTurn(state, payload.RunID, payload.Turn)
		if err != nil {
			return service.State{}, err
		}
		if payload.RequestID == "" {
			return service.State{}, fmt.Errorf("model request id is required")
		}
		turn.ModelRequestID = payload.RequestID
		run.Turns[payload.Turn-1] = turn
		run.Phase, run.PendingCorrelation = PhaseWaitingModel, payload.RequestID
		state.Runs[run.RunID] = run
	case modelRejectedEvent:
		var payload modelRejectedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, turn, err := requireTurn(state, payload.RunID, payload.Turn)
		if err != nil {
			return service.State{}, err
		}
		turn.ModelResponseRef = cloneArtifact(&payload.ResponseRef)
		turn.Feedback = payload.Feedback
		run.Turns[payload.Turn-1] = turn
		run.PendingCorrelation = ""
		state.Runs[run.RunID] = run
	case capabilityRequestedEvent:
		var payload capabilityRequestedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, turn, err := requireTurn(state, payload.RunID, payload.Turn)
		if err != nil {
			return service.State{}, err
		}
		turn.ModelResponseRef = cloneArtifact(&payload.ResponseRef)
		action := payload.Action.clone()
		turn.Action = &action
		run.Turns[payload.Turn-1] = turn
		run.Phase, run.PendingCallID, run.PendingCorrelation = PhaseWaitingCapability, payload.CallID, payload.CallID
		state.Runs[run.RunID] = run
	case capabilityResultObservedEvent:
		var payload capabilityObservedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, turn, err := requireTurn(state, payload.RunID, payload.Turn)
		if err != nil {
			return service.State{}, err
		}
		outcome := payload.Outcome.clone()
		turn.Capability = &outcome
		run.Turns[payload.Turn-1] = turn
		run.PendingCallID, run.PendingCorrelation = "", ""
		state.Runs[run.RunID] = run
	case outputRequestedEvent:
		var payload outputRequestedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, turn, err := requireTurn(state, payload.RunID, payload.Turn)
		if err != nil {
			return service.State{}, err
		}
		turn.ModelResponseRef = cloneArtifact(&payload.ResponseRef)
		action := payload.Action.clone()
		turn.Action = &action
		run.Turns[payload.Turn-1] = turn
		run.Phase, run.PendingCorrelation = PhaseFinalizing, ""
		state.Runs[run.RunID] = run
	case runCompletedEvent:
		var payload runCompletedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, err := requireRun(state, payload.RunID)
		if err != nil {
			return service.State{}, err
		}
		run.Phase, run.Output, run.CompletedAt = PhaseCompleted, cloneArtifact(&payload.Output), cloneTime(&payload.CompletedAt)
		run.PendingCallID, run.PendingCorrelation, run.PendingTurn = "", "", 0
		state.Runs[run.RunID] = run
	case runFailedEvent, runCancelledEvent:
		var payload runTerminalPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, err
		}
		run, err := requireRun(state, payload.RunID)
		if err != nil {
			return service.State{}, err
		}
		if event.EventType == runFailedEvent {
			run.Phase = PhaseFailed
		} else {
			run.Phase = PhaseCancelled
		}
		run.ErrorCode, run.ErrorMessage, run.CompletedAt = payload.ErrorCode, payload.ErrorMessage, cloneTime(&payload.CompletedAt)
		run.PendingCallID, run.PendingCorrelation, run.PendingTurn = "", "", 0
		state.Runs[run.RunID] = run
	default:
		return service.State{}, fmt.Errorf("unknown agent event %q", event.EventType)
	}
	return encodeState(state)
}

func requireRun(state aggregateState, runID string) (RunState, error) {
	run, found := state.Runs[runID]
	if !found {
		return RunState{}, fmt.Errorf("agent run %q was not found", runID)
	}
	return run.clone(), nil
}

func requireTurn(state aggregateState, runID string, number int) (RunState, TurnRecord, error) {
	run, err := requireRun(state, runID)
	if err != nil {
		return RunState{}, TurnRecord{}, err
	}
	if number <= 0 || number > len(run.Turns) || run.Turns[number-1].Number != number {
		return RunState{}, TurnRecord{}, fmt.Errorf("agent run %q turn %d was not found", runID, number)
	}
	return run, run.Turns[number-1].clone(), nil
}

func cloneCapabilities(values []ResolvedCapability) []ResolvedCapability {
	cloned := append([]ResolvedCapability(nil), values...)
	for index := range cloned {
		cloned[index] = cloned[index].clone()
	}
	return cloned
}

func cloneArtifact(value *contract.ArtifactRef) *contract.ArtifactRef {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

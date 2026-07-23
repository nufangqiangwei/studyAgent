package interaction

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"encoding/json"
	"fmt"
)

func initialState() State {
	return State{Requests: make(map[string]RequestState), TerminalOrderIDs: make([]string, 0, RetainedTerminalRequests)}
}

func encodeState(state State) (service.State, error) {
	if state.Requests == nil {
		state.Requests = make(map[string]RequestState)
	}
	state.TerminalOrderIDs = append([]string(nil), state.TerminalOrderIDs...)
	payload, err := json.Marshal(state)
	if err != nil {
		return service.State{}, fmt.Errorf("encode interaction state: %w", err)
	}
	return service.State{SchemaVersion: StateSchema.Version, Data: payload}, nil
}

func decodeState(raw service.State) (State, error) {
	if raw.SchemaVersion != StateSchema.Version {
		return State{}, fmt.Errorf("interaction state schema %d is unsupported", raw.SchemaVersion)
	}
	var state State
	if err := json.Unmarshal(raw.Data, &state); err != nil {
		return State{}, fmt.Errorf("decode interaction state: %w", err)
	}
	if state.Requests == nil {
		state.Requests = make(map[string]RequestState)
	}
	state.TerminalOrderIDs = append([]string(nil), state.TerminalOrderIDs...)
	for id, request := range state.Requests {
		if id != request.RequestID {
			return State{}, fmt.Errorf("interaction request map key %q does not match request id %q", id, request.RequestID)
		}
		request = request.clone()
		if err := request.validate(); err != nil {
			return State{}, fmt.Errorf("validate interaction request %q: %w", id, err)
		}
		state.Requests[id] = request
	}
	if err := validateTerminalProjection(state); err != nil {
		return State{}, err
	}
	return state, nil
}

func (*interactionService) Apply(raw service.State, event contract.StoredEvent) (service.State, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.State{}, err
	}
	if event.EventVersion != ProtocolVersion {
		return service.State{}, fmt.Errorf("interaction event %q version %d is unsupported", event.EventType, event.EventVersion)
	}
	var request RequestState
	if err := json.Unmarshal(event.Payload, &request); err != nil {
		return service.State{}, fmt.Errorf("decode interaction event %q: %w", event.EventType, err)
	}
	if err := request.validate(); err != nil {
		return service.State{}, fmt.Errorf("validate interaction event %q: %w", event.EventType, err)
	}
	existing, found := state.Requests[request.RequestID]
	switch event.EventType {
	case requestSubmittedEvent:
		if found || request.Phase != PhaseRunning {
			return service.State{}, fmt.Errorf("interaction request %q cannot be submitted", request.RequestID)
		}
	case requestCompletedEvent:
		if !found || existing.Phase != PhaseRunning || request.Phase != PhaseCompleted || existing.IdentityFingerprint != request.IdentityFingerprint {
			return service.State{}, fmt.Errorf("interaction request %q cannot transition to %q", request.RequestID, request.Phase)
		}
	case requestFailedEvent:
		if !found || existing.Phase != PhaseRunning || request.Phase != PhaseFailed || existing.IdentityFingerprint != request.IdentityFingerprint {
			return service.State{}, fmt.Errorf("interaction request %q cannot transition to %q", request.RequestID, request.Phase)
		}
	default:
		return service.State{}, fmt.Errorf("interaction event type %q is unsupported", event.EventType)
	}
	state.Requests[request.RequestID] = request.clone()
	if request.Phase.terminal() {
		retainTerminalRequest(&state, request.RequestID)
	}
	return encodeState(state)
}

func retainTerminalRequest(state *State, requestID string) {
	if state == nil {
		return
	}
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
		return fmt.Errorf("interaction state retains %d terminal requests; maximum is %d", len(state.TerminalOrderIDs), RetainedTerminalRequests)
	}
	seen := make(map[string]struct{}, len(state.TerminalOrderIDs))
	for _, requestID := range state.TerminalOrderIDs {
		if _, exists := seen[requestID]; exists {
			return fmt.Errorf("interaction terminal request %q is duplicated", requestID)
		}
		request, found := state.Requests[requestID]
		if !found || !request.Phase.terminal() {
			return fmt.Errorf("interaction terminal request %q is missing or not terminal", requestID)
		}
		seen[requestID] = struct{}{}
	}
	for requestID, request := range state.Requests {
		if !request.Phase.terminal() {
			continue
		}
		if _, found := seen[requestID]; !found {
			return fmt.Errorf("interaction terminal request %q is absent from retention order", requestID)
		}
	}
	return nil
}

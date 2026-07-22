package capability

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"encoding/json"
	"fmt"
	"time"
)

type authorizationStateEventPayload struct {
	CallID      string                    `json:"call_id"`
	Decision    AuthorizationDecisionKind `json:"decision"`
	RuleRef     string                    `json:"rule_ref"`
	ReasonCode  string                    `json:"reason_code"`
	RiskSummary string                    `json:"risk_summary,omitempty"`
	ApprovalID  string                    `json:"approval_id,omitempty"`
	Approval    contract.ServiceAddress   `json:"approval_service,omitempty"`
	Phase       CallPhase                 `json:"phase"`
	CompletedAt *time.Time                `json:"completed_at,omitempty"`
}

type executionStateEventPayload struct {
	CallID                 string                  `json:"call_id"`
	Kind                   ExecutionKind           `json:"kind"`
	ExecutionKey           string                  `json:"execution_key"`
	Generation             uint64                  `json:"generation"`
	ExecutorRef            string                  `json:"executor_ref,omitempty"`
	EffectType             contract.EffectType     `json:"effect_type,omitempty"`
	Target                 contract.ServiceAddress `json:"target,omitempty"`
	MessageType            contract.MessageType    `json:"message_type,omitempty"`
	MessageVersion         int                     `json:"message_version,omitempty"`
	ReplyType              contract.MessageType    `json:"reply_type,omitempty"`
	ReplyVersion           int                     `json:"reply_version,omitempty"`
	ExecutionCorrelationID string                  `json:"execution_correlation_id"`
	ApprovalDecision       string                  `json:"approval_decision,omitempty"`
	ApprovalDecidedBy      string                  `json:"approval_decided_by,omitempty"`
	ApprovalDecidedAt      *time.Time              `json:"approval_decided_at,omitempty"`
}

type terminalStateEventPayload struct {
	CallID            string                `json:"call_id"`
	Phase             CallPhase             `json:"phase"`
	ResultRef         *contract.ArtifactRef `json:"result_ref,omitempty"`
	Result            json.RawMessage       `json:"result,omitempty"`
	ErrorCode         string                `json:"error_code,omitempty"`
	ErrorMessage      string                `json:"error_message,omitempty"`
	CompletedAt       time.Time             `json:"completed_at"`
	ApprovalDecision  string                `json:"approval_decision,omitempty"`
	ApprovalDecidedBy string                `json:"approval_decided_by,omitempty"`
	ApprovalDecidedAt *time.Time            `json:"approval_decided_at,omitempty"`
}

type lateOutcomeStateEventPayload struct {
	CallID    string                `json:"call_id"`
	OutcomeID string                `json:"outcome_id"`
	Kind      string                `json:"kind"`
	ResultRef *contract.ArtifactRef `json:"result_ref,omitempty"`
	Result    json.RawMessage       `json:"result,omitempty"`
	ErrorCode string                `json:"error_code,omitempty"`
}

type compactedStateEventPayload struct {
	CallID    string        `json:"call_id"`
	Tombstone CallTombstone `json:"tombstone"`
}

type tombstoneRemovedStateEventPayload struct {
	CallID string `json:"call_id"`
}

func initialAggregateState() aggregateState {
	return aggregateState{Calls: make(map[string]CallState), Tombstones: make(map[string]CallTombstone)}
}

func encodeState(value aggregateState) (service.State, error) {
	if value.Calls == nil {
		value.Calls = make(map[string]CallState)
	}
	if value.Tombstones == nil {
		value.Tombstones = make(map[string]CallTombstone)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return service.State{}, fmt.Errorf("encode capability state: %w", err)
	}
	return service.State{SchemaVersion: StateSchema.Version, Data: data}, nil
}

func decodeState(raw service.State) (aggregateState, error) {
	if raw.SchemaVersion != StateSchema.Version {
		return aggregateState{}, fmt.Errorf("capability state schema version %d is unsupported", raw.SchemaVersion)
	}
	if len(raw.Data) == 0 {
		return aggregateState{}, fmt.Errorf("capability state data is empty")
	}
	var value aggregateState
	if err := json.Unmarshal(raw.Data, &value); err != nil {
		return aggregateState{}, fmt.Errorf("decode capability state: %w", err)
	}
	if value.Calls == nil {
		value.Calls = make(map[string]CallState)
	}
	if value.Tombstones == nil {
		value.Tombstones = make(map[string]CallTombstone)
	}
	calls := make(map[string]CallState, len(value.Calls))
	for id, call := range value.Calls {
		if clean(id) == "" || call.CallID != id || !call.Phase.Valid() || clean(call.IdentityFingerprint) == "" {
			return aggregateState{}, fmt.Errorf("capability state contains an invalid call %q", id)
		}
		calls[id] = call.Clone()
	}
	tombstones := make(map[string]CallTombstone, len(value.Tombstones))
	for id, tombstone := range value.Tombstones {
		if clean(id) == "" || tombstone.CallID != id || !tombstone.Phase.Terminal() ||
			tombstone.CompletedAt.IsZero() || !tombstone.ExpiresAt.After(tombstone.CompletedAt) {
			return aggregateState{}, fmt.Errorf("capability state contains an invalid tombstone %q", id)
		}
		tombstones[id] = tombstone.Clone()
	}
	value.Calls, value.Tombstones = calls, tombstones
	return value, nil
}

func applyStoredEvent(raw service.State, event contract.StoredEvent) (service.State, error) {
	if event.EventVersion != ProtocolVersion {
		return service.State{}, fmt.Errorf("unsupported capability event %q v%d", event.EventType, event.EventVersion)
	}
	state, err := decodeState(raw)
	if err != nil {
		return service.State{}, err
	}
	switch event.EventType {
	case callReceivedStateEvent:
		var call CallState
		if err := json.Unmarshal(event.Payload, &call); err != nil {
			return service.State{}, fmt.Errorf("decode capability received event: %w", err)
		}
		if call.CallID == "" || call.Phase != PhaseReceived || call.IdentityFingerprint == "" {
			return service.State{}, fmt.Errorf("capability received event is invalid")
		}
		if _, exists := state.Calls[call.CallID]; exists {
			return service.State{}, fmt.Errorf("capability received event duplicates call %q", call.CallID)
		}
		if _, exists := state.Tombstones[call.CallID]; exists {
			return service.State{}, fmt.Errorf("capability received event conflicts with tombstone %q", call.CallID)
		}
		state.Calls[call.CallID] = call.Clone()
	case callAuthorizationStateEvent:
		var payload authorizationStateEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, fmt.Errorf("decode capability authorization event: %w", err)
		}
		call, exists := state.Calls[payload.CallID]
		if !exists || call.Phase != PhaseReceived || !payload.Decision.Valid() {
			return service.State{}, fmt.Errorf("capability authorization event violates state invariant")
		}
		switch payload.Decision {
		case AuthorizationAllow:
			if payload.Phase != PhaseAuthorized || payload.ApprovalID != "" || payload.Approval != "" {
				return service.State{}, fmt.Errorf("allowed capability authorization event is invalid")
			}
		case AuthorizationAsk:
			if payload.Phase != PhaseWaitingApproval || clean(payload.ApprovalID) == "" || payload.Approval == "" {
				return service.State{}, fmt.Errorf("approval capability authorization event is invalid")
			}
		case AuthorizationDeny:
			if payload.Phase != PhaseReceived || payload.ApprovalID != "" || payload.Approval != "" {
				return service.State{}, fmt.Errorf("denied capability authorization event is invalid")
			}
		}
		call.AuthorizationDecision, call.AuthorizationRule = payload.Decision, payload.RuleRef
		call.AuthorizationReason, call.RiskSummary = payload.ReasonCode, payload.RiskSummary
		call.ApprovalID, call.ApprovalService, call.Phase = payload.ApprovalID, payload.Approval, payload.Phase
		call.CompletedAt = cloneTime(payload.CompletedAt)
		state.Calls[call.CallID] = call.Clone()
	case callExecutionStateEvent:
		var payload executionStateEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, fmt.Errorf("decode capability execution event: %w", err)
		}
		call, exists := state.Calls[payload.CallID]
		if !exists || (call.Phase != PhaseAuthorized && call.Phase != PhaseWaitingApproval) ||
			!payload.Kind.Valid() || clean(payload.ExecutionKey) == "" || payload.Generation == 0 || clean(payload.ExecutionCorrelationID) == "" {
			return service.State{}, fmt.Errorf("capability execution event violates state invariant")
		}
		if call.Phase == PhaseWaitingApproval && (payload.ApprovalDecision != "approve" || payload.ApprovalDecidedAt == nil) {
			return service.State{}, fmt.Errorf("approved capability execution event lacks approval facts")
		}
		if payload.Kind == ExecutionEffect && (clean(payload.ExecutorRef) == "" || payload.EffectType == "" || payload.Target != "" || payload.MessageType != "" || payload.ReplyType != "") {
			return service.State{}, fmt.Errorf("capability effect execution event is invalid")
		}
		if payload.Kind == ExecutionServiceCommand && (payload.Target == "" || payload.MessageType == "" || payload.MessageVersion <= 0 || payload.ReplyType == "" || payload.ReplyVersion <= 0 || payload.ExecutorRef != "" || payload.EffectType != "") {
			return service.State{}, fmt.Errorf("capability service-command execution event is invalid")
		}
		call.Phase, call.ExecutionKind = PhaseWaitingExecution, payload.Kind
		call.ExecutionKey, call.ExecutionGeneration = payload.ExecutionKey, payload.Generation
		call.ExecutorRef, call.EffectType = payload.ExecutorRef, payload.EffectType
		call.ExecutionTarget, call.ExecutionMessageType = payload.Target, payload.MessageType
		call.ExecutionMessageVersion = payload.MessageVersion
		call.ExecutionReplyType, call.ExecutionReplyVersion = payload.ReplyType, payload.ReplyVersion
		call.ExecutionCorrelationID = payload.ExecutionCorrelationID
		call.ApprovalDecision, call.ApprovalDecidedBy = payload.ApprovalDecision, payload.ApprovalDecidedBy
		call.ApprovalDecidedAt = cloneTime(payload.ApprovalDecidedAt)
		state.Calls[call.CallID] = call.Clone()
	case callTerminalStateEvent:
		var payload terminalStateEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, fmt.Errorf("decode capability terminal event: %w", err)
		}
		call, exists := state.Calls[payload.CallID]
		if !exists || call.Phase.Terminal() || !payload.Phase.Terminal() || payload.CompletedAt.IsZero() {
			return service.State{}, fmt.Errorf("capability terminal event violates state invariant")
		}
		if payload.Phase == PhaseSucceeded && payload.ErrorCode != "" {
			return service.State{}, fmt.Errorf("successful capability terminal event contains an error")
		}
		if payload.Phase != PhaseSucceeded && clean(payload.ErrorCode) == "" {
			return service.State{}, fmt.Errorf("failed capability terminal event lacks an error code")
		}
		call.Phase, call.Result = payload.Phase, contract.CloneRaw(payload.Result)
		if payload.ResultRef != nil {
			value := *payload.ResultRef
			call.ResultRef = &value
		} else {
			call.ResultRef = nil
		}
		call.ErrorCode, call.ErrorMessage = payload.ErrorCode, payload.ErrorMessage
		if payload.ApprovalDecision != "" {
			call.ApprovalDecision, call.ApprovalDecidedBy = payload.ApprovalDecision, payload.ApprovalDecidedBy
			call.ApprovalDecidedAt = cloneTime(payload.ApprovalDecidedAt)
		}
		call.CompletedAt = cloneTime(&payload.CompletedAt)
		state.Calls[call.CallID] = call.Clone()
	case callLateOutcomeStateEvent:
		var payload lateOutcomeStateEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, fmt.Errorf("decode capability late-outcome event: %w", err)
		}
		call, exists := state.Calls[payload.CallID]
		if !exists || !call.Phase.Terminal() || clean(payload.OutcomeID) == "" || (payload.Kind != "succeeded" && payload.Kind != "failed") {
			return service.State{}, fmt.Errorf("capability late-outcome event violates state invariant")
		}
		call.LateOutcomeID = payload.OutcomeID
		call.LateOutcomeKind = payload.Kind
		call.LateResult = contract.CloneRaw(payload.Result)
		if payload.ResultRef != nil {
			value := *payload.ResultRef
			call.LateResultRef = &value
		} else {
			call.LateResultRef = nil
		}
		call.LateErrorCode = payload.ErrorCode
		state.Calls[call.CallID] = call.Clone()
	case callCompactedStateEvent:
		var payload compactedStateEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, fmt.Errorf("decode capability compacted event: %w", err)
		}
		call, exists := state.Calls[payload.CallID]
		if !exists || !call.Phase.Terminal() || payload.Tombstone.CallID != payload.CallID ||
			payload.Tombstone.Phase != call.Phase || payload.Tombstone.IdentityFingerprint != call.IdentityFingerprint ||
			!payload.Tombstone.ExpiresAt.After(payload.Tombstone.CompletedAt) {
			return service.State{}, fmt.Errorf("capability compacted event violates state invariant")
		}
		delete(state.Calls, payload.CallID)
		state.Tombstones[payload.CallID] = payload.Tombstone.Clone()
	case tombstoneRemovedStateEvent:
		var payload tombstoneRemovedStateEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return service.State{}, fmt.Errorf("decode capability tombstone-removed event: %w", err)
		}
		if _, exists := state.Tombstones[payload.CallID]; !exists {
			return service.State{}, fmt.Errorf("capability tombstone %q does not exist", payload.CallID)
		}
		delete(state.Tombstones, payload.CallID)
	default:
		return service.State{}, fmt.Errorf("unsupported capability event %q v%d", event.EventType, event.EventVersion)
	}
	return encodeState(state)
}

package interaction

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/agent"
	"agent/services/approval"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	errInvalidRequest  = "interaction_invalid_request"
	errRequestConflict = "interaction_request_conflict"
	errAgentFailure    = "interaction_agent_failed"
)

type interactionService struct {
	address      contract.ServiceAddress
	agentAddress contract.ServiceAddress
	clock        contract.Clock
}

func (*interactionService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: Component, StateSchema: StateSchema}
}

func (*interactionService) InitialState(context.Context, service.Init) (service.State, error) {
	return encodeState(initialState())
}

func (s *interactionService) Handle(_ context.Context, raw service.State, message contract.Message) (service.Decision, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.Decision{}, err
	}
	if message.Version != ProtocolVersion {
		return service.Decision{}, fmt.Errorf("unsupported interaction message version %d", message.Version)
	}
	switch {
	case message.Kind == contract.MessageCommand && message.Type == SubmitMessageType:
		return s.handleSubmit(state, message)
	case message.Kind == contract.MessageReply && message.Type == agent.CompletedMessageType:
		return s.handleAgentCompleted(state, message)
	case message.Kind == contract.MessageEvent && message.Type == approval.RequestedEventType:
		return s.handleApprovalRequested(message)
	default:
		return service.Decision{}, fmt.Errorf("unsupported interaction message %s %q v%d", message.Kind, message.Type, message.Version)
	}
}

func (s *interactionService) handleSubmit(state State, message contract.Message) (service.Decision, error) {
	var input SubmitRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return s.presentationOnly(errorPresentation(message.ID, message.ID, errInvalidRequest, "interaction request payload is invalid"))
	}
	input.RequestID = strings.TrimSpace(input.RequestID)
	if input.RequestID == "" {
		input.RequestID = strings.TrimSpace(message.RunID)
	}
	if input.RequestID == "" {
		input.RequestID = message.ID
	}
	if strings.TrimSpace(input.Input) == "" && input.InputArtifact == nil {
		return s.presentationOnly(errorPresentation(input.RequestID, input.RequestID, errInvalidRequest, "interaction input is required"))
	}
	if int64(len(input.Input)) > MaxInlineInputBytes {
		return s.presentationOnly(errorPresentation(input.RequestID, input.RequestID, errInvalidRequest, "inline interaction input is too large; use an input artifact"))
	}
	if input.InputArtifact != nil {
		if err := artifact.ValidateRef(*input.InputArtifact); err != nil {
			return s.presentationOnly(errorPresentation(input.RequestID, input.RequestID, errInvalidRequest, "interaction input artifact is invalid"))
		}
	}
	fingerprint, err := requestFingerprint(message, input)
	if err != nil {
		return service.Decision{}, err
	}
	if existing, found := state.Requests[input.RequestID]; found {
		if existing.IdentityFingerprint != fingerprint {
			return s.presentationOnly(errorPresentation(input.RequestID, existing.RunID, errRequestConflict, "request id is already bound to different input"))
		}
		if existing.Phase.terminal() {
			return s.presentationOnly(presentationForRequest(existing))
		}
		return service.Decision{}, nil
	}

	request := RequestState{
		RequestID: input.RequestID, RunID: input.RequestID, Caller: message.From,
		UserID: message.UserID, GoalID: message.GoalID, IdentityFingerprint: fingerprint,
		Phase: PhaseRunning, StartedAt: s.now(),
	}
	eventPayload, err := json.Marshal(request)
	if err != nil {
		return service.Decision{}, err
	}
	agentPayload, err := json.Marshal(agent.ExecuteRequest{
		RunID: input.RequestID, Input: input.Input, InputArtifact: cloneArtifact(input.InputArtifact),
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{{
			Key:  "interaction-request-submitted/" + input.RequestID,
			Type: requestSubmittedEvent, Version: ProtocolVersion, Payload: eventPayload,
		}},
		Outgoing: []service.OutgoingMessage{{
			Key:  "interaction-agent-execute/" + input.RequestID,
			Kind: contract.MessageCommand, Type: agent.ExecuteMessageType, Version: agent.ProtocolVersion,
			To: s.agentAddress, ReplyTo: s.address, CorrelationID: input.RequestID, Payload: agentPayload,
		}},
	}, nil
}

func (s *interactionService) handleAgentCompleted(state State, message contract.Message) (service.Decision, error) {
	requestID := strings.TrimSpace(message.RunID)
	if requestID == "" {
		requestID = strings.TrimSpace(message.CorrelationID)
	}
	request, found := state.Requests[requestID]
	if !found {
		return service.Decision{}, fmt.Errorf("interaction request %q was not found for agent completion", requestID)
	}
	if request.Phase.terminal() {
		return service.Decision{}, nil
	}
	completedAt := s.now()
	request.CompletedAt = &completedAt
	eventType := requestCompletedEvent
	if isReplyError(message) {
		var replyError service.ReplyError
		if err := json.Unmarshal(message.Payload, &replyError); err != nil {
			replyError = service.ReplyError{Code: errAgentFailure, Message: "agent returned an invalid error reply"}
		}
		request.Phase, request.ErrorCode, request.ErrorMessage = PhaseFailed, cleanErrorCode(replyError.Code), cleanErrorMessage(replyError.Message)
		eventType = requestFailedEvent
	} else {
		var result agent.ExecuteResult
		if err := json.Unmarshal(message.Payload, &result); err != nil {
			return service.Decision{}, fmt.Errorf("decode agent completion: %w", err)
		}
		if result.RunID != request.RunID {
			return service.Decision{}, fmt.Errorf("agent completion run %q does not match interaction run %q", result.RunID, request.RunID)
		}
		if result.Phase == agent.PhaseCompleted && result.Output != nil {
			if err := artifact.ValidateRef(*result.Output); err != nil {
				return service.Decision{}, fmt.Errorf("validate agent output: %w", err)
			}
			request.Phase, request.Output = PhaseCompleted, cloneArtifact(result.Output)
		} else {
			request.Phase = PhaseFailed
			request.ErrorCode, request.ErrorMessage = cleanErrorCode(result.ErrorCode), cleanErrorMessage(result.ErrorMessage)
			eventType = requestFailedEvent
		}
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return service.Decision{}, err
	}
	presentation, err := plannedPresentation(presentationForRequest(request))
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{{
			Key:  "interaction-request-terminal/" + request.RequestID,
			Type: eventType, Version: ProtocolVersion, Payload: payload,
		}},
		Effects: []service.PlannedEffect{presentation},
	}, nil
}

func (s *interactionService) handleApprovalRequested(message contract.Message) (service.Decision, error) {
	var requested approval.Requested
	if err := json.Unmarshal(message.Payload, &requested); err != nil {
		return service.Decision{}, fmt.Errorf("decode approval notification: %w", err)
	}
	if strings.TrimSpace(requested.ApprovalID) == "" {
		return service.Decision{}, fmt.Errorf("approval notification requires approval id")
	}
	presentation := Presentation{
		ID: "approval/" + requested.ApprovalID, Kind: PresentationApproval,
		RequestID: message.RunID, RunID: message.RunID, Approval: &requested,
		ErrorCode:    "interaction_approval_not_supported",
		ErrorMessage: "this CLI build can display approval requests but cannot resolve them yet",
	}
	return s.presentationOnly(presentation)
}

func (s *interactionService) presentationOnly(presentation Presentation) (service.Decision, error) {
	planned, err := plannedPresentation(presentation)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Effects: []service.PlannedEffect{planned}}, nil
}

func plannedPresentation(presentation Presentation) (service.PlannedEffect, error) {
	payload, err := json.Marshal(presentation.clone())
	if err != nil {
		return service.PlannedEffect{}, fmt.Errorf("encode interaction presentation: %w", err)
	}
	return service.PlannedEffect{
		Key:  "interaction-presentation/" + presentation.ID,
		Type: PresentEffectType, Version: ProtocolVersion, ExecutorRef: PresentExecutorRef,
		IdempotencyKey: "interaction/presentation/" + presentation.ID, Payload: payload,
	}, nil
}

func presentationForRequest(request RequestState) Presentation {
	value := Presentation{
		ID:        "request/" + request.RequestID + "/" + string(request.Phase),
		RequestID: request.RequestID, RunID: request.RunID,
	}
	if request.Phase == PhaseCompleted {
		value.Kind, value.Output = PresentationAnswer, cloneArtifact(request.Output)
	} else {
		value.Kind, value.ErrorCode, value.ErrorMessage = PresentationError, request.ErrorCode, request.ErrorMessage
	}
	return value
}

func errorPresentation(requestID, runID, code, message string) Presentation {
	return Presentation{
		ID: "request/" + requestID + "/error/" + code, Kind: PresentationError,
		RequestID: requestID, RunID: runID, ErrorCode: code, ErrorMessage: message,
	}
}

func requestFingerprint(message contract.Message, input SubmitRequest) (string, error) {
	value := struct {
		Caller        contract.ServiceAddress `json:"caller"`
		UserID        string                  `json:"user_id"`
		GoalID        string                  `json:"goal_id"`
		Input         string                  `json:"input"`
		InputArtifact *contract.ArtifactRef   `json:"input_artifact,omitempty"`
	}{message.From, message.UserID, message.GoalID, input.Input, cloneArtifact(input.InputArtifact)}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func cloneArtifact(value *contract.ArtifactRef) *contract.ArtifactRef {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cleanErrorCode(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return errAgentFailure
}

func cleanErrorMessage(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "agent execution failed"
}

func isReplyError(message contract.Message) bool {
	return message.Metadata[contract.MetadataReplyError] == "true"
}

func (s *interactionService) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock.Now().UTC()
}

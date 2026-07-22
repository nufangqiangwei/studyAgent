package agent

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/capability"
	"agent/services/llmClient"
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
	errInvalidRequest      = "agent_invalid_request"
	errRunConflict         = "agent_run_conflict"
	errRunNotFound         = "agent_run_not_found"
	errAccessDenied        = "agent_access_denied"
	errDeadlineExpired     = "agent_deadline_expired"
	errCapabilityDiscovery = "agent_capability_discovery_failed"
	errModelFailed         = "agent_model_failed"
	errModelProtocol       = "agent_model_protocol_invalid"
	errCapabilityResult    = "agent_capability_result_invalid"
	errTurnLimit           = "agent_turn_limit_reached"
	errArtifactPreparation = "agent_artifact_preparation_failed"
	errCancelled           = "agent_cancelled"
)

type agentService struct {
	address           contract.ServiceAddress
	modelAddress      contract.ServiceAddress
	capabilityAddress contract.ServiceAddress
	spec              AgentSpec
	artifacts         artifact.Reader
	clock             contract.Clock
}

func (s *agentService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: Component, StateSchema: StateSchema}
}

func (*agentService) InitialState(context.Context, service.Init) (service.State, error) {
	return encodeState(initialAggregateState())
}

func (s *agentService) Handle(ctx context.Context, raw service.State, message contract.Message) (service.Decision, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.Decision{}, err
	}
	if message.Version != ProtocolVersion {
		return service.Decision{}, fmt.Errorf("unsupported agent message version %d", message.Version)
	}
	switch {
	case message.Kind == contract.MessageCommand && message.Type == ExecuteMessageType:
		return s.handleExecute(state, message)
	case message.Kind == contract.MessageCommand && message.Type == CancelMessageType:
		return s.handleCancel(state, message)
	case message.Kind == contract.MessageQuery && message.Type == GetMessageType:
		return s.handleGet(state, message)
	case message.Kind == contract.MessageReply && message.Type == llmClient.CompletedMessageType:
		return s.handleModelReply(ctx, state, message)
	case message.Kind == contract.MessageReply && message.Type == capability.ResultMessageType:
		return s.handleCapabilityReply(state, message)
	case message.Kind == contract.MessageEvent && message.Type == ArtifactPreparedMessageType:
		return s.handleArtifactPrepared(state, message)
	case message.Kind == contract.MessageEvent && message.Type == ArtifactFailedMessageType:
		return s.handleArtifactFailed(state, message)
	default:
		return service.Decision{}, fmt.Errorf("unsupported agent message %s %q v%d", message.Kind, message.Type, message.Version)
	}
}

func (s *agentService) handleExecute(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, fmt.Errorf("agent execute requires reply_to")
	}
	if message.Deadline != nil && !message.Deadline.After(s.now()) {
		return rejection(errDeadlineExpired, "agent run deadline has expired"), nil
	}
	var input ExecuteRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return rejection(errInvalidRequest, "agent execute payload is invalid"), nil
	}
	input.RunID = strings.TrimSpace(input.RunID)
	if input.RunID == "" {
		input.RunID = strings.TrimSpace(message.RunID)
	}
	if input.RunID == "" {
		input.RunID = message.ID
	}
	if input.RunID == "" || (strings.TrimSpace(input.Input) == "" && input.InputArtifact == nil) {
		return rejection(errInvalidRequest, "run id and input are required"), nil
	}
	if len(input.Input) > maxInlineAgentInputBytes {
		return rejection(errInvalidRequest, "inline agent input is too large; use input_artifact"), nil
	}
	if err := validateInputArtifact(input.InputArtifact); err != nil {
		return rejection(errInvalidRequest, "input artifact is invalid"), nil
	}
	fingerprint := runFingerprint(message, input)
	if existing, found := state.Runs[input.RunID]; found {
		if existing.IdentityFingerprint != fingerprint {
			return rejection(errRunConflict, "run id is already bound to another invocation"), nil
		}
		if existing.Phase.Terminal() {
			return resultReply(existing, "agent-execute-terminal/"+input.RunID), nil
		}
		return statusReply(existing, "agent-execute-idempotent/"+input.RunID), nil
	}
	correlationID := message.CorrelationID
	if correlationID == "" {
		correlationID = message.ID
	}
	pending := input.RunID + "/capabilities"
	run := RunState{
		RunID: input.RunID, Phase: PhaseDiscoveringCapabilities,
		Caller: message.From, ReplyTo: message.ReplyTo, UserID: message.UserID, GoalID: message.GoalID,
		CorrelationID: correlationID, IdentityFingerprint: fingerprint,
		Input: input.Input, InputArtifact: cloneArtifact(input.InputArtifact),
		PendingCorrelation: pending, Deadline: cloneTime(message.Deadline), StartedAt: s.now(),
	}
	eventPayload, err := json.Marshal(run)
	if err != nil {
		return service.Decision{}, err
	}
	queryPayload, _ := json.Marshal(capability.ListRequest{})
	return service.Decision{
		Events: []service.NewEvent{{
			Key: "agent-run-started/" + run.RunID, Type: runStartedEvent,
			Version: ProtocolVersion, Payload: eventPayload,
		}},
		Outgoing: []service.OutgoingMessage{{
			Key:  "agent-capabilities-query/" + run.RunID,
			Kind: contract.MessageQuery, Type: capability.ListMessageType, Version: capability.ProtocolVersion,
			To: s.capabilityAddress, ReplyTo: s.address, CorrelationID: pending,
			Deadline: cloneTime(run.Deadline), Payload: queryPayload,
		}},
	}, nil
}

func (s *agentService) handleCapabilityReply(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.capabilityAddress {
		return service.Decision{}, fmt.Errorf("capability reply came from unexpected address %q", message.From)
	}
	run, found := findPendingRun(state, message.CorrelationID)
	if !found || run.Phase.Terminal() {
		return service.Decision{}, nil
	}
	if s.expired(run) {
		return s.failRun(run, errDeadlineExpired, "agent run deadline has expired")
	}
	switch run.Phase {
	case PhaseDiscoveringCapabilities:
		if replyHasError(message) {
			return s.failRun(run, errCapabilityDiscovery, "capability catalog query was rejected")
		}
		var response capability.ListResponse
		if err := json.Unmarshal(message.Payload, &response); err != nil {
			return s.failRun(run, errCapabilityDiscovery, "capability catalog response is invalid")
		}
		resolved, err := resolveCapabilities(s.spec, response)
		if err != nil {
			return s.failRun(run, errCapabilityDiscovery, err.Error())
		}
		payload, err := json.Marshal(capabilitiesResolvedPayload{RunID: run.RunID, Capabilities: resolved})
		if err != nil {
			return service.Decision{}, err
		}
		if len(payload) > maxInlineAgentEffectBytes {
			return s.failRun(run, errCapabilityDiscovery, "resolved capability view is too large")
		}
		decision := service.Decision{Events: []service.NewEvent{{
			Key:  "agent-capabilities-resolved/" + run.RunID,
			Type: capabilitiesResolvedEvent, Version: ProtocolVersion, Payload: payload,
		}}}
		run.Capabilities, run.PendingCorrelation = cloneCapabilities(resolved), ""
		prompt, err := s.schedulePrompt(run, 1)
		return mergeDecision(decision, prompt), err
	case PhaseWaitingCapability:
		return s.observeCapabilityResult(run, message)
	default:
		return service.Decision{}, nil
	}
}

func (s *agentService) handleArtifactPrepared(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.address {
		return service.Decision{}, fmt.Errorf("agent artifact notification came from unexpected address %q", message.From)
	}
	var prepared artifactPrepared
	if err := json.Unmarshal(message.Payload, &prepared); err != nil {
		return service.Decision{}, fmt.Errorf("decode prepared agent artifact: %w", err)
	}
	if err := artifact.ValidateRef(prepared.Artifact); err != nil {
		return service.Decision{}, fmt.Errorf("prepared agent artifact is invalid: %w", err)
	}
	run, found := state.Runs[prepared.RunID]
	if !found || run.Phase.Terminal() {
		return service.Decision{}, nil
	}
	if s.expired(run) {
		return s.failRun(run, errDeadlineExpired, "agent run deadline has expired")
	}
	switch prepared.Operation {
	case preparePrompt:
		if run.Phase != PhasePreparingPrompt || run.PendingTurn != prepared.Turn || prepared.Turn <= 0 || prepared.Turn > len(run.Turns) {
			return service.Decision{}, fmt.Errorf("prepared prompt does not match run %q", run.RunID)
		}
		requestID := run.RunID + "/model/" + strconv.Itoa(prepared.Turn)
		preparedPayload, _ := json.Marshal(promptPreparedPayload{RunID: run.RunID, Turn: prepared.Turn, Artifact: prepared.Artifact})
		requestedPayload, _ := json.Marshal(modelRequestedPayload{RunID: run.RunID, Turn: prepared.Turn, RequestID: requestID})
		completionPayload, err := json.Marshal(llmClient.CompletionRequest{
			RequestID:     requestID,
			System:        "Follow the response protocol contained in the input artifact.",
			InputArtifact: cloneArtifact(&prepared.Artifact),
			Temperature:   cloneFloat(s.spec.Temperature), MaxTokens: s.spec.MaxTokens,
		})
		if err != nil {
			return service.Decision{}, err
		}
		return service.Decision{
			Events: []service.NewEvent{
				{Key: "agent-prompt-prepared/" + requestID, Type: promptPreparedEvent, Version: ProtocolVersion, Payload: preparedPayload},
				{Key: "agent-model-requested/" + requestID, Type: modelRequestedEvent, Version: ProtocolVersion, Payload: requestedPayload},
			},
			Outgoing: []service.OutgoingMessage{{
				Key:  "agent-model-command/" + requestID,
				Kind: contract.MessageCommand, Type: llmClient.CompleteMessageType, Version: llmClient.ProtocolVersion,
				To: s.modelAddress, ReplyTo: s.address, CorrelationID: requestID,
				Deadline: cloneTime(run.Deadline), Payload: completionPayload,
			}},
		}, nil
	case prepareOutput:
		if run.Phase != PhaseFinalizing || run.PendingTurn != prepared.Turn {
			return service.Decision{}, fmt.Errorf("prepared output does not match run %q", run.RunID)
		}
		completedAt := s.now()
		payload, _ := json.Marshal(runCompletedPayload{RunID: run.RunID, Output: prepared.Artifact, CompletedAt: completedAt})
		completed := run.clone()
		completed.Phase, completed.Output, completed.CompletedAt = PhaseCompleted, cloneArtifact(&prepared.Artifact), &completedAt
		resultPayload, _ := json.Marshal(executeResult(completed))
		return service.Decision{
			Events: []service.NewEvent{{
				Key: "agent-run-completed/" + run.RunID, Type: runCompletedEvent,
				Version: ProtocolVersion, Payload: payload,
			}},
			Outgoing: []service.OutgoingMessage{{
				Key:  "agent-completed-reply/" + run.RunID,
				Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion,
				To: run.ReplyTo, CorrelationID: run.CorrelationID, Payload: resultPayload,
			}},
		}, nil
	default:
		return service.Decision{}, fmt.Errorf("unknown agent artifact operation %q", prepared.Operation)
	}
}

func (s *agentService) handleArtifactFailed(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.address {
		return service.Decision{}, fmt.Errorf("agent artifact failure came from unexpected address %q", message.From)
	}
	var failed artifactFailed
	if err := json.Unmarshal(message.Payload, &failed); err != nil {
		return service.Decision{}, err
	}
	run, found := state.Runs[failed.RunID]
	if !found || run.Phase.Terminal() {
		return service.Decision{}, nil
	}
	return s.failRun(run, errArtifactPreparation, "agent could not prepare a required artifact")
}

func (s *agentService) handleModelReply(ctx context.Context, state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.modelAddress {
		return service.Decision{}, fmt.Errorf("model reply came from unexpected address %q", message.From)
	}
	run, found := findPendingRun(state, message.CorrelationID)
	if !found || run.Phase.Terminal() {
		return service.Decision{}, nil
	}
	if run.Phase != PhaseWaitingModel {
		return service.Decision{}, nil
	}
	if s.expired(run) {
		return s.failRun(run, errDeadlineExpired, "agent run deadline has expired")
	}
	if replyHasError(message) {
		return s.failRun(run, errModelFailed, "model completion was rejected or failed")
	}
	var reply llmClient.CompletionReply
	if err := json.Unmarshal(message.Payload, &reply); err != nil || reply.RequestID != run.PendingCorrelation || reply.ArtifactKey != reply.Artifact.Key {
		return s.failRun(run, errModelFailed, "model completion response is invalid")
	}
	if err := artifact.ValidateRef(reply.Artifact); err != nil {
		return s.failRun(run, errModelFailed, "model completion artifact is invalid")
	}
	data, err := readArtifact(ctx, s.artifacts, reply.Artifact, s.spec.MaxArtifactBytes)
	if err != nil {
		return s.failRun(run, errModelFailed, "model completion artifact cannot be read")
	}
	action, actionErr := parseModelAction(data)
	if actionErr != nil {
		return s.rejectModelResponse(run, reply.Artifact, actionErr.Error())
	}
	turn := run.PendingTurn
	switch action.Action {
	case "finish":
		payload, _ := json.Marshal(outputRequestedPayload{RunID: run.RunID, Turn: turn, ResponseRef: reply.Artifact, Action: action})
		decision := service.Decision{Events: []service.NewEvent{{
			Key:  "agent-output-requested/" + run.RunID + "/" + strconv.Itoa(turn),
			Type: outputRequestedEvent, Version: ProtocolVersion, Payload: payload,
		}}}
		run.Phase, run.PendingCorrelation = PhaseFinalizing, ""
		if turn > 0 && turn <= len(run.Turns) {
			run.Turns[turn-1].ModelResponseRef = cloneArtifact(&reply.Artifact)
			selected := action.clone()
			run.Turns[turn-1].Action = &selected
		}
		output, err := s.scheduleOutput(run, turn, reply.Artifact)
		return mergeDecision(decision, output), err
	case "capability":
		resolved, exists := findCapability(run.Capabilities, action.CapabilityRef, action.CapabilityVersion)
		if !exists {
			return s.rejectModelResponse(run, reply.Artifact, "requested capability is not available in this run")
		}
		if len(action.Arguments) > s.spec.MaxInlineCapabilityResultBytes {
			return s.rejectModelResponse(run, reply.Artifact, "capability arguments exceed the inline limit")
		}
		callID := run.RunID + "/capability/" + strconv.Itoa(turn)
		invokePayload, err := json.Marshal(capability.InvokeRequest{
			CallID: callID, CapabilityRef: resolved.Descriptor.Ref,
			CapabilityVersion:  resolved.Descriptor.Version,
			DescriptorRevision: resolved.Descriptor.DescriptorRevision,
			Arguments:          contract.CloneRaw(action.Arguments),
		})
		if err != nil {
			return service.Decision{}, err
		}
		eventPayload, _ := json.Marshal(capabilityRequestedPayload{
			RunID: run.RunID, Turn: turn, ResponseRef: reply.Artifact, Action: action, CallID: callID,
		})
		return service.Decision{
			Events: []service.NewEvent{{
				Key:  "agent-capability-requested/" + callID,
				Type: capabilityRequestedEvent, Version: ProtocolVersion, Payload: eventPayload,
			}},
			Outgoing: []service.OutgoingMessage{{
				Key:  "agent-capability-command/" + callID,
				Kind: contract.MessageCommand, Type: capability.InvokeMessageType, Version: capability.ProtocolVersion,
				To: s.capabilityAddress, ReplyTo: s.address, CorrelationID: callID,
				Deadline: cloneTime(run.Deadline), Payload: invokePayload,
			}},
		}, nil
	default:
		return s.rejectModelResponse(run, reply.Artifact, "model action is unsupported")
	}
}

func (s *agentService) observeCapabilityResult(run RunState, message contract.Message) (service.Decision, error) {
	turn := run.PendingTurn
	if turn <= 0 || turn > len(run.Turns) {
		return service.Decision{}, fmt.Errorf("agent run %q has no pending capability turn", run.RunID)
	}
	outcome := CapabilityOutcome{CallID: run.PendingCallID}
	if replyHasError(message) {
		var replyError service.ReplyError
		_ = json.Unmarshal(message.Payload, &replyError)
		outcome.Phase = capability.PhaseFailed
		outcome.ErrorCode = strings.TrimSpace(replyError.Code)
		outcome.ErrorMessage = strings.TrimSpace(replyError.Message)
		if outcome.ErrorCode == "" {
			outcome.ErrorCode = "capability_rejected"
		}
		if outcome.ErrorMessage == "" {
			outcome.ErrorMessage = "capability invocation was rejected"
		}
	} else {
		var result capability.Result
		if err := json.Unmarshal(message.Payload, &result); err != nil || result.CallID != run.PendingCallID || !result.Phase.Terminal() {
			return s.failRun(run, errCapabilityResult, "capability result is invalid")
		}
		if len(result.Result) > s.spec.MaxInlineCapabilityResultBytes && result.ResultRef == nil {
			return s.failRun(run, errCapabilityResult, "capability result exceeds the inline limit and has no usable artifact")
		}
		if result.ResultRef != nil {
			if err := artifact.ValidateRef(*result.ResultRef); err != nil {
				return s.failRun(run, errCapabilityResult, "capability result artifact is invalid")
			}
		}
		outcome = CapabilityOutcome{
			CallID: result.CallID, Phase: result.Phase, ResultRef: cloneArtifact(result.ResultRef),
			Result: contract.CloneRaw(result.Result), ErrorCode: result.ErrorCode, ErrorMessage: result.ErrorMessage,
		}
		if len(outcome.Result) > s.spec.MaxInlineCapabilityResultBytes {
			outcome.Result = nil
		}
	}
	payload, _ := json.Marshal(capabilityObservedPayload{RunID: run.RunID, Turn: turn, Outcome: outcome})
	decision := service.Decision{Events: []service.NewEvent{{
		Key:  "agent-capability-observed/" + run.PendingCallID,
		Type: capabilityResultObservedEvent, Version: ProtocolVersion, Payload: payload,
	}}}
	run.Turns[turn-1].Capability = pointerOutcome(outcome)
	run.PendingCallID, run.PendingCorrelation = "", ""
	if turn >= s.spec.MaxTurns {
		failed, err := s.failRun(run, errTurnLimit, "agent reached the maximum number of model turns")
		return mergeDecision(decision, failed), err
	}
	prompt, err := s.schedulePrompt(run, turn+1)
	return mergeDecision(decision, prompt), err
}

func (s *agentService) rejectModelResponse(run RunState, response contract.ArtifactRef, feedback string) (service.Decision, error) {
	turn := run.PendingTurn
	if turn <= 0 || turn > len(run.Turns) {
		return service.Decision{}, fmt.Errorf("agent run %q has no pending model turn", run.RunID)
	}
	feedback = strings.TrimSpace(feedback)
	if feedback == "" {
		feedback = "model response did not follow the required protocol"
	}
	payload, _ := json.Marshal(modelRejectedPayload{RunID: run.RunID, Turn: turn, ResponseRef: response, Feedback: feedback})
	decision := service.Decision{Events: []service.NewEvent{{
		Key:  "agent-model-rejected/" + run.RunID + "/" + strconv.Itoa(turn),
		Type: modelRejectedEvent, Version: ProtocolVersion, Payload: payload,
	}}}
	run.Turns[turn-1].ModelResponseRef = cloneArtifact(&response)
	run.Turns[turn-1].Feedback = feedback
	run.PendingCorrelation = ""
	if turn >= s.spec.MaxTurns {
		failed, err := s.failRun(run, errModelProtocol, "model did not produce a valid action before the turn limit")
		return mergeDecision(decision, failed), err
	}
	prompt, err := s.schedulePrompt(run, turn+1)
	return mergeDecision(decision, prompt), err
}

func (s *agentService) schedulePrompt(run RunState, turn int) (service.Decision, error) {
	if s.expired(run) {
		return s.failRun(run, errDeadlineExpired, "agent run deadline has expired")
	}
	input := promptPreparation{
		Operation: preparePrompt, AgentAddress: s.address, RunID: run.RunID, Turn: turn,
		UserID: run.UserID, GoalID: run.GoalID, CorrelationID: run.CorrelationID,
		Spec: s.spec, Input: run.Input, InputArtifact: cloneArtifact(run.InputArtifact),
		Capabilities: cloneCapabilities(run.Capabilities), History: promptHistory(run.Turns),
	}
	effectPayload, err := json.Marshal(input)
	if err != nil {
		return service.Decision{}, err
	}
	if len(effectPayload) > maxInlineAgentEffectBytes {
		return s.failRun(run, errArtifactPreparation, "agent prompt description is too large to persist")
	}
	eventPayload, _ := json.Marshal(promptRequestedPayload{RunID: run.RunID, Turn: turn})
	key := run.RunID + "/" + strconv.Itoa(turn)
	return service.Decision{
		Events: []service.NewEvent{{
			Key:  "agent-prompt-requested/" + key,
			Type: promptRequestedEvent, Version: ProtocolVersion, Payload: eventPayload,
		}},
		Effects: []service.PlannedEffect{{
			Key:  "agent-prompt-effect/" + key,
			Type: PrepareArtifactEffectType, Version: ProtocolVersion,
			ExecutorRef: PrepareArtifactExecutorRef, IdempotencyKey: "agent-prompt/" + key,
			Payload: effectPayload, Deadline: cloneTime(run.Deadline),
		}},
	}, nil
}

func (s *agentService) scheduleOutput(run RunState, turn int, source contract.ArtifactRef) (service.Decision, error) {
	input := promptPreparation{
		Operation: prepareOutput, AgentAddress: s.address, RunID: run.RunID, Turn: turn,
		UserID: run.UserID, GoalID: run.GoalID, CorrelationID: run.CorrelationID,
		Spec: s.spec, Source: cloneArtifact(&source),
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return service.Decision{}, err
	}
	if len(payload) > maxInlineAgentEffectBytes {
		return s.failRun(run, errArtifactPreparation, "agent output description is too large to persist")
	}
	key := run.RunID + "/" + strconv.Itoa(turn)
	return service.Decision{Effects: []service.PlannedEffect{{
		Key:  "agent-output-effect/" + key,
		Type: PrepareArtifactEffectType, Version: ProtocolVersion,
		ExecutorRef: PrepareArtifactExecutorRef, IdempotencyKey: "agent-output/" + key,
		Payload: payload, Deadline: cloneTime(run.Deadline),
	}}}, nil
}

func (s *agentService) handleCancel(state aggregateState, message contract.Message) (service.Decision, error) {
	var input CancelRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil || strings.TrimSpace(input.RunID) == "" {
		if message.ReplyTo == "" {
			return service.Decision{}, fmt.Errorf("agent cancel requires a valid run id")
		}
		return rejection(errInvalidRequest, "run id is required"), nil
	}
	run, found := state.Runs[strings.TrimSpace(input.RunID)]
	if !found {
		if message.ReplyTo == "" {
			return service.Decision{}, fmt.Errorf("agent run %q was not found", input.RunID)
		}
		return rejection(errRunNotFound, "agent run was not found"), nil
	}
	if !authorized(message, run) {
		if message.ReplyTo == "" {
			return service.Decision{}, fmt.Errorf("agent run %q belongs to another caller", run.RunID)
		}
		return rejection(errAccessDenied, "agent run belongs to another caller"), nil
	}
	if run.Phase.Terminal() {
		if message.ReplyTo == "" {
			return service.Decision{}, nil
		}
		return statusReply(run, "agent-cancel-terminal/"+run.RunID), nil
	}
	completedAt := s.now()
	payload, _ := json.Marshal(runTerminalPayload{
		RunID: run.RunID, ErrorCode: errCancelled, ErrorMessage: "agent run was cancelled", CompletedAt: completedAt,
	})
	cancelled := run.clone()
	cancelled.Phase, cancelled.ErrorCode, cancelled.ErrorMessage, cancelled.CompletedAt = PhaseCancelled, errCancelled, "agent run was cancelled", &completedAt
	resultPayload, _ := json.Marshal(executeResult(cancelled))
	decision := service.Decision{
		Events: []service.NewEvent{{
			Key: "agent-run-cancelled/" + run.RunID, Type: runCancelledEvent,
			Version: ProtocolVersion, Payload: payload,
		}},
		Outgoing: []service.OutgoingMessage{{
			Key:  "agent-cancelled-reply/" + run.RunID,
			Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion,
			To: run.ReplyTo, CorrelationID: run.CorrelationID, Payload: resultPayload,
		}},
	}
	if run.Phase == PhaseWaitingCapability && run.PendingCallID != "" {
		cancelPayload, _ := json.Marshal(capability.CancelRequest{CallID: run.PendingCallID, ReasonCode: errCancelled})
		decision.Outgoing = append(decision.Outgoing, service.OutgoingMessage{
			Key:  "agent-capability-cancel/" + run.PendingCallID,
			Kind: contract.MessageCommand, Type: capability.CancelMessageType, Version: capability.ProtocolVersion,
			To: s.capabilityAddress, ReplyTo: s.address, CorrelationID: run.PendingCallID + "/cancel", Payload: cancelPayload,
		})
	}
	if message.ReplyTo != "" {
		decision.Reply = &service.Reply{
			Key: "agent-cancel-status/" + run.RunID, Type: StatusMessageType,
			Version: ProtocolVersion, Payload: resultPayload,
		}
	}
	return decision, nil
}

func (s *agentService) handleGet(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, fmt.Errorf("agent get requires reply_to")
	}
	var input GetRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil || strings.TrimSpace(input.RunID) == "" {
		return queryRejection(errInvalidRequest, "run id is required"), nil
	}
	run, found := state.Runs[strings.TrimSpace(input.RunID)]
	if !found {
		return queryRejection(errRunNotFound, "agent run was not found"), nil
	}
	if !authorized(message, run) {
		return queryRejection(errAccessDenied, "agent run belongs to another caller"), nil
	}
	value := run.clone()
	payload, err := json.Marshal(GetResponse{Run: &value})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Reply: &service.Reply{
		Key: "agent-get/" + run.RunID, Type: StatusMessageType, Version: ProtocolVersion, Payload: payload,
	}}, nil
}

func (s *agentService) failRun(run RunState, code, safeMessage string) (service.Decision, error) {
	if run.Phase.Terminal() {
		return service.Decision{}, nil
	}
	completedAt := s.now()
	payload, err := json.Marshal(runTerminalPayload{
		RunID: run.RunID, ErrorCode: code, ErrorMessage: safeMessage, CompletedAt: completedAt,
	})
	if err != nil {
		return service.Decision{}, err
	}
	failed := run.clone()
	failed.Phase, failed.ErrorCode, failed.ErrorMessage, failed.CompletedAt = PhaseFailed, code, safeMessage, &completedAt
	resultPayload, err := json.Marshal(executeResult(failed))
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{{
			Key:  "agent-run-failed/" + run.RunID + "/" + code,
			Type: runFailedEvent, Version: ProtocolVersion, Payload: payload,
		}},
		Outgoing: []service.OutgoingMessage{{
			Key:  "agent-failed-reply/" + run.RunID + "/" + code,
			Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion,
			To: run.ReplyTo, CorrelationID: run.CorrelationID, Payload: resultPayload,
		}},
	}, nil
}

func rejection(code, message string) service.Decision {
	return service.Decision{Reply: &service.Reply{
		Key: "agent-rejected/" + code, Type: CompletedMessageType, Version: ProtocolVersion,
		Error: &service.ReplyError{Code: code, Message: message},
	}}
}

func queryRejection(code, message string) service.Decision {
	return service.Decision{Reply: &service.Reply{
		Key: "agent-query-rejected/" + code, Type: StatusMessageType, Version: ProtocolVersion,
		Error: &service.ReplyError{Code: code, Message: message},
	}}
}

func resultReply(run RunState, key string) service.Decision {
	payload, _ := json.Marshal(executeResult(run))
	return service.Decision{Reply: &service.Reply{Key: key, Type: CompletedMessageType, Version: ProtocolVersion, Payload: payload}}
}

func statusReply(run RunState, key string) service.Decision {
	payload, _ := json.Marshal(executeResult(run))
	return service.Decision{Reply: &service.Reply{Key: key, Type: StatusMessageType, Version: ProtocolVersion, Payload: payload}}
}

func executeResult(run RunState) ExecuteResult {
	return ExecuteResult{
		RunID: run.RunID, Phase: run.Phase, Output: cloneArtifact(run.Output),
		ErrorCode: run.ErrorCode, ErrorMessage: run.ErrorMessage, Turns: len(run.Turns),
	}
}

func findPendingRun(state aggregateState, correlationID string) (RunState, bool) {
	if correlationID == "" {
		return RunState{}, false
	}
	for _, run := range state.Runs {
		if run.PendingCorrelation == correlationID {
			return run.clone(), true
		}
	}
	return RunState{}, false
}

func authorized(message contract.Message, run RunState) bool {
	if run.Caller != "" && message.From == run.Caller {
		return true
	}
	return run.UserID != "" && message.UserID == run.UserID
}

func replyHasError(message contract.Message) bool {
	return message.Metadata[contract.MetadataReplyError] == "true"
}

func (s *agentService) expired(run RunState) bool {
	return run.Deadline != nil && !run.Deadline.After(s.now())
}

func (s *agentService) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock.Now().UTC()
}

func runFingerprint(message contract.Message, input ExecuteRequest) string {
	payload, _ := json.Marshal(struct {
		Caller        contract.ServiceAddress `json:"caller"`
		ReplyTo       contract.ServiceAddress `json:"reply_to"`
		UserID        string                  `json:"user_id"`
		Input         string                  `json:"input"`
		InputArtifact *contract.ArtifactRef   `json:"input_artifact,omitempty"`
	}{message.From, message.ReplyTo, message.UserID, input.Input, input.InputArtifact})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func mergeDecision(left, right service.Decision) service.Decision {
	left.Events = append(left.Events, right.Events...)
	left.Outgoing = append(left.Outgoing, right.Outgoing...)
	left.Effects = append(left.Effects, right.Effects...)
	if right.Reply != nil {
		left.Reply = right.Reply
	}
	return left
}

func promptHistory(values []TurnRecord) []TurnRecord {
	cloned := make([]TurnRecord, 0, len(values))
	for _, value := range values {
		cloned = append(cloned, TurnRecord{
			Number: value.Number, ModelResponseRef: cloneArtifact(value.ModelResponseRef),
			Capability: pointerOptionalOutcome(value.Capability), Feedback: value.Feedback,
		})
	}
	return cloned
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func pointerOutcome(value CapabilityOutcome) *CapabilityOutcome {
	cloned := value.clone()
	return &cloned
}

func pointerOptionalOutcome(value *CapabilityOutcome) *CapabilityOutcome {
	if value == nil {
		return nil
	}
	cloned := value.clone()
	return &cloned
}

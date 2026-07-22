package capability

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"agent/services/approval"
)

type approvalFact struct {
	Decision  string
	DecidedBy string
	DecidedAt *time.Time
}

func (s *capabilityService) handleApprovalResult(ctx context.Context, state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.approvalAddress {
		return service.Decision{}, fmt.Errorf("approval result came from unexpected address %q", message.From)
	}
	var result approval.Result
	if err := json.Unmarshal(message.Payload, &result); err != nil {
		return service.Decision{}, fmt.Errorf("decode approval result: %w", err)
	}
	call, found := state.Calls[result.CallID]
	if !found {
		if _, retained := state.Tombstones[result.CallID]; retained {
			return service.Decision{}, nil
		}
		return service.Decision{}, fmt.Errorf("approval result references unknown call %q", result.CallID)
	}
	if call.Phase.Terminal() {
		return service.Decision{}, nil
	}
	if call.Phase != PhaseWaitingApproval || result.ApprovalID != call.ApprovalID ||
		result.Requester != s.address || result.CapabilityRef != call.CapabilityRef ||
		result.CapabilityVersion != call.CapabilityVersion {
		return service.Decision{}, fmt.Errorf("approval result does not match waiting call %q", call.CallID)
	}
	fact := &approvalFact{Decision: string(result.Decision), DecidedBy: result.DecidedBy, DecidedAt: cloneTime(&result.DecidedAt)}
	if call.Deadline != nil && !call.Deadline.After(s.now()) {
		return s.terminalDecision(message, call, PhaseExpired, nil, nil,
			errCallExpired, "capability call expired before execution", false, fact)
	}
	switch message.Type {
	case approval.ResolvedEventType:
		switch result.Status {
		case approval.StatusApproved:
			descriptor, provider, found := s.catalog.Resolve(call.CapabilityRef, call.CapabilityVersion)
			if !found || descriptor.DescriptorRevision != call.DescriptorRevision {
				return s.terminalDecision(message, call, PhaseFailed, nil, nil,
					errDescriptorMismatch, "persisted capability descriptor is unavailable", false, fact)
			}
			return s.startExecution(ctx, message, call, descriptor, provider, false, fact)
		case approval.StatusDenied:
			return s.terminalDecision(message, call, PhaseDenied, nil, nil,
				errApprovalDenied, "capability approval was denied", false, fact)
		default:
			return service.Decision{}, fmt.Errorf("resolved approval has invalid status %q", result.Status)
		}
	case approval.CancelledEventType:
		if result.Status != approval.StatusCancelled {
			return service.Decision{}, fmt.Errorf("cancelled approval result has status %q", result.Status)
		}
		return s.terminalDecision(message, call, PhaseCancelled, nil, nil,
			errApprovalCancelled, "capability approval was cancelled", false, fact)
	case approval.ExpiredEventType:
		if result.Status != approval.StatusExpired {
			return service.Decision{}, fmt.Errorf("expired approval result has status %q", result.Status)
		}
		return s.terminalDecision(message, call, PhaseExpired, nil, nil,
			errApprovalExpired, "capability approval expired", false, fact)
	default:
		return service.Decision{}, fmt.Errorf("unsupported approval result %q", message.Type)
	}
}

func (s *capabilityService) handleApprovalResponse(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.approvalAddress {
		return service.Decision{}, fmt.Errorf("approval response came from unexpected address %q", message.From)
	}
	call, found := state.Calls[message.CorrelationID]
	if !found || call.Phase.Terminal() {
		return service.Decision{}, nil
	}
	if call.Phase != PhaseWaitingApproval {
		return service.Decision{}, nil
	}
	if message.Metadata[contract.MetadataReplyError] != "true" {
		return service.Decision{}, nil
	}
	var replyError service.ReplyError
	if err := json.Unmarshal(message.Payload, &replyError); err != nil {
		return service.Decision{}, fmt.Errorf("decode approval error response: %w", err)
	}
	return s.terminalDecision(message, call, PhaseFailed, nil, nil,
		errApprovalRequest, "approval request was rejected", false, nil)
}

func (s *capabilityService) startExecution(ctx context.Context, message contract.Message, call CallState, descriptor CapabilityDescriptor, provider CapabilityProvider, direct bool, approvalResult *approvalFact) (service.Decision, error) {
	if call.Deadline != nil && !call.Deadline.After(s.now()) {
		return s.terminalDecision(message, call, PhaseExpired, nil, nil,
			errCallExpired, "capability call expired before execution", direct, approvalResult)
	}
	plan, err := provider.Plan(ctx, CapabilityInvocation{
		CallID: call.CallID, Caller: call.Caller, UserID: call.UserID,
		Descriptor: descriptor.Clone(), Arguments: call.Arguments.Clone(),
		Deadline: cloneTime(call.Deadline), PlanRevision: call.PlanRevision,
	})
	if err != nil {
		return s.terminalDecision(message, call, PhaseFailed, nil, nil,
			errExecutionPlan, "capability provider could not form an execution plan", direct, approvalResult)
	}
	if clean(plan.ExecutionKey) == "" || plan.Kind != descriptor.ExecutionKind {
		return s.terminalDecision(message, call, PhaseFailed, nil, nil,
			errExecutionPlan, "capability execution plan is invalid", direct, approvalResult)
	}
	generation := uint64(1)
	executionCorrelationID := stableID("capability-execution", call.CallID, plan.ExecutionKey, "1")
	eventPayload := executionStateEventPayload{
		CallID: call.CallID, Kind: plan.Kind, ExecutionKey: plan.ExecutionKey,
		Generation: generation, ExecutionCorrelationID: executionCorrelationID,
	}
	if approvalResult != nil {
		eventPayload.ApprovalDecision = approvalResult.Decision
		eventPayload.ApprovalDecidedBy = approvalResult.DecidedBy
		eventPayload.ApprovalDecidedAt = cloneTime(approvalResult.DecidedAt)
	}
	decision := service.Decision{}
	switch plan.Kind {
	case ExecutionEffect:
		if plan.Effect == nil || plan.ServiceCommand != nil || plan.Effect.Type == "" || plan.Effect.Version <= 0 ||
			clean(plan.Effect.ExecutorRef) == "" || plan.Effect.ExecutorRef != descriptor.ExecutorRef ||
			plan.Effect.Type != descriptor.EffectType || !jsonPayloadValid(plan.Effect.Payload) {
			return s.terminalDecision(message, call, PhaseFailed, nil, nil,
				errExecutionPlan, "capability effect plan is invalid", direct, approvalResult)
		}
		deadline := earliestDeadline(call.Deadline, plan.Effect.Deadline)
		if deadline != nil && !deadline.After(s.now()) {
			return s.terminalDecision(message, call, PhaseExpired, nil, nil,
				errCallExpired, "capability effect deadline has expired", direct, approvalResult)
		}
		envelopePayload, err := json.Marshal(executionEffectEnvelope{
			CallID: call.CallID, ExecutionKey: plan.ExecutionKey,
			Generation: generation, CapabilityAddress: s.address,
			ExecutorRef: plan.Effect.ExecutorRef, Payload: contract.CloneRaw(plan.Effect.Payload),
			CorrelationID: call.CorrelationID, UserID: call.UserID,
		})
		if err != nil {
			return service.Decision{}, err
		}
		eventPayload.ExecutorRef, eventPayload.EffectType = plan.Effect.ExecutorRef, plan.Effect.Type
		decision.Effects = append(decision.Effects, service.PlannedEffect{
			Key:  "capability-effect/" + call.CallID + "/" + plan.ExecutionKey + "/1",
			Type: plan.Effect.Type, Version: plan.Effect.Version,
			ExecutorRef:    plan.Effect.ExecutorRef,
			IdempotencyKey: stableID("capability-idempotency", call.CallID, plan.ExecutionKey),
			Payload:        envelopePayload, Deadline: deadline,
		})
	case ExecutionServiceCommand:
		command := plan.ServiceCommand
		if command == nil || plan.Effect != nil || command.To == "" || command.Type == "" ||
			command.Version <= 0 || command.ReplyType == "" || command.ReplyVersion <= 0 ||
			command.Type != descriptor.CommandType || command.Version != descriptor.CommandVersion ||
			command.ReplyType != descriptor.ReplyType || command.ReplyVersion != descriptor.ReplyVersion ||
			!jsonPayloadValid(command.Payload) {
			return s.terminalDecision(message, call, PhaseFailed, nil, nil,
				errExecutionPlan, "capability service-command plan is invalid", direct, approvalResult)
		}
		deadline := earliestDeadline(call.Deadline, command.Deadline)
		if deadline != nil && !deadline.After(s.now()) {
			return s.terminalDecision(message, call, PhaseExpired, nil, nil,
				errCallExpired, "capability command deadline has expired", direct, approvalResult)
		}
		eventPayload.Target, eventPayload.MessageType = command.To, command.Type
		eventPayload.MessageVersion = command.Version
		eventPayload.ReplyType, eventPayload.ReplyVersion = command.ReplyType, command.ReplyVersion
		decision.Outgoing = append(decision.Outgoing, service.OutgoingMessage{
			Key:  "capability-command/" + call.CallID + "/" + plan.ExecutionKey + "/1",
			Kind: contract.MessageCommand, Type: command.Type, Version: command.Version,
			To: command.To, ReplyTo: s.address, CorrelationID: executionCorrelationID,
			Deadline: deadline, Payload: contract.CloneRaw(command.Payload),
		})
	default:
		return s.terminalDecision(message, call, PhaseFailed, nil, nil,
			errExecutionPlan, "capability execution kind is invalid", direct, approvalResult)
	}
	encoded, err := json.Marshal(eventPayload)
	if err != nil {
		return service.Decision{}, err
	}
	decision.Events = append([]service.NewEvent{{
		Key:  "capability-execution-started/" + call.CallID + "/" + plan.ExecutionKey + "/1",
		Type: callExecutionStateEvent, Version: ProtocolVersion, Payload: encoded,
	}}, decision.Events...)
	return decision, nil
}

func (s *capabilityService) handleExecutionCompleted(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.address {
		return service.Decision{}, fmt.Errorf("capability execution result came from unexpected source %q", message.From)
	}
	var input ExecutionCompleted
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, fmt.Errorf("decode capability execution completion: %w", err)
	}
	if input.ResultRef != nil {
		if err := artifact.ValidateRef(*input.ResultRef); err != nil {
			return service.Decision{}, fmt.Errorf("execution result artifact is invalid")
		}
	}
	if !jsonPayloadValid(input.Result) {
		return service.Decision{}, fmt.Errorf("execution result payload is invalid")
	}
	call, found := state.Calls[input.CallID]
	if !found {
		if _, retained := state.Tombstones[input.CallID]; retained {
			return service.Decision{}, nil
		}
		return service.Decision{}, fmt.Errorf("execution result references unknown call %q", input.CallID)
	}
	if call.Phase.Terminal() {
		return lateOutcomeDecision(call, input.OutcomeID, "succeeded", input.ResultRef, input.Result, "")
	}
	if err := validateEffectOutcome(call, input.ExecutionKey, input.ExecutorRef, input.Generation, input.OutcomeID); err != nil {
		return service.Decision{}, err
	}
	return s.terminalDecision(message, call, PhaseSucceeded, input.ResultRef, input.Result, "", "", false, nil)
}

func (s *capabilityService) handleExecutionFailed(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.address {
		return service.Decision{}, fmt.Errorf("capability execution failure came from unexpected source %q", message.From)
	}
	var input ExecutionFailed
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, fmt.Errorf("decode capability execution failure: %w", err)
	}
	call, found := state.Calls[input.CallID]
	if !found {
		if _, retained := state.Tombstones[input.CallID]; retained {
			return service.Decision{}, nil
		}
		return service.Decision{}, fmt.Errorf("execution failure references unknown call %q", input.CallID)
	}
	if call.Phase.Terminal() {
		return lateOutcomeDecision(call, input.OutcomeID, "failed", nil, nil, input.ErrorCode)
	}
	if err := validateEffectOutcome(call, input.ExecutionKey, input.ExecutorRef, input.Generation, input.OutcomeID); err != nil {
		return service.Decision{}, err
	}
	code := clean(input.ErrorCode)
	if code == "" {
		code = errExecutionFailed
	}
	messageText := clean(input.ErrorMessage)
	if messageText == "" {
		messageText = "capability execution failed"
	}
	return s.terminalDecision(message, call, PhaseFailed, nil, nil, code, messageText, false, nil)
}

func (s *capabilityService) handleServiceReply(state aggregateState, message contract.Message) (service.Decision, error) {
	var call CallState
	found := false
	for _, candidate := range state.Calls {
		if candidate.ExecutionCorrelationID == message.CorrelationID {
			call, found = candidate, true
			break
		}
	}
	if !found {
		return service.Decision{}, fmt.Errorf("service reply has unknown execution correlation %q", message.CorrelationID)
	}
	if call.Phase.Terminal() {
		kind, code := "succeeded", ""
		if message.Metadata[contract.MetadataReplyError] == "true" {
			kind, code = "failed", errExecutionFailed
		}
		return lateOutcomeDecision(call, message.ID, kind, nil, message.Payload, code)
	}
	if call.Phase != PhaseWaitingExecution || call.ExecutionKind != ExecutionServiceCommand ||
		message.From != call.ExecutionTarget || message.Type != call.ExecutionReplyType ||
		message.Version != call.ExecutionReplyVersion {
		return service.Decision{}, fmt.Errorf("service reply does not match capability call %q", call.CallID)
	}
	if message.Metadata[contract.MetadataReplyError] == "true" {
		var replyError service.ReplyError
		if err := json.Unmarshal(message.Payload, &replyError); err != nil {
			return service.Decision{}, fmt.Errorf("decode downstream reply error: %w", err)
		}
		code := clean(replyError.Code)
		if code == "" {
			code = errExecutionFailed
		}
		return s.terminalDecision(message, call, PhaseFailed, nil, nil,
			code, "downstream capability service rejected the request", false, nil)
	}
	if !jsonPayloadValid(message.Payload) {
		return service.Decision{}, fmt.Errorf("downstream capability reply payload is invalid")
	}
	return s.terminalDecision(message, call, PhaseSucceeded, nil, message.Payload, "", "", false, nil)
}

func (s *capabilityService) terminalDecision(input contract.Message, call CallState, phase CallPhase, resultRef *contract.ArtifactRef, result json.RawMessage, code, safeMessage string, direct bool, approvalResult *approvalFact) (service.Decision, error) {
	completedAt := s.now()
	payload := terminalStateEventPayload{
		CallID: call.CallID, Phase: phase, ResultRef: resultRef,
		Result: contract.CloneRaw(result), ErrorCode: code, ErrorMessage: safeMessage,
		CompletedAt: completedAt,
	}
	if approvalResult != nil {
		payload.ApprovalDecision = approvalResult.Decision
		payload.ApprovalDecidedBy = approvalResult.DecidedBy
		payload.ApprovalDecidedAt = cloneTime(approvalResult.DecidedAt)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return service.Decision{}, err
	}
	call.Phase, call.Result = phase, contract.CloneRaw(result)
	if resultRef != nil {
		value := *resultRef
		call.ResultRef = &value
	}
	call.ErrorCode, call.ErrorMessage, call.CompletedAt = code, safeMessage, &completedAt
	if approvalResult != nil {
		call.ApprovalDecision, call.ApprovalDecidedBy = approvalResult.Decision, approvalResult.DecidedBy
		call.ApprovalDecidedAt = cloneTime(approvalResult.DecidedAt)
	}
	decision := service.Decision{Events: []service.NewEvent{{
		Key:  "capability-call-terminal/" + call.CallID + "/" + string(phase),
		Type: callTerminalStateEvent, Version: ProtocolVersion, Payload: encoded,
	}}}
	resultPayload, err := json.Marshal(resultFromCall(call))
	if err != nil {
		return service.Decision{}, err
	}
	if direct {
		if input.ReplyTo == "" {
			return service.Decision{}, fmt.Errorf("direct capability result requires reply_to")
		}
		decision.Reply = &service.Reply{
			Key:  "capability-result-direct/" + call.CallID + "/" + string(phase),
			Type: ResultMessageType, Version: ProtocolVersion, Payload: resultPayload,
		}
	} else {
		decision.Outgoing = append(decision.Outgoing, service.OutgoingMessage{
			Key:  "capability-result-delayed/" + call.CallID + "/" + string(phase),
			Kind: contract.MessageReply, Type: ResultMessageType, Version: ProtocolVersion,
			To: call.ReplyTo, CorrelationID: call.CorrelationID, Payload: resultPayload,
		})
	}
	return decision, nil
}

func currentResultDecision(message contract.Message, call CallState, key string) (service.Decision, error) {
	payload, err := json.Marshal(resultFromCall(call))
	if err != nil {
		return service.Decision{}, err
	}
	return replyPayload(message, key, payload)
}

func tombstoneResultDecision(message contract.Message, tombstone CallTombstone, key string) (service.Decision, error) {
	result := Result{
		CallID: tombstone.CallID, CapabilityRef: tombstone.CapabilityRef,
		CapabilityVersion: tombstone.CapabilityVersion, Phase: tombstone.Phase,
		ResultRef: tombstone.ResultRef, Result: contract.CloneRaw(tombstone.Result),
		ErrorCode: tombstone.ErrorCode, ErrorMessage: tombstone.ErrorMessage,
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return service.Decision{}, err
	}
	return replyPayload(message, key, payload)
}

func resultFromCall(call CallState) Result {
	return Result{
		CallID: call.CallID, CapabilityRef: call.CapabilityRef,
		CapabilityVersion: call.CapabilityVersion, Phase: call.Phase,
		ResultRef: call.ResultRef, Result: contract.CloneRaw(call.Result),
		ErrorCode: call.ErrorCode, ErrorMessage: call.ErrorMessage,
	}
}

func replyPayload(message contract.Message, key string, payload json.RawMessage) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, fmt.Errorf("capability response requires reply_to")
	}
	return service.Decision{Reply: &service.Reply{
		Key: key, Type: ResultMessageType, Version: ProtocolVersion, Payload: contract.CloneRaw(payload),
	}}, nil
}

func rejection(message contract.Message, code, safeMessage string) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, fmt.Errorf("%s: %s", code, safeMessage)
	}
	return service.Decision{Reply: &service.Reply{
		Key: "capability-rejected/" + code, Type: ResultMessageType, Version: ProtocolVersion,
		Error: &service.ReplyError{Code: code, Message: safeMessage},
	}}, nil
}

func mergeDecisions(left, right service.Decision) service.Decision {
	left.Events = append(left.Events, right.Events...)
	left.Outgoing = append(left.Outgoing, right.Outgoing...)
	left.Effects = append(left.Effects, right.Effects...)
	if right.Reply != nil {
		left.Reply = right.Reply
	}
	return left
}

func validateEffectOutcome(call CallState, executionKey, executorRef string, generation uint64, outcomeID string) error {
	if call.Phase != PhaseWaitingExecution || call.ExecutionKind != ExecutionEffect ||
		call.ExecutionKey != executionKey || call.ExecutorRef != executorRef ||
		call.ExecutionGeneration != generation || clean(outcomeID) == "" {
		return fmt.Errorf("execution outcome does not match capability call %q", call.CallID)
	}
	return nil
}

func lateOutcomeDecision(call CallState, outcomeID, kind string, resultRef *contract.ArtifactRef, result json.RawMessage, errorCode string) (service.Decision, error) {
	if clean(outcomeID) == "" || call.LateOutcomeID == outcomeID {
		return service.Decision{}, nil
	}
	payload, err := json.Marshal(lateOutcomeStateEventPayload{
		CallID: call.CallID, OutcomeID: outcomeID, Kind: kind,
		ResultRef: resultRef, Result: contract.CloneRaw(result), ErrorCode: errorCode,
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Events: []service.NewEvent{{
		Key:  "capability-late-outcome/" + call.CallID + "/" + outcomeID,
		Type: callLateOutcomeStateEvent, Version: ProtocolVersion, Payload: payload,
	}}}, nil
}

func earliestDeadline(first, second *time.Time) *time.Time {
	if first == nil {
		return cloneTime(second)
	}
	if second == nil {
		return cloneTime(first)
	}
	if first.Before(*second) {
		return cloneTime(first)
	}
	return cloneTime(second)
}

func jsonPayloadValid(value json.RawMessage) bool { return len(value) == 0 || json.Valid(value) }

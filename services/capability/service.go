package capability

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"agent/services/approval"
)

const (
	errInvalidRequest      = "capability_invalid_request"
	errNotFound            = "capability_not_found"
	errVersionMismatch     = "capability_version_mismatch"
	errDescriptorMismatch  = "capability_descriptor_mismatch"
	errArgumentsInvalid    = "capability_arguments_invalid"
	errAccessDenied        = "capability_access_denied"
	errCallConflict        = "capability_call_conflict"
	errAuthorizationDenied = "capability_authorization_denied"
	errApprovalDenied      = "capability_approval_denied"
	errApprovalCancelled   = "capability_approval_cancelled"
	errApprovalExpired     = "capability_approval_expired"
	errApprovalRequest     = "capability_approval_request_failed"
	errExecutionPlan       = "capability_execution_plan_invalid"
	errExecutionFailed     = "capability_execution_failed"
	errCallCancelled       = "capability_cancelled"
	errCallExpired         = "capability_expired"
)

type capabilityService struct {
	address           contract.ServiceAddress
	approvalAddress   contract.ServiceAddress
	schedulerAddress  contract.ServiceAddress
	catalog           *Catalog
	evaluator         AuthorizationEvaluator
	validator         ArgumentValidator
	visibility        VisibilityEvaluator
	clock             contract.Clock
	terminalRetention time.Duration
	idempotencyWindow time.Duration
}

func (s *capabilityService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: Component, StateSchema: StateSchema}
}

func (*capabilityService) InitialState(context.Context, service.Init) (service.State, error) {
	return encodeState(initialAggregateState())
}

func (s *capabilityService) Handle(ctx context.Context, raw service.State, message contract.Message) (service.Decision, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.Decision{}, err
	}
	if message.Version != ProtocolVersion {
		return service.Decision{}, fmt.Errorf("unsupported capability message version %d", message.Version)
	}
	switch {
	case message.Kind == contract.MessageCommand && message.Type == InvokeMessageType:
		return s.handleInvoke(ctx, state, message)
	case message.Kind == contract.MessageCommand && message.Type == CancelMessageType:
		return s.handleCancel(state, message)
	case message.Kind == contract.MessageCommand && message.Type == PruneMessageType:
		return s.handlePrune(state, message)
	case message.Kind == contract.MessageQuery && message.Type == GetMessageType:
		return s.handleGet(state, message)
	case message.Kind == contract.MessageQuery && message.Type == ListMessageType:
		return s.handleList(message)
	case message.Kind == contract.MessageEvent && (message.Type == approval.ResolvedEventType || message.Type == approval.CancelledEventType || message.Type == approval.ExpiredEventType):
		return s.handleApprovalResult(ctx, state, message)
	case message.Kind == contract.MessageReply && message.Type == approval.ResponseMessageType:
		return s.handleApprovalResponse(state, message)
	case message.Kind == contract.MessageEvent && message.Type == ExecutionCompletedMessageType:
		return s.handleExecutionCompleted(state, message)
	case message.Kind == contract.MessageEvent && message.Type == ExecutionFailedMessageType:
		return s.handleExecutionFailed(state, message)
	case message.Kind == contract.MessageReply:
		return s.handleServiceReply(state, message)
	default:
		return service.Decision{}, fmt.Errorf("unsupported capability message %s %q v%d", message.Kind, message.Type, message.Version)
	}
}

func (s *capabilityService) handleInvoke(ctx context.Context, state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From == "" || message.ReplyTo == "" {
		return rejection(message, errInvalidRequest, "capability invocation requires caller and reply_to")
	}
	if message.Deadline != nil && !message.Deadline.After(s.now()) {
		return rejection(message, errCallExpired, "capability invocation deadline has expired")
	}
	var input InvokeRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return rejection(message, errInvalidRequest, "capability invocation payload is invalid")
	}
	input.CallID, input.CapabilityRef = clean(input.CallID), clean(input.CapabilityRef)
	input.CapabilityVersion, input.DescriptorRevision = clean(input.CapabilityVersion), clean(input.DescriptorRevision)
	if input.CallID == "" || input.CapabilityRef == "" || input.CapabilityVersion == "" {
		return rejection(message, errInvalidRequest, "call id, capability ref, and capability version are required")
	}
	descriptor, provider, found := s.catalog.Resolve(input.CapabilityRef, input.CapabilityVersion)
	if !found {
		if s.hasCapabilityRef(input.CapabilityRef) {
			return rejection(message, errVersionMismatch, "capability version is not available")
		}
		return rejection(message, errNotFound, "capability is not available")
	}
	if input.DescriptorRevision != "" && input.DescriptorRevision != descriptor.DescriptorRevision {
		return rejection(message, errDescriptorMismatch, "capability descriptor revision does not match")
	}
	arguments, err := normalizeArguments(input)
	if err != nil {
		return rejection(message, errArgumentsInvalid, err.Error())
	}
	if !descriptor.InputSchema.Empty() {
		if s.validator == nil {
			return rejection(message, errArgumentsInvalid, "capability argument schema validator is unavailable")
		}
		if err := s.validator.Validate(ctx, descriptor.InputSchema, arguments.Clone()); err != nil {
			return rejection(message, errArgumentsInvalid, "capability arguments do not satisfy the input schema")
		}
	}
	fingerprint := invocationFingerprint(message.From, message.ReplyTo, message.UserID, descriptor, arguments, message.Deadline)
	if existing, exists := state.Calls[input.CallID]; exists {
		if existing.IdentityFingerprint != fingerprint {
			return rejection(message, errCallConflict, "call id is already bound to another invocation")
		}
		return currentResultDecision(message, existing, "capability-invoke-idempotent/"+input.CallID)
	}
	if tombstone, exists := state.Tombstones[input.CallID]; exists {
		if tombstone.IdentityFingerprint != fingerprint {
			return rejection(message, errCallConflict, "call id is retained for another invocation")
		}
		return tombstoneResultDecision(message, tombstone, "capability-tombstone-idempotent/"+input.CallID)
	}
	now := s.now()
	correlationID := message.CorrelationID
	if correlationID == "" {
		correlationID = message.ID
	}
	call := CallState{
		CallID: input.CallID, InvocationMessageID: message.ID,
		Caller: message.From, ReplyTo: message.ReplyTo, UserID: message.UserID,
		CorrelationID: correlationID, PlanRevision: message.PlanRevision,
		CapabilityRef: descriptor.Ref, CapabilityVersion: descriptor.Version,
		DescriptorRevision: descriptor.DescriptorRevision, Phase: PhaseReceived,
		Arguments: arguments.Clone(), IdentityFingerprint: fingerprint,
		Deadline: cloneTime(message.Deadline), ReceivedAt: now,
	}
	receivedPayload, err := json.Marshal(call)
	if err != nil {
		return service.Decision{}, err
	}
	decision := service.Decision{Events: []service.NewEvent{{
		Key:  "capability-call-received/" + call.CallID,
		Type: callReceivedStateEvent, Version: ProtocolVersion, Payload: receivedPayload,
	}}}
	authorization, err := s.evaluator.Evaluate(AuthorizationInput{
		RuntimeID: message.RuntimeID, PlanRevision: message.PlanRevision,
		Caller: message.From, UserID: message.UserID, Descriptor: descriptor.Clone(),
		Arguments: arguments.Clone(), Deadline: cloneTime(message.Deadline),
	})
	if err != nil {
		return service.Decision{}, fmt.Errorf("evaluate capability authorization: %w", err)
	}
	if err := validateAuthorization(authorization); err != nil {
		return service.Decision{}, err
	}
	call.AuthorizationDecision = authorization.Decision
	call.AuthorizationRule, call.AuthorizationReason = authorization.RuleRef, authorization.ReasonCode
	call.RiskSummary = authorization.RiskSummary
	authPayload := authorizationStateEventPayload{
		CallID: call.CallID, Decision: authorization.Decision,
		RuleRef: authorization.RuleRef, ReasonCode: authorization.ReasonCode,
		RiskSummary: authorization.RiskSummary,
	}
	switch authorization.Decision {
	case AuthorizationDeny:
		authPayload.Phase = PhaseReceived
		encoded, err := json.Marshal(authPayload)
		if err != nil {
			return service.Decision{}, err
		}
		decision.Events = append(decision.Events, service.NewEvent{
			Key:  "capability-authorization-denied/" + call.CallID,
			Type: callAuthorizationStateEvent, Version: ProtocolVersion, Payload: encoded,
		})
		terminal, err := s.terminalDecision(message, call, PhaseDenied, nil, nil,
			errAuthorizationDenied, "capability authorization was denied", true, nil)
		return mergeDecisions(decision, terminal), err
	case AuthorizationAsk:
		if clean(message.UserID) == "" {
			return service.Decision{}, fmt.Errorf("authorization requested approval without a trusted user id")
		}
		call.Phase, call.ApprovalService = PhaseWaitingApproval, s.approvalAddress
		call.ApprovalID = stableID("approval", call.CallID, descriptor.DescriptorRevision, authorization.RuleRef)
		authPayload.Phase, authPayload.ApprovalID, authPayload.Approval = call.Phase, call.ApprovalID, s.approvalAddress
		encoded, err := json.Marshal(authPayload)
		if err != nil {
			return service.Decision{}, err
		}
		requestPayload, err := json.Marshal(approval.Request{
			ApprovalID: call.ApprovalID, CallID: call.CallID, UserID: call.UserID,
			CapabilityRef: call.CapabilityRef, CapabilityVersion: call.CapabilityVersion,
			RiskSummary: call.RiskSummary, ArgumentsDigest: call.Arguments.Digest,
			ArgumentsRef: call.Arguments.Ref, RequestedAt: now, ExpiresAt: cloneTime(call.Deadline),
		})
		if err != nil {
			return service.Decision{}, err
		}
		decision.Events = append(decision.Events, service.NewEvent{
			Key:  "capability-waiting-approval/" + call.CallID,
			Type: callAuthorizationStateEvent, Version: ProtocolVersion, Payload: encoded,
		})
		decision.Outgoing = append(decision.Outgoing, service.OutgoingMessage{
			Key:  "capability-request-approval/" + call.ApprovalID,
			Kind: contract.MessageCommand, Type: approval.RequestMessageType, Version: approval.ProtocolVersion,
			To: s.approvalAddress, ReplyTo: s.address, CorrelationID: call.CallID,
			Deadline: cloneTime(call.Deadline), Payload: requestPayload,
		})
		return decision, nil
	case AuthorizationAllow:
		call.Phase = PhaseAuthorized
		authPayload.Phase = PhaseAuthorized
		encoded, err := json.Marshal(authPayload)
		if err != nil {
			return service.Decision{}, err
		}
		decision.Events = append(decision.Events, service.NewEvent{
			Key:  "capability-authorization-allowed/" + call.CallID,
			Type: callAuthorizationStateEvent, Version: ProtocolVersion, Payload: encoded,
		})
		execution, err := s.startExecution(ctx, message, call, descriptor, provider, true, nil)
		return mergeDecisions(decision, execution), err
	default:
		return service.Decision{}, fmt.Errorf("unsupported authorization decision %q", authorization.Decision)
	}
}

func (s *capabilityService) handleCancel(state aggregateState, message contract.Message) (service.Decision, error) {
	var input CancelRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil || clean(input.CallID) == "" {
		return rejection(message, errInvalidRequest, "call id is required")
	}
	call, found := state.Calls[clean(input.CallID)]
	if !found {
		if tombstone, ok := state.Tombstones[clean(input.CallID)]; ok {
			return tombstoneResultDecision(message, tombstone, "capability-cancel-tombstone/"+tombstone.CallID)
		}
		return rejection(message, errNotFound, "capability call was not found")
	}
	if message.From != call.Caller {
		return rejection(message, errAccessDenied, "only the original caller can cancel the capability call")
	}
	if call.Phase.Terminal() {
		return currentResultDecision(message, call, "capability-cancel-idempotent/"+call.CallID)
	}
	decision, err := s.terminalDecision(message, call, PhaseCancelled, nil, nil,
		errCallCancelled, "capability call was cancelled", false, nil)
	if err != nil {
		return service.Decision{}, err
	}
	if call.Phase == PhaseWaitingApproval {
		payload, err := json.Marshal(approval.CancelRequest{
			ApprovalID: call.ApprovalID, CallID: call.CallID, ReasonCode: clean(input.ReasonCode),
		})
		if err != nil {
			return service.Decision{}, err
		}
		decision.Outgoing = append(decision.Outgoing, service.OutgoingMessage{
			Key:  "capability-cancel-approval/" + call.ApprovalID,
			Kind: contract.MessageCommand, Type: approval.CancelMessageType, Version: approval.ProtocolVersion,
			To: call.ApprovalService, ReplyTo: s.address, CorrelationID: call.CallID, Payload: payload,
		})
	}
	return decision, nil
}

func (s *capabilityService) handleGet(state aggregateState, message contract.Message) (service.Decision, error) {
	var input GetRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil || clean(input.CallID) == "" {
		return rejection(message, errInvalidRequest, "call id is required")
	}
	call, found := state.Calls[clean(input.CallID)]
	if found {
		if message.From != call.Caller && message.From != call.ReplyTo {
			return rejection(message, errAccessDenied, "capability call query is not authorized")
		}
		value := call.Clone()
		payload, err := json.Marshal(GetResponse{Call: &value})
		if err != nil {
			return service.Decision{}, err
		}
		return replyPayload(message, "capability-get/"+call.CallID, payload)
	}
	tombstone, found := state.Tombstones[clean(input.CallID)]
	if !found {
		return rejection(message, errNotFound, "capability call was not found")
	}
	if message.From != tombstone.Caller && message.From != tombstone.ReplyTo {
		return rejection(message, errAccessDenied, "capability call query is not authorized")
	}
	value := tombstone.Clone()
	payload, err := json.Marshal(GetResponse{Tombstone: &value})
	if err != nil {
		return service.Decision{}, err
	}
	return replyPayload(message, "capability-get-tombstone/"+tombstone.CallID, payload)
}

func (s *capabilityService) handleList(message contract.Message) (service.Decision, error) {
	if message.From == "" {
		return rejection(message, errAccessDenied, "capability catalog query requires a caller")
	}
	descriptors := s.catalog.Descriptors()
	visible := make([]CapabilityDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if s.visibility == nil || s.visibility.Visible(message.From, message.UserID, descriptor.Clone()) {
			visible = append(visible, descriptor.Clone())
		}
	}
	payload, err := json.Marshal(ListResponse{Descriptors: visible})
	if err != nil {
		return service.Decision{}, err
	}
	return replyPayload(message, "capability-list", payload)
}

func (s *capabilityService) handlePrune(state aggregateState, message contract.Message) (service.Decision, error) {
	if s.schedulerAddress == "" || message.From != s.schedulerAddress {
		return rejection(message, errAccessDenied, "capability retention command is not authorized")
	}
	var input PruneRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil || input.Before.IsZero() || input.Before.After(s.now()) {
		return rejection(message, errInvalidRequest, "valid retention cutoff is required")
	}
	callIDs := make([]string, 0, len(state.Calls))
	for id := range state.Calls {
		callIDs = append(callIDs, id)
	}
	sort.Strings(callIDs)
	decision := service.Decision{}
	for _, id := range callIDs {
		call := state.Calls[id]
		if !call.Phase.Terminal() || call.CompletedAt == nil || call.CompletedAt.Add(s.terminalRetention).After(input.Before) {
			continue
		}
		tombstone := CallTombstone{
			CallID: call.CallID, Caller: call.Caller, ReplyTo: call.ReplyTo,
			CapabilityRef: call.CapabilityRef, CapabilityVersion: call.CapabilityVersion,
			IdentityFingerprint: call.IdentityFingerprint, Phase: call.Phase,
			ResultRef: call.ResultRef, Result: contract.CloneRaw(call.Result),
			ErrorCode: call.ErrorCode, ErrorMessage: call.ErrorMessage,
			CompletedAt: call.CompletedAt.UTC(), ExpiresAt: call.CompletedAt.Add(s.idempotencyWindow).UTC(),
		}
		payload, err := json.Marshal(compactedStateEventPayload{CallID: id, Tombstone: tombstone})
		if err != nil {
			return service.Decision{}, err
		}
		decision.Events = append(decision.Events, service.NewEvent{
			Key: "capability-compact/" + id, Type: callCompactedStateEvent,
			Version: ProtocolVersion, Payload: payload,
		})
		if !tombstone.ExpiresAt.After(input.Before) {
			removePayload, err := json.Marshal(tombstoneRemovedStateEventPayload{CallID: id})
			if err != nil {
				return service.Decision{}, err
			}
			decision.Events = append(decision.Events, service.NewEvent{
				Key:  "capability-remove-new-tombstone/" + id,
				Type: tombstoneRemovedStateEvent, Version: ProtocolVersion, Payload: removePayload,
			})
		}
	}
	tombstoneIDs := make([]string, 0, len(state.Tombstones))
	for id := range state.Tombstones {
		tombstoneIDs = append(tombstoneIDs, id)
	}
	sort.Strings(tombstoneIDs)
	for _, id := range tombstoneIDs {
		if state.Tombstones[id].ExpiresAt.After(input.Before) {
			continue
		}
		payload, err := json.Marshal(tombstoneRemovedStateEventPayload{CallID: id})
		if err != nil {
			return service.Decision{}, err
		}
		decision.Events = append(decision.Events, service.NewEvent{
			Key: "capability-remove-tombstone/" + id, Type: tombstoneRemovedStateEvent,
			Version: ProtocolVersion, Payload: payload,
		})
	}
	if message.ReplyTo != "" {
		payload, _ := json.Marshal(map[string]int{"events": len(decision.Events)})
		decision.Reply = &service.Reply{
			Key: "capability-prune-result", Type: ResultMessageType,
			Version: ProtocolVersion, Payload: payload,
		}
	}
	return decision, nil
}

func (*capabilityService) Apply(raw service.State, event contract.StoredEvent) (service.State, error) {
	return applyStoredEvent(raw, event)
}

func normalizeArguments(input InvokeRequest) (Arguments, error) {
	if len(input.Arguments) > 0 && input.ArgumentsRef != nil {
		return Arguments{}, fmt.Errorf("inline arguments and arguments_ref are mutually exclusive")
	}
	if input.ArgumentsRef != nil {
		if err := artifact.ValidateRef(*input.ArgumentsRef); err != nil || clean(input.ArgumentsRef.Checksum) == "" {
			return Arguments{}, fmt.Errorf("arguments artifact reference and checksum are required")
		}
		value := *input.ArgumentsRef
		digest := stableDigest(value.Store, value.Key, value.Checksum)
		return Arguments{Ref: &value, Digest: digest}, nil
	}
	value := contract.CloneRaw(input.Arguments)
	if len(value) == 0 {
		value = json.RawMessage(`{}`)
	}
	if !json.Valid(value) {
		return Arguments{}, fmt.Errorf("inline arguments are not valid JSON")
	}
	return Arguments{Inline: value, Digest: stableDigest(string(value))}, nil
}

func invocationFingerprint(caller, replyTo contract.ServiceAddress, userID string, descriptor CapabilityDescriptor, arguments Arguments, deadline *time.Time) string {
	deadlineValue := ""
	if deadline != nil {
		deadlineValue = deadline.UTC().Format(time.RFC3339Nano)
	}
	return stableDigest(string(caller), string(replyTo), userID, descriptor.Ref, descriptor.Version, descriptor.DescriptorRevision, arguments.Digest, deadlineValue)
}

func validateAuthorization(value AuthorizationDecision) error {
	if !value.Decision.Valid() || clean(value.RuleRef) == "" || clean(value.ReasonCode) == "" {
		return fmt.Errorf("authorization decision, rule ref, and reason code are required")
	}
	if value.Decision == AuthorizationAsk && (clean(value.RiskSummary) == "" || value.ApprovalScope != "call") {
		return fmt.Errorf("ask authorization requires a risk summary and call approval scope")
	}
	return nil
}

func (s *capabilityService) hasCapabilityRef(ref string) bool {
	for _, descriptor := range s.catalog.Descriptors() {
		if descriptor.Ref == ref {
			return true
		}
	}
	return false
}

func stableDigest(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func stableID(kind string, parts ...string) string {
	return kind + "-" + stableDigest(append([]string{kind}, parts...)...)[:24]
}

func (s *capabilityService) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock.Now().UTC()
}

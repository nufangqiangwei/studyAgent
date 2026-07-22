package approval

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const (
	errInvalidRequest       = "approval_invalid_request"
	errAccessDenied         = "approval_access_denied"
	errNotFound             = "approval_not_found"
	errConflict             = "approval_conflict"
	errAlreadyDecided       = "approval_already_decided"
	errExpired              = "approval_expired"
	errSchedulerUnavailable = "approval_scheduler_unavailable"
)

type approvalService struct {
	address           contract.ServiceAddress
	interaction       contract.ServiceAddress
	scheduler         contract.ServiceAddress
	clock             contract.Clock
	trustedRequesters map[contract.ServiceAddress]struct{}
}

func (s *approvalService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: Component, StateSchema: StateSchema}
}

func (*approvalService) InitialState(context.Context, service.Init) (service.State, error) {
	return encodeState(initialAggregateState())
}

func (s *approvalService) Handle(_ context.Context, raw service.State, message contract.Message) (service.Decision, error) {
	state, err := decodeState(raw)
	if err != nil {
		return service.Decision{}, err
	}
	if message.Version != ProtocolVersion {
		return service.Decision{}, fmt.Errorf("unsupported approval message version %d", message.Version)
	}
	switch {
	case message.Kind == contract.MessageCommand && message.Type == RequestMessageType:
		return s.handleRequest(state, message)
	case message.Kind == contract.MessageCommand && message.Type == ResolveMessageType:
		return s.handleResolve(state, message)
	case message.Kind == contract.MessageCommand && message.Type == CancelMessageType:
		return s.handleCancel(state, message)
	case message.Kind == contract.MessageCommand && message.Type == ExpireMessageType:
		return s.handleExpire(state, message)
	case message.Kind == contract.MessageQuery && message.Type == GetMessageType:
		return s.handleGet(state, message)
	case message.Kind == contract.MessageQuery && message.Type == ListPendingMessageType:
		return s.handleListPending(state, message)
	default:
		return service.Decision{}, fmt.Errorf("unsupported approval message %s %q v%d", message.Kind, message.Type, message.Version)
	}
}

func (s *approvalService) handleRequest(state aggregateState, message contract.Message) (service.Decision, error) {
	if _, trusted := s.trustedRequesters[message.From]; !trusted {
		return rejection(message, errAccessDenied, "approval requester is not trusted")
	}
	var input Request
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return rejection(message, errInvalidRequest, "approval request payload is invalid")
	}
	input = input.clone()
	if err := validateRequest(input); err != nil {
		return rejection(message, errInvalidRequest, err.Error())
	}
	if message.UserID != input.UserID {
		return rejection(message, errAccessDenied, "approval request user does not match trusted message context")
	}
	if input.ExpiresAt != nil && !input.ExpiresAt.After(s.now()) {
		return rejection(message, errExpired, "approval request has already expired")
	}
	if existing, found := state.Approvals[input.ApprovalID]; found {
		if !sameRequest(existing, input, message.From) {
			return rejection(message, errConflict, "approval id is already bound to a different request")
		}
		return responseDecision(message, "approval-request-idempotent/"+input.ApprovalID, existing, true)
	}
	record := State{
		ApprovalID: input.ApprovalID, RequestMessageID: message.ID,
		Requester: message.From, NotifyTo: s.interaction,
		CallID: input.CallID, UserID: input.UserID,
		CapabilityRef: input.CapabilityRef, CapabilityVersion: input.CapabilityVersion,
		Status: StatusPending, RiskSummary: input.RiskSummary,
		ArgumentsDigest: input.ArgumentsDigest, ArgumentsRef: input.ArgumentsRef,
		RequestedAt: input.RequestedAt.UTC(), ExpiresAt: cloneTime(input.ExpiresAt),
	}
	eventPayload, err := json.Marshal(record)
	if err != nil {
		return service.Decision{}, err
	}
	notificationPayload, err := json.Marshal(Requested{
		ApprovalID: record.ApprovalID, CallID: record.CallID, UserID: record.UserID,
		CapabilityRef: record.CapabilityRef, CapabilityVersion: record.CapabilityVersion,
		RiskSummary: record.RiskSummary, ArgumentsDigest: record.ArgumentsDigest,
		ArgumentsRef: record.ArgumentsRef, RequestedAt: record.RequestedAt, ExpiresAt: record.ExpiresAt,
	})
	if err != nil {
		return service.Decision{}, err
	}
	decision := service.Decision{
		Events: []service.NewEvent{{
			Key:  "approval-state-requested/" + record.ApprovalID,
			Type: approvalRequestedStateEvent, Version: ProtocolVersion, Payload: eventPayload,
		}},
		Outgoing: []service.OutgoingMessage{{
			Key:  "approval-notify-requested/" + record.ApprovalID,
			Kind: contract.MessageEvent, Type: RequestedEventType, Version: ProtocolVersion,
			To: s.interaction, CorrelationID: record.CallID, Payload: notificationPayload,
		}},
	}
	if message.ReplyTo != "" {
		reply, err := responseReply("approval-request-accepted/"+record.ApprovalID, record, true)
		if err != nil {
			return service.Decision{}, err
		}
		decision.Reply = reply
	}
	return decision, nil
}

func (s *approvalService) handleResolve(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.interaction || clean(message.UserID) == "" {
		return rejection(message, errAccessDenied, "approval resolution source is not authorized")
	}
	var input ResolveRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return rejection(message, errInvalidRequest, "approval resolution payload is invalid")
	}
	if clean(input.ApprovalID) == "" || clean(input.CallID) == "" || !input.Decision.Valid() {
		return rejection(message, errInvalidRequest, "approval id, call id, and approve/deny decision are required")
	}
	record, found := state.Approvals[input.ApprovalID]
	if !found || record.CallID != input.CallID {
		return rejection(message, errNotFound, "approval was not found")
	}
	if record.UserID != message.UserID {
		return rejection(message, errAccessDenied, "user is not authorized to resolve this approval")
	}
	if record.Status.Terminal() {
		expected := statusForDecision(input.Decision)
		if record.Status == expected && record.Decision == input.Decision && record.DecidedBy == message.UserID {
			return responseDecision(message, "approval-resolution-idempotent/"+record.ApprovalID, record, true)
		}
		return rejection(message, errAlreadyDecided, "approval already has a different terminal decision")
	}
	if record.ExpiresAt != nil && !record.ExpiresAt.After(s.now()) {
		return s.transition(message, record, StatusExpired, "", "", "expired")
	}
	return s.transition(message, record, statusForDecision(input.Decision), input.Decision, message.UserID, clean(input.ReasonCode))
}

func (s *approvalService) handleCancel(state aggregateState, message contract.Message) (service.Decision, error) {
	var input CancelRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return rejection(message, errInvalidRequest, "approval cancellation payload is invalid")
	}
	record, found := state.Approvals[input.ApprovalID]
	if !found || record.CallID != input.CallID {
		return rejection(message, errNotFound, "approval was not found")
	}
	if message.From != record.Requester {
		return rejection(message, errAccessDenied, "only the approval requester can cancel")
	}
	if record.Status == StatusCancelled {
		return responseDecision(message, "approval-cancel-idempotent/"+record.ApprovalID, record, true)
	}
	if record.Status.Terminal() {
		return rejection(message, errAlreadyDecided, "approval is already terminal")
	}
	return s.transition(message, record, StatusCancelled, "", "", clean(input.ReasonCode))
}

func (s *approvalService) handleExpire(state aggregateState, message contract.Message) (service.Decision, error) {
	if s.scheduler == "" {
		return rejection(message, errSchedulerUnavailable, "approval scheduler is not configured")
	}
	if message.From != s.scheduler {
		return rejection(message, errAccessDenied, "approval expiration source is not authorized")
	}
	var input ExpireRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return rejection(message, errInvalidRequest, "approval expiration payload is invalid")
	}
	record, found := state.Approvals[input.ApprovalID]
	if !found || record.CallID != input.CallID {
		return rejection(message, errNotFound, "approval was not found")
	}
	if record.Status == StatusExpired {
		return responseDecision(message, "approval-expire-idempotent/"+record.ApprovalID, record, true)
	}
	if record.Status.Terminal() {
		return rejection(message, errAlreadyDecided, "approval is already terminal")
	}
	if record.ExpiresAt == nil || record.ExpiresAt.After(s.now()) {
		return rejection(message, errInvalidRequest, "approval has not reached its persisted expiration time")
	}
	return s.transition(message, record, StatusExpired, "", "", "expired")
}

func (s *approvalService) handleGet(state aggregateState, message contract.Message) (service.Decision, error) {
	var input GetRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil || clean(input.ApprovalID) == "" {
		return rejection(message, errInvalidRequest, "approval id is required")
	}
	record, found := state.Approvals[input.ApprovalID]
	if !found {
		return rejection(message, errNotFound, "approval was not found")
	}
	if message.From != record.Requester && (message.From != s.interaction || message.UserID != record.UserID) {
		return rejection(message, errAccessDenied, "approval query is not authorized")
	}
	return responseDecision(message, "approval-get/"+record.ApprovalID, record, true)
}

func (s *approvalService) handleListPending(state aggregateState, message contract.Message) (service.Decision, error) {
	if message.From != s.interaction || clean(message.UserID) == "" {
		return rejection(message, errAccessDenied, "pending approval query is not authorized")
	}
	var input ListPendingRequest
	if len(message.Payload) > 0 {
		if err := json.Unmarshal(message.Payload, &input); err != nil {
			return rejection(message, errInvalidRequest, "pending approval query payload is invalid")
		}
	}
	userID := clean(input.UserID)
	if userID == "" {
		userID = message.UserID
	}
	if userID != message.UserID {
		return rejection(message, errAccessDenied, "cannot list approvals for another user")
	}
	ids := make([]string, 0)
	for id, record := range state.Approvals {
		if record.Status == StatusPending && record.UserID == userID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	values := make([]State, 0, len(ids))
	for _, id := range ids {
		values = append(values, state.Approvals[id].Clone())
	}
	payload, err := json.Marshal(Response{Approvals: values, Accepted: true})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Reply: &service.Reply{
		Key: "approval-list-pending/" + userID, Type: ResponseMessageType,
		Version: ProtocolVersion, Payload: payload,
	}}, nil
}

func (s *approvalService) transition(message contract.Message, record State, status Status, decision Decision, decidedBy, reasonCode string) (service.Decision, error) {
	now := s.now()
	transition := transitionEvent{
		ApprovalID: record.ApprovalID, Status: status, Decision: decision,
		DecidedAt: now, DecidedBy: decidedBy, ReasonCode: reasonCode,
	}
	eventPayload, err := json.Marshal(transition)
	if err != nil {
		return service.Decision{}, err
	}
	resultPayload, err := json.Marshal(Result{
		ApprovalID: record.ApprovalID, CallID: record.CallID, Requester: record.Requester,
		CapabilityRef: record.CapabilityRef, CapabilityVersion: record.CapabilityVersion,
		Status: status, Decision: decision, DecidedAt: now, DecidedBy: decidedBy, ReasonCode: reasonCode,
	})
	if err != nil {
		return service.Decision{}, err
	}
	stateEventType, messageType := approvalResolvedStateEvent, ResolvedEventType
	switch status {
	case StatusApproved, StatusDenied:
	case StatusCancelled:
		stateEventType, messageType = approvalCancelledStateEvent, CancelledEventType
	case StatusExpired:
		stateEventType, messageType = approvalExpiredStateEvent, ExpiredEventType
	default:
		return service.Decision{}, fmt.Errorf("unsupported approval transition to %q", status)
	}
	keySuffix := record.ApprovalID + "/" + string(status)
	decisionOutput := service.Decision{
		Events: []service.NewEvent{{
			Key:  "approval-state-terminal/" + keySuffix,
			Type: stateEventType, Version: ProtocolVersion, Payload: eventPayload,
		}},
		Outgoing: []service.OutgoingMessage{{
			Key:  "approval-result/" + keySuffix,
			Kind: contract.MessageEvent, Type: messageType, Version: ProtocolVersion,
			To: record.Requester, CorrelationID: record.CallID, Payload: resultPayload,
		}},
	}
	if message.ReplyTo != "" {
		updated := record.Clone()
		updated.Status, updated.Decision, updated.DecidedBy, updated.ReasonCode = status, decision, decidedBy, reasonCode
		updated.DecidedAt = &now
		reply, err := responseReply("approval-transition-accepted/"+keySuffix, updated, true)
		if err != nil {
			return service.Decision{}, err
		}
		decisionOutput.Reply = reply
	}
	return decisionOutput, nil
}

func (s *approvalService) Apply(raw service.State, event contract.StoredEvent) (service.State, error) {
	if event.EventVersion != ProtocolVersion {
		return service.State{}, fmt.Errorf("unsupported approval event %q v%d", event.EventType, event.EventVersion)
	}
	state, err := decodeState(raw)
	if err != nil {
		return service.State{}, err
	}
	switch event.EventType {
	case approvalRequestedStateEvent:
		var record State
		if err := json.Unmarshal(event.Payload, &record); err != nil {
			return service.State{}, fmt.Errorf("decode approval requested event: %w", err)
		}
		if _, exists := state.Approvals[record.ApprovalID]; exists || record.Status != StatusPending ||
			clean(record.ApprovalID) == "" || clean(record.CallID) == "" || record.Requester == "" || record.NotifyTo == "" ||
			clean(record.UserID) == "" || clean(record.CapabilityRef) == "" || clean(record.CapabilityVersion) == "" ||
			clean(record.ArgumentsDigest) == "" || record.RequestedAt.IsZero() {
			return service.State{}, fmt.Errorf("approval requested event conflicts with state")
		}
		state.Approvals[record.ApprovalID] = record.Clone()
	case approvalResolvedStateEvent, approvalCancelledStateEvent, approvalExpiredStateEvent:
		var transition transitionEvent
		if err := json.Unmarshal(event.Payload, &transition); err != nil {
			return service.State{}, fmt.Errorf("decode approval transition event: %w", err)
		}
		record, exists := state.Approvals[transition.ApprovalID]
		if !exists || record.Status != StatusPending || !transition.Status.Terminal() || transition.DecidedAt.IsZero() {
			return service.State{}, fmt.Errorf("approval transition event violates state invariant")
		}
		if event.EventType == approvalResolvedStateEvent && (transition.Status != StatusApproved && transition.Status != StatusDenied) {
			return service.State{}, fmt.Errorf("approval resolved event has invalid status %q", transition.Status)
		}
		if event.EventType == approvalCancelledStateEvent && transition.Status != StatusCancelled {
			return service.State{}, fmt.Errorf("approval cancelled event has invalid status %q", transition.Status)
		}
		if event.EventType == approvalExpiredStateEvent && transition.Status != StatusExpired {
			return service.State{}, fmt.Errorf("approval expired event has invalid status %q", transition.Status)
		}
		if (transition.Status == StatusApproved || transition.Status == StatusDenied) && (!transition.Decision.Valid() || clean(transition.DecidedBy) == "") {
			return service.State{}, fmt.Errorf("approval resolution event lacks decision audit facts")
		}
		record.Status, record.Decision = transition.Status, transition.Decision
		record.DecidedBy, record.ReasonCode = transition.DecidedBy, transition.ReasonCode
		record.DecidedAt = cloneTime(&transition.DecidedAt)
		state.Approvals[record.ApprovalID] = record.Clone()
	default:
		return service.State{}, fmt.Errorf("unsupported approval event %q v%d", event.EventType, event.EventVersion)
	}
	return encodeState(state)
}

func validateRequest(input Request) error {
	if clean(input.ApprovalID) == "" || clean(input.CallID) == "" || clean(input.UserID) == "" ||
		clean(input.CapabilityRef) == "" || clean(input.CapabilityVersion) == "" || clean(input.RiskSummary) == "" ||
		clean(input.ArgumentsDigest) == "" || input.RequestedAt.IsZero() {
		return fmt.Errorf("approval id, call id, user, capability, risk summary, argument digest, and requested time are required")
	}
	if input.ArgumentsRef != nil {
		if err := artifact.ValidateRef(*input.ArgumentsRef); err != nil {
			return fmt.Errorf("arguments artifact reference is invalid")
		}
	}
	if input.ExpiresAt != nil && !input.ExpiresAt.After(input.RequestedAt) {
		return fmt.Errorf("approval expiration must be after requested time")
	}
	return nil
}

func sameRequest(existing State, input Request, requester contract.ServiceAddress) bool {
	if existing.Requester != requester || existing.CallID != input.CallID || existing.UserID != input.UserID ||
		existing.CapabilityRef != input.CapabilityRef || existing.CapabilityVersion != input.CapabilityVersion ||
		existing.RiskSummary != input.RiskSummary || existing.ArgumentsDigest != input.ArgumentsDigest ||
		!existing.RequestedAt.Equal(input.RequestedAt) || !sameTime(existing.ExpiresAt, input.ExpiresAt) {
		return false
	}
	return sameArtifact(existing.ArgumentsRef, input.ArgumentsRef)
}

func sameArtifact(left, right *contract.ArtifactRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func statusForDecision(decision Decision) Status {
	if decision == DecisionApprove {
		return StatusApproved
	}
	return StatusDenied
}

func responseDecision(message contract.Message, key string, record State, accepted bool) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, nil
	}
	reply, err := responseReply(key, record, accepted)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Reply: reply}, nil
}

func responseReply(key string, record State, accepted bool) (*service.Reply, error) {
	value := record.Clone()
	payload, err := json.Marshal(Response{Approval: &value, Accepted: accepted})
	if err != nil {
		return nil, err
	}
	return &service.Reply{Key: key, Type: ResponseMessageType, Version: ProtocolVersion, Payload: payload}, nil
}

func rejection(message contract.Message, code, safeMessage string) (service.Decision, error) {
	if message.ReplyTo == "" {
		return service.Decision{}, fmt.Errorf("%s: %s", code, safeMessage)
	}
	return service.Decision{Reply: &service.Reply{
		Key: "approval-rejected/" + code, Type: ResponseMessageType, Version: ProtocolVersion,
		Error: &service.ReplyError{Code: code, Message: safeMessage},
	}}, nil
}

func (s *approvalService) now() time.Time {
	if s.clock == nil {
		return time.Now().UTC()
	}
	return s.clock.Now().UTC()
}

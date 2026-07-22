package approval

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }

func newTestService(clock contract.Clock) *approvalService {
	return &approvalService{
		address: "approval.main", interaction: "ui.main", scheduler: "scheduler.main",
		clock: clock, trustedRequesters: map[contract.ServiceAddress]struct{}{"capability.main": {}},
	}
}

func initialTestState(t *testing.T, target *approvalService) service.State {
	t.Helper()
	state, err := target.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func applyApprovalDecision(t *testing.T, target *approvalService, state service.State, decision service.Decision) service.State {
	t.Helper()
	for index, event := range decision.Events {
		var err error
		state, err = target.Apply(state, contract.StoredEvent{
			EventID: "event", EventType: event.Type, EventVersion: event.Version,
			Sequence: uint64(index + 1), Payload: event.Payload,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func TestApprovalRequestResolveAndIdempotency(t *testing.T) {
	now := time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC)
	clock := &testClock{now: now}
	target := newTestService(clock)
	state := initialTestState(t, target)
	expires := now.Add(time.Hour)
	payload, _ := json.Marshal(Request{
		ApprovalID: "approval-1", CallID: "call-1", UserID: "user-1",
		CapabilityRef: "shell.exec", CapabilityVersion: "v1",
		RiskSummary: "writes a file", ArgumentsDigest: "digest-1",
		RequestedAt: now, ExpiresAt: &expires,
	})
	request := contract.Message{
		ID: "request-1", Kind: contract.MessageCommand, Type: RequestMessageType, Version: ProtocolVersion,
		From: "capability.main", To: "approval.main", ReplyTo: "capability.main",
		UserID: "user-1", Payload: payload,
	}
	decision, err := target.Handle(context.Background(), state, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Events) != 1 || len(decision.Outgoing) != 1 || decision.Reply == nil {
		t.Fatalf("request decision=%#v", decision)
	}
	if decision.Outgoing[0].To != "ui.main" || decision.Outgoing[0].Type != RequestedEventType {
		t.Fatalf("requested notification=%#v", decision.Outgoing[0])
	}
	if err := decision.ValidateAt(request, nil, now); err != nil {
		t.Fatal(err)
	}
	state = applyApprovalDecision(t, target, state, decision)

	duplicate := request.Clone()
	duplicate.ID = "request-duplicate"
	decision, err = target.Handle(context.Background(), state, duplicate)
	if err != nil || len(decision.Events) != 0 || len(decision.Outgoing) != 0 || decision.Reply == nil {
		t.Fatalf("duplicate request decision=%#v err=%v", decision, err)
	}

	resolvePayload, _ := json.Marshal(ResolveRequest{
		ApprovalID: "approval-1", CallID: "call-1", Decision: DecisionApprove, ReasonCode: "confirmed",
	})
	unauthorized := contract.Message{
		ID: "resolve-unauthorized", Kind: contract.MessageCommand, Type: ResolveMessageType, Version: ProtocolVersion,
		From: "evil", ReplyTo: "ui.main", UserID: "user-1", Payload: resolvePayload,
	}
	decision, err = target.Handle(context.Background(), state, unauthorized)
	if err != nil || decision.Reply == nil || decision.Reply.Error == nil || decision.Reply.Error.Code != errAccessDenied {
		t.Fatalf("unauthorized resolution=%#v err=%v", decision, err)
	}

	resolve := unauthorized.Clone()
	resolve.ID, resolve.From = "resolve-1", "ui.main"
	decision, err = target.Handle(context.Background(), state, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Events) != 1 || len(decision.Outgoing) != 1 || decision.Outgoing[0].To != "capability.main" || decision.Outgoing[0].Type != ResolvedEventType {
		t.Fatalf("resolve decision=%#v", decision)
	}
	state = applyApprovalDecision(t, target, state, decision)

	decision, err = target.Handle(context.Background(), state, resolve)
	if err != nil || len(decision.Events) != 0 || decision.Reply == nil || decision.Reply.Error != nil {
		t.Fatalf("idempotent resolution=%#v err=%v", decision, err)
	}
	conflictPayload, _ := json.Marshal(ResolveRequest{
		ApprovalID: "approval-1", CallID: "call-1", Decision: DecisionDeny,
	})
	conflict := resolve.Clone()
	conflict.ID, conflict.Payload = "resolve-conflict", conflictPayload
	decision, err = target.Handle(context.Background(), state, conflict)
	if err != nil || decision.Reply == nil || decision.Reply.Error == nil || decision.Reply.Error.Code != errAlreadyDecided {
		t.Fatalf("conflicting resolution=%#v err=%v", decision, err)
	}

	decoded, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	stored := decoded.Approvals["approval-1"]
	if stored.Status != StatusApproved || stored.DecidedBy != "user-1" || stored.Decision != DecisionApprove {
		t.Fatalf("stored approval=%#v", stored)
	}
}

func TestApprovalReplayAndQueriesAreDeterministic(t *testing.T) {
	now := time.Date(2026, 7, 21, 11, 0, 0, 0, time.UTC)
	clock := &testClock{now: now}
	target := newTestService(clock)
	initial := initialTestState(t, target)
	payload, _ := json.Marshal(Request{
		ApprovalID: "approval-replay", CallID: "call-replay", UserID: "user-1",
		CapabilityRef: "tool.read", CapabilityVersion: "v1", RiskSummary: "reads data",
		ArgumentsDigest: "digest", RequestedAt: now,
	})
	message := contract.Message{
		ID: "request-replay", Kind: contract.MessageCommand, Type: RequestMessageType, Version: ProtocolVersion,
		From: "capability.main", ReplyTo: "capability.main", UserID: "user-1", Payload: payload,
	}
	requestDecision, err := target.Handle(context.Background(), initial, message)
	if err != nil {
		t.Fatal(err)
	}
	first := applyApprovalDecision(t, target, initial, requestDecision)
	second := applyApprovalDecision(t, target, initial, requestDecision)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("replay states differ\nfirst=%s\nsecond=%s", first.Data, second.Data)
	}

	queryPayload, _ := json.Marshal(ListPendingRequest{UserID: "user-1"})
	query := contract.Message{
		ID: "list", Kind: contract.MessageQuery, Type: ListPendingMessageType, Version: ProtocolVersion,
		From: "ui.main", ReplyTo: "ui.main", UserID: "user-1", Payload: queryPayload,
	}
	decision, err := target.Handle(context.Background(), first, query)
	if err != nil || len(decision.Events) != 0 || len(decision.Effects) != 0 || decision.Reply == nil {
		t.Fatalf("query decision=%#v err=%v", decision, err)
	}

	_, err = target.Apply(first, contract.StoredEvent{EventType: "approval.unknown", EventVersion: 1})
	if err == nil {
		t.Fatal("unknown replay event was accepted")
	}
}

package capability

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"agent/services/approval"
)

type capabilityTestClock struct{ now time.Time }

func (c *capabilityTestClock) Now() time.Time { return c.now }

type testProvider struct {
	descriptor CapabilityDescriptor
	plan       CapabilityExecutionPlan
	calls      int
}

func (p *testProvider) Ref() string { return p.descriptor.ProviderRef }

func (p *testProvider) Describe() []CapabilityDescriptor {
	return []CapabilityDescriptor{p.descriptor.Clone()}
}

func (p *testProvider) Plan(context.Context, CapabilityInvocation) (CapabilityExecutionPlan, error) {
	p.calls++
	return p.plan, nil
}

func newCapabilityTestService(t *testing.T, clock contract.Clock, provider *testProvider, evaluator AuthorizationEvaluator) *capabilityService {
	t.Helper()
	catalog, err := NewCatalog([]CapabilityProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	return &capabilityService{
		address: "capability.main", approvalAddress: "approval.main", schedulerAddress: "scheduler.main",
		catalog: catalog, evaluator: evaluator, clock: clock,
		terminalRetention: time.Hour, idempotencyWindow: 24 * time.Hour,
	}
}

func newEffectProvider() *testProvider {
	return &testProvider{
		descriptor: CapabilityDescriptor{
			Ref: "test.echo", Version: "v1", ProviderRef: "test-provider",
			ExecutionKind: ExecutionEffect, ExecutorRef: "test.echo@v1",
			EffectType: "test.echo", DescriptorRevision: "descriptor-1",
		},
		plan: CapabilityExecutionPlan{
			Kind: ExecutionEffect, ExecutionKey: "echo",
			Effect: &EffectPlan{Type: "test.echo", Version: 1, ExecutorRef: "test.echo@v1", Payload: json.RawMessage(`{"value":"hello"}`)},
		},
	}
}

func initialCapabilityState(t *testing.T, target *capabilityService) service.State {
	t.Helper()
	state, err := target.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func applyCapabilityDecision(t *testing.T, target *capabilityService, state service.State, decision service.Decision) service.State {
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

func invokeMessage(t *testing.T, callID string, arguments string, deadline time.Time) contract.Message {
	t.Helper()
	payload, err := json.Marshal(InvokeRequest{
		CallID: callID, CapabilityRef: "test.echo", CapabilityVersion: "v1",
		Arguments: json.RawMessage(arguments),
	})
	if err != nil {
		t.Fatal(err)
	}
	return contract.Message{
		ID: "invoke-" + callID, Kind: contract.MessageCommand, Type: InvokeMessageType, Version: ProtocolVersion,
		From: "agent.main", To: "capability.main", ReplyTo: "agent.main",
		RuntimeID: "runtime", PlanRevision: "v1", UserID: "user-1",
		CorrelationID: "correlation-" + callID, Deadline: &deadline, Payload: payload,
	}
}

func TestCapabilityAllowPlansEffectAndCompletesThroughDurableResult(t *testing.T) {
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	clock := &capabilityTestClock{now: now}
	provider := newEffectProvider()
	target := newCapabilityTestService(t, clock, provider, AuthorizationEvaluatorFunc(func(AuthorizationInput) (AuthorizationDecision, error) {
		return AuthorizationDecision{Decision: AuthorizationAllow, RuleRef: "allow-test@v1", ReasonCode: "allowed"}, nil
	}))
	initial := initialCapabilityState(t, target)
	message := invokeMessage(t, "call-allow", `{"input":1}`, now.Add(time.Hour))
	decision, err := target.Handle(context.Background(), initial, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Events) != 3 || len(decision.Effects) != 1 || len(decision.Outgoing) != 0 || decision.Reply != nil {
		t.Fatalf("allow decision=%#v", decision)
	}
	if provider.calls != 1 || decision.Effects[0].IdempotencyKey == "" {
		t.Fatalf("provider calls=%d effect=%#v", provider.calls, decision.Effects[0])
	}
	if err := decision.ValidateAt(message, func(ref string) bool { return ref == "test.echo@v1" }, now); err != nil {
		t.Fatal(err)
	}
	invokeDecision := decision
	state := applyCapabilityDecision(t, target, initial, decision)

	duplicate := message.Clone()
	duplicate.ID = "invoke-call-allow-duplicate"
	decision, err = target.Handle(context.Background(), state, duplicate)
	if err != nil || len(decision.Events) != 0 || len(decision.Effects) != 0 || decision.Reply == nil || provider.calls != 1 {
		t.Fatalf("duplicate decision=%#v provider_calls=%d err=%v", decision, provider.calls, err)
	}

	completedPayload, _ := json.Marshal(ExecutionCompleted{
		CallID: "call-allow", ExecutionKey: "echo", ExecutorRef: "test.echo@v1",
		Generation: 1, OutcomeID: "effect-1", Result: json.RawMessage(`{"value":"world"}`),
	})
	completed := contract.Message{
		ID: "completion-1", Kind: contract.MessageEvent, Type: ExecutionCompletedMessageType, Version: ProtocolVersion,
		From: "capability.main", To: "capability.main", Payload: completedPayload,
	}
	decision, err = target.Handle(context.Background(), state, completed)
	if err != nil || len(decision.Events) != 1 || len(decision.Outgoing) != 1 || decision.Outgoing[0].Kind != contract.MessageReply || decision.Outgoing[0].To != "agent.main" {
		t.Fatalf("completion decision=%#v err=%v", decision, err)
	}
	state = applyCapabilityDecision(t, target, state, decision)
	decoded, err := decodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	call := decoded.Calls["call-allow"]
	if call.Phase != PhaseSucceeded || string(call.Result) != `{"value":"world"}` {
		t.Fatalf("completed call=%#v", call)
	}

	completionDecision := decision
	replayed := applyCapabilityDecision(t, target, initial, service.Decision{Events: append(append([]service.NewEvent(nil),
		invokeDecision.Events...), completionDecision.Events...)})
	if provider.calls != 1 {
		t.Fatalf("unexpected provider calls during replay: %d", provider.calls)
	}
	if !reflect.DeepEqual(state, replayed) {
		t.Fatalf("replay states differ\nstate=%s\nreplayed=%s", state.Data, replayed.Data)
	}
}

func TestCapabilityAskUsesApprovalMessagesAndStableApprovalID(t *testing.T) {
	now := time.Date(2026, 7, 21, 13, 0, 0, 0, time.UTC)
	clock := &capabilityTestClock{now: now}
	provider := newEffectProvider()
	target := newCapabilityTestService(t, clock, provider, AuthorizationEvaluatorFunc(func(AuthorizationInput) (AuthorizationDecision, error) {
		return AuthorizationDecision{
			Decision: AuthorizationAsk, RuleRef: "ask-test@v1", ReasonCode: "needs_confirmation",
			RiskSummary: "executes a test action", ApprovalScope: "call",
		}, nil
	}))
	state := initialCapabilityState(t, target)
	message := invokeMessage(t, "call-ask", `{"input":2}`, now.Add(time.Hour))
	decision, err := target.Handle(context.Background(), state, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Events) != 2 || len(decision.Outgoing) != 1 || len(decision.Effects) != 0 || provider.calls != 0 {
		t.Fatalf("ask decision=%#v provider_calls=%d", decision, provider.calls)
	}
	if decision.Outgoing[0].Type != approval.RequestMessageType || decision.Outgoing[0].To != "approval.main" {
		t.Fatalf("approval request=%#v", decision.Outgoing[0])
	}
	var approvalRequest approval.Request
	if err := json.Unmarshal(decision.Outgoing[0].Payload, &approvalRequest); err != nil {
		t.Fatal(err)
	}
	if approvalRequest.ApprovalID == "" || approvalRequest.CallID != "call-ask" {
		t.Fatalf("approval request payload=%#v", approvalRequest)
	}
	state = applyCapabilityDecision(t, target, state, decision)

	duplicate := message.Clone()
	duplicate.ID = "duplicate-ask"
	duplicateDecision, err := target.Handle(context.Background(), state, duplicate)
	if err != nil || len(duplicateDecision.Outgoing) != 0 || len(duplicateDecision.Effects) != 0 {
		t.Fatalf("duplicate ask=%#v err=%v", duplicateDecision, err)
	}

	approvedAt := now.Add(time.Minute)
	approvedPayload, _ := json.Marshal(approval.Result{
		ApprovalID: approvalRequest.ApprovalID, CallID: "call-ask", Requester: "capability.main",
		CapabilityRef: "test.echo", CapabilityVersion: "v1", Status: approval.StatusApproved,
		Decision: approval.DecisionApprove, DecidedAt: approvedAt, DecidedBy: "user-1",
	})
	approved := contract.Message{
		ID: "approval-result", Kind: contract.MessageEvent, Type: approval.ResolvedEventType, Version: approval.ProtocolVersion,
		From: "approval.main", To: "capability.main", Payload: approvedPayload,
	}
	decision, err = target.Handle(context.Background(), state, approved)
	if err != nil || len(decision.Events) != 1 || len(decision.Effects) != 1 || provider.calls != 1 {
		t.Fatalf("approved decision=%#v provider_calls=%d err=%v", decision, provider.calls, err)
	}
	state = applyCapabilityDecision(t, target, state, decision)
	decoded, _ := decodeState(state)
	call := decoded.Calls["call-ask"]
	if call.Phase != PhaseWaitingExecution || call.ApprovalDecision != string(approval.DecisionApprove) || call.ApprovalDecidedBy != "user-1" {
		t.Fatalf("approved call=%#v", call)
	}
}

func TestCapabilityDenyAndConflictDoNotExecuteProvider(t *testing.T) {
	now := time.Date(2026, 7, 21, 14, 0, 0, 0, time.UTC)
	provider := newEffectProvider()
	target := newCapabilityTestService(t, &capabilityTestClock{now: now}, provider, AuthorizationEvaluatorFunc(func(AuthorizationInput) (AuthorizationDecision, error) {
		return AuthorizationDecision{Decision: AuthorizationDeny, RuleRef: "deny-test@v1", ReasonCode: "blocked"}, nil
	}))
	state := initialCapabilityState(t, target)
	message := invokeMessage(t, "call-deny", `{"input":3}`, now.Add(time.Hour))
	decision, err := target.Handle(context.Background(), state, message)
	if err != nil || len(decision.Effects) != 0 || len(decision.Outgoing) != 0 || decision.Reply == nil || provider.calls != 0 {
		t.Fatalf("deny decision=%#v provider_calls=%d err=%v", decision, provider.calls, err)
	}
	state = applyCapabilityDecision(t, target, state, decision)
	decoded, _ := decodeState(state)
	if decoded.Calls["call-deny"].Phase != PhaseDenied {
		t.Fatalf("denied call=%#v", decoded.Calls["call-deny"])
	}

	conflict := invokeMessage(t, "call-deny", `{"different":true}`, now.Add(time.Hour))
	conflict.ID = "invoke-conflict"
	decision, err = target.Handle(context.Background(), state, conflict)
	if err != nil || decision.Reply == nil || decision.Reply.Error == nil || decision.Reply.Error.Code != errCallConflict || provider.calls != 0 {
		t.Fatalf("conflict decision=%#v provider_calls=%d err=%v", decision, provider.calls, err)
	}

	queryPayload, _ := json.Marshal(GetRequest{CallID: "call-deny"})
	query := contract.Message{
		ID: "get-deny", Kind: contract.MessageQuery, Type: GetMessageType, Version: ProtocolVersion,
		From: "agent.main", ReplyTo: "agent.main", Payload: queryPayload,
	}
	decision, err = target.Handle(context.Background(), state, query)
	if err != nil || len(decision.Events) != 0 || len(decision.Effects) != 0 || decision.Reply == nil {
		t.Fatalf("query decision=%#v err=%v", decision, err)
	}

	_, err = target.Apply(state, contract.StoredEvent{EventType: "capability.unknown", EventVersion: 1})
	if err == nil {
		t.Fatal("unknown replay event was accepted")
	}
}

func TestCapabilityRetentionCompactsThenRemovesTombstone(t *testing.T) {
	now := time.Date(2026, 7, 21, 18, 0, 0, 0, time.UTC)
	clock := &capabilityTestClock{now: now}
	provider := newEffectProvider()
	target := newCapabilityTestService(t, clock, provider, AuthorizationEvaluatorFunc(func(AuthorizationInput) (AuthorizationDecision, error) {
		return AuthorizationDecision{Decision: AuthorizationAllow, RuleRef: "allow@v1", ReasonCode: "allowed"}, nil
	}))
	completedAt := now.Add(-2 * time.Hour)
	state, err := encodeState(aggregateState{
		Calls: map[string]CallState{
			"call-retained": {
				CallID: "call-retained", Caller: "agent.main", ReplyTo: "agent.main",
				CapabilityRef: "test.echo", CapabilityVersion: "v1",
				IdentityFingerprint: "fingerprint", Phase: PhaseSucceeded,
				ReceivedAt: completedAt.Add(-time.Minute), CompletedAt: &completedAt,
				Result: json.RawMessage(`{"ok":true}`),
			},
		},
		Tombstones: make(map[string]CallTombstone),
	})
	if err != nil {
		t.Fatal(err)
	}
	prunePayload, _ := json.Marshal(PruneRequest{Before: now})
	prune := contract.Message{
		ID: "prune-1", Kind: contract.MessageCommand, Type: PruneMessageType, Version: ProtocolVersion,
		From: "scheduler.main", ReplyTo: "scheduler.main", Payload: prunePayload,
	}
	decision, err := target.Handle(context.Background(), state, prune)
	if err != nil || len(decision.Events) != 1 {
		t.Fatalf("compact decision=%#v err=%v", decision, err)
	}
	state = applyCapabilityDecision(t, target, state, decision)
	decoded, _ := decodeState(state)
	if _, exists := decoded.Calls["call-retained"]; exists {
		t.Fatal("terminal call was not compacted")
	}
	if _, exists := decoded.Tombstones["call-retained"]; !exists {
		t.Fatal("idempotency tombstone was not retained")
	}

	clock.now = now.Add(25 * time.Hour)
	prunePayload, _ = json.Marshal(PruneRequest{Before: clock.now})
	prune.ID, prune.Payload = "prune-2", prunePayload
	decision, err = target.Handle(context.Background(), state, prune)
	if err != nil || len(decision.Events) != 1 {
		t.Fatalf("remove decision=%#v err=%v", decision, err)
	}
	state = applyCapabilityDecision(t, target, state, decision)
	decoded, _ = decodeState(state)
	if _, exists := decoded.Tombstones["call-retained"]; exists {
		t.Fatal("expired idempotency tombstone was not removed")
	}
}

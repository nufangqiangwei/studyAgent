package agent

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

func TestParseModelAction(t *testing.T) {
	action, err := parseModelAction([]byte(`{"action":"capability","capability_ref":"workspace.read","capability_version":"v1","arguments":{"path":"README.md"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if action.Action != "capability" || action.CapabilityRef != "workspace.read" || string(action.Arguments) != `{"path":"README.md"}` {
		t.Fatalf("action=%#v", action)
	}
	finish, err := parseModelAction([]byte("```json\n{\"action\":\"finish\",\"answer\":\"done\"}\n```"))
	if err != nil || finish.Answer != "done" {
		t.Fatalf("finish=%#v err=%v", finish, err)
	}
	if _, err := parseModelAction([]byte(`{"action":"finish","answer":""}`)); err == nil {
		t.Fatal("expected empty answer to be rejected")
	}
}

func TestExecuteStartsDurableCapabilityDiscovery(t *testing.T) {
	now := time.Date(2026, 7, 22, 8, 0, 0, 0, time.UTC)
	svc := &agentService{
		address: "agent.main", modelAddress: "model.default", capabilityAddress: "capability.main",
		spec:  AgentSpec{Ref: "coding", Version: "v1", SystemPrompt: "Complete the task."}.withDefaults(),
		clock: testClock{now: now},
	}
	initial, err := svc.InitialState(context.Background(), service.Init{})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(ExecuteRequest{RunID: "run-1", Input: "fix the project"})
	message := contract.Message{
		ID: "execute-1", Kind: contract.MessageCommand, Type: ExecuteMessageType, Version: ProtocolVersion,
		From: "owner", To: "agent.main", ReplyTo: "owner", UserID: "user-1", Payload: payload,
	}
	decision, err := svc.Handle(context.Background(), initial, message)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Events) != 1 || len(decision.Outgoing) != 1 || len(decision.Effects) != 0 {
		t.Fatalf("decision=%#v", decision)
	}
	query := decision.Outgoing[0]
	if query.Kind != contract.MessageQuery || query.Type != "capability.list" || query.ReplyTo != "agent.main" {
		t.Fatalf("query=%#v", query)
	}
	stored := contract.StoredEvent{EventType: runStartedEvent, EventVersion: ProtocolVersion, Payload: decision.Events[0].Payload}
	next, err := svc.Apply(initial, stored)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeState(next)
	if err != nil {
		t.Fatal(err)
	}
	run := decoded.Runs["run-1"]
	if run.Phase != PhaseDiscoveringCapabilities || run.PendingCorrelation == "" || run.StartedAt != now {
		t.Fatalf("run=%#v", run)
	}

	replayed, err := svc.Apply(initial, stored)
	if err != nil {
		t.Fatal(err)
	}
	if string(replayed.Data) != string(next.Data) {
		t.Fatalf("replay differs:\n%s\n%s", next.Data, replayed.Data)
	}
}

func TestAgentSpecRejectsDuplicateCapabilities(t *testing.T) {
	spec := AgentSpec{
		Ref: "coding", Version: "v1", SystemPrompt: "Complete tasks.",
		Capabilities: []CapabilityPrompt{{Ref: "workspace.read", Version: "v1"}, {Ref: "workspace.read", Version: "v1"}},
	}.withDefaults()
	if err := spec.validate(); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("error=%v", err)
	}
}

func TestGetIsReadOnlyAndCancelIsDurable(t *testing.T) {
	now := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	svc := &agentService{address: "agent.main", capabilityAddress: "capability.main", clock: testClock{now: now}}
	run := RunState{
		RunID: "run-query", Phase: PhaseWaitingModel, Caller: "owner", ReplyTo: "owner",
		CorrelationID: "execute-query", IdentityFingerprint: "fingerprint", PendingCorrelation: "model-query",
		PendingTurn: 1, Turns: []TurnRecord{{Number: 1}}, StartedAt: now,
	}
	raw, err := encodeState(aggregateState{Runs: map[string]RunState{run.RunID: run}})
	if err != nil {
		t.Fatal(err)
	}
	getPayload, _ := json.Marshal(GetRequest{RunID: run.RunID})
	getDecision, err := svc.Handle(context.Background(), raw, contract.Message{
		ID: "get-run", Kind: contract.MessageQuery, Type: GetMessageType, Version: ProtocolVersion,
		From: "owner", ReplyTo: "owner", Payload: getPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if getDecision.Reply == nil || len(getDecision.Events) != 0 || len(getDecision.Effects) != 0 {
		t.Fatalf("get decision=%#v", getDecision)
	}
	cancelPayload, _ := json.Marshal(CancelRequest{RunID: run.RunID})
	cancelDecision, err := svc.Handle(context.Background(), raw, contract.Message{
		ID: "cancel-run", Kind: contract.MessageCommand, Type: CancelMessageType, Version: ProtocolVersion,
		From: "owner", ReplyTo: "owner", Payload: cancelPayload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cancelDecision.Events) != 1 || cancelDecision.Events[0].Type != runCancelledEvent || len(cancelDecision.Outgoing) != 1 || cancelDecision.Reply == nil {
		t.Fatalf("cancel decision=%#v", cancelDecision)
	}
}

func TestApplyRejectsUnknownAgentEvent(t *testing.T) {
	svc := &agentService{}
	raw, err := encodeState(initialAggregateState())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Apply(raw, contract.StoredEvent{EventType: "agent.unknown", EventVersion: ProtocolVersion}); err == nil {
		t.Fatal("expected unknown event to be rejected")
	}
}

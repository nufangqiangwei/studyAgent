package system

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"testing"
)

func TestRuntimeServicePlansDurableSystemEffect(t *testing.T) {
	svc := &runtimeService{}
	declaration := DeclareInstanceRequest{
		InstanceID: "child-1", Address: "agent.child.1",
		Component: contract.ComponentRef{Type: "agent.worker", Version: "v1"}, ParentID: "root-1",
	}
	payload := mustJSON(t, Call{
		CallID: "spawn-1", Operation: DeclareInstanceOperation, OperationVersion: 1,
		Payload: mustJSON(t, declaration),
	})
	decision, err := svc.Handle(context.Background(), zeroState(), contract.Message{
		Kind: contract.MessageCommand, Type: CallMessageType, Version: 1,
		From: "agent.supervisor", ReplyTo: "agent.supervisor", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Reply != nil || len(decision.Effects) != 1 {
		t.Fatalf("decision = %#v", decision)
	}
	planned := decision.Effects[0]
	if planned.ExecutorRef != ExecutorRef || planned.IdempotencyKey != "spawn-1" {
		t.Fatalf("planned effect = %#v", planned)
	}
	var input systemEffectPayload
	if err := json.Unmarshal(planned.Payload, &input); err != nil {
		t.Fatal(err)
	}
	if input.Caller != "agent.supervisor" || input.ReplyTo != "agent.supervisor" || input.Call.CallID != "spawn-1" {
		t.Fatalf("effect input = %#v", input)
	}
}

func TestRuntimeServiceReturnsStructuredRejection(t *testing.T) {
	svc := &runtimeService{}
	payload := mustJSON(t, Call{CallID: "call-2", Operation: "runtime.unsupported", OperationVersion: 1})
	decision, err := svc.Handle(context.Background(), zeroState(), contract.Message{
		Kind: contract.MessageCommand, Type: CallMessageType, Version: 1,
		From: "agent.untrusted", ReplyTo: "agent.untrusted", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Reply == nil || decision.Reply.Error == nil || decision.Reply.Error.Code != "unsupported_operation" {
		t.Fatalf("reply = %#v", decision.Reply)
	}
	if decision.Reply.Metadata[MetadataCallID] != "call-2" {
		t.Fatalf("reply metadata = %#v", decision.Reply.Metadata)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func zeroState() service.State {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}
}

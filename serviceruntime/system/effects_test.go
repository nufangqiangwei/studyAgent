package system

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type effectTestControl struct {
	calls int
}

func (c *effectTestControl) Declare(_ context.Context, _ contract.ServiceAddress, declaration instance.Declaration) (instance.Record, error) {
	c.calls++
	return instance.Record{InstanceID: declaration.InstanceID, Address: declaration.Address}, nil
}

type effectTestIDs struct{}

func (effectTestIDs) New(kind string) (string, error) { return kind + "-new", nil }
func (effectTestIDs) Derive(kind string, parts ...string) string {
	return kind + ":" + strings.Join(parts, ":")
}

func TestSystemEffectRedeliveryUsesStableResultMessage(t *testing.T) {
	ctx := context.Background()
	control := &effectTestControl{}
	var messages []contract.Message
	module := NewModule()
	if err := module.BindRuntime(assembly.RuntimePorts{
		RuntimeID: "runtime-1", PlanRevision: "v1", Instances: control, IDs: effectTestIDs{},
		Ingress: assembly.MessageIngressFunc(func(_ context.Context, message contract.Message) error {
			messages = append(messages, message.Clone())
			return nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	call := Call{
		CallID: "spawn-1", Operation: DeclareInstanceOperation, OperationVersion: 1,
		Payload: mustJSON(t, DeclareInstanceRequest{
			InstanceID: "child-1", Address: "agent.child.1",
			Component: contract.ComponentRef{Type: "agent.worker", Version: "v1"},
		}),
	}
	effectPayload, err := json.Marshal(systemEffectPayload{Call: call, Caller: "agent.supervisor", ReplyTo: "agent.supervisor"})
	if err != nil {
		t.Fatal(err)
	}
	record := persistence.EffectRecord{
		EffectID: "effect-1", RuntimeID: "runtime-1", PlanRevision: "v1",
		SourceMessageID: "message-1", Payload: effectPayload,
	}
	if _, err := module.executeEffect(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := module.executeEffect(ctx, record); err != nil {
		t.Fatal(err)
	}
	if control.calls != 2 || len(messages) != 2 {
		t.Fatalf("control calls=%d messages=%d", control.calls, len(messages))
	}
	if messages[0].ID == "" || messages[0].ID != messages[1].ID {
		t.Fatalf("result message ids = %q, %q", messages[0].ID, messages[1].ID)
	}
	if messages[0].Kind != contract.MessageReply || messages[0].To != "agent.supervisor" {
		t.Fatalf("result message = %#v", messages[0])
	}
}

package task

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/host"
	"agent/serviceruntime/instance"
	persistencememory "agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/service"
	"agent/services/agent"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

var (
	integrationAgentComponent = contract.ComponentRef{Type: "test.task-agent", Version: "v1"}
	integrationOwnerComponent = contract.ComponentRef{Type: "test.task-owner", Version: "v1"}
)

type integrationAgent struct{}

func (*integrationAgent) Descriptor() service.Descriptor {
	return service.Descriptor{Component: integrationAgentComponent}
}

func (*integrationAgent) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (*integrationAgent) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	switch message.Type {
	case agent.ExecuteMessageType:
		var input agent.ExecuteRequest
		if err := json.Unmarshal(message.Payload, &input); err != nil {
			return service.Decision{}, err
		}
		output := contract.ArtifactRef{Store: "fake-agent", Key: "results/" + input.RunID + ".txt", ContentType: "text/plain", Size: 4}
		payload, err := json.Marshal(agent.ExecuteResult{RunID: input.RunID, Phase: agent.PhaseCompleted, Output: &output, Turns: 1})
		if err != nil {
			return service.Decision{}, err
		}
		return service.Decision{Reply: &service.Reply{
			Key: "fake-agent-completed/" + input.RunID, Type: agent.CompletedMessageType,
			Version: agent.ProtocolVersion, Payload: payload,
		}}, nil
	case agent.CancelMessageType:
		return service.Decision{}, nil
	default:
		return service.Decision{}, fmt.Errorf("fake Agent does not handle %q", message.Type)
	}
}

func (*integrationAgent) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

type integrationOwner struct {
	messages chan contract.Message
}

func (*integrationOwner) Descriptor() service.Descriptor {
	return service.Descriptor{Component: integrationOwnerComponent}
}

func (*integrationOwner) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (o *integrationOwner) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	o.messages <- message.Clone()
	return service.Decision{}, nil
}

func (*integrationOwner) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

func TestPendingAgentExecutionContinuesAfterRuntimeRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := &testClock{now: time.Date(2026, 7, 22, 14, 0, 0, 0, time.UTC)}
	storage := persistencememory.New(clock)
	defer storage.Close()
	messages := make(chan contract.Message, 32)

	runtime1 := buildIntegrationRuntime(t, ctx, storage, clock, messages)
	if _, err := runtime1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := runtime1.DeclareInstance(ctx, serviceruntime.InstanceDeclaration{
		InstanceID: "task-recovery-42", Address: "task.recovery.42", Component: Component,
		Metadata: map[string]string{"task_id": "42"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.Kind != instance.ServiceVirtual {
		t.Fatalf("task instance=%#v", record)
	}

	publishTaskCommand(t, ctx, runtime1, "task.recovery.42", CreateMessageType, "create-42", CreateRequest{
		TaskID: "42", GoalID: "goal-1", Input: "complete after restart",
	})
	deliverAndObserve(t, ctx, runtime1, "owner.main")
	publishTaskCommand(t, ctx, runtime1, "task.recovery.42", MarkReadyMessageType, "ready-42", struct{}{})
	deliverAndObserve(t, ctx, runtime1, "owner.main")
	publishTaskCommand(t, ctx, runtime1, "task.recovery.42", AssignMessageType, "assign-42", AssignRequest{AgentAddress: "agent.test"})
	deliverAndObserve(t, ctx, runtime1, "owner.main")

	clock.now = clock.now.Add(time.Minute)
	publishTaskCommand(t, ctx, runtime1, "task.recovery.42", StartMessageType, "start-42", struct{}{})
	// Deliberately leave both agent.execute and the immediate task.status Reply
	// in the durable Outbox, then restart the process.
	if err := runtime1.Close(); err != nil {
		t.Fatal(err)
	}
	drainObserved(messages)

	runtime2 := buildIntegrationRuntime(t, ctx, storage, clock, messages)
	defer runtime2.Close()
	if _, err := runtime2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	dispatchAll(t, ctx, runtime2)
	handleAll(t, ctx, runtime2, "owner.main")
	handleAll(t, ctx, runtime2, "agent.test")
	dispatchAll(t, ctx, runtime2)
	handleAll(t, ctx, runtime2, "task.recovery.42")
	dispatchAll(t, ctx, runtime2)
	handleAll(t, ctx, runtime2, "owner.main")

	var completed *Result
	for {
		select {
		case message := <-messages:
			if message.Type != CompletedEventType {
				continue
			}
			var result Result
			if err := json.Unmarshal(message.Payload, &result); err != nil {
				t.Fatal(err)
			}
			completed = &result
		default:
			goto observed
		}
	}

observed:
	if completed == nil || completed.TaskID != "42" || completed.Phase != PhaseCompleted || completed.Attempt != 1 || completed.ResultRef == nil {
		t.Fatalf("completed result=%#v", completed)
	}

	queryPayload := mustJSON(t, GetRequest{TaskID: "42"})
	if _, err := runtime2.Publish(ctx, contract.Message{
		ID: "get-after-recovery", Kind: contract.MessageQuery, Type: GetMessageType, Version: ProtocolVersion,
		From: "owner.main", To: "task.recovery.42", ReplyTo: "owner.main", UserID: "user-1", GoalID: "goal-1", Payload: queryPayload,
	}); err != nil {
		t.Fatal(err)
	}
	assertCommitted(t, runtime2, ctx, "task.recovery.42")
	dispatchAll(t, ctx, runtime2)
	handleAll(t, ctx, runtime2, "owner.main")

	var restored *State
	for {
		select {
		case message := <-messages:
			if message.Type != StatusMessageType {
				continue
			}
			var response StatusResponse
			if err := json.Unmarshal(message.Payload, &response); err != nil {
				t.Fatal(err)
			}
			restored = response.Task
		default:
			goto queried
		}
	}

queried:
	if restored == nil || restored.Phase != PhaseCompleted || restored.Attempt != 1 || restored.ResultRef == nil {
		t.Fatalf("restored task=%#v", restored)
	}
}

func buildIntegrationRuntime(t *testing.T, ctx context.Context, storage *persistencememory.Store, clock contract.Clock, messages chan contract.Message) *serviceruntime.Runtime {
	t.Helper()
	builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
		Storage: storage, Clock: clock, IDs: serviceruntime.StableIDs{}, OwnerID: "task-integration-node",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := NewModule(clock).Register(builder); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: integrationAgentComponent,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &integrationAgent{}, nil
		}),
		Scope: building.ScopeMounted,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: agent.ExecuteMessageType, Version: agent.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: agent.CancelMessageType, Version: agent.ProtocolVersion},
		},
		Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: integrationOwnerComponent,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &integrationOwner{messages: messages}, nil
		}),
		Scope: building.ScopeMounted,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageReply, Type: StatusMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: StatusChangedEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: CompletedEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: FailedEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: CancelledEventType, Version: ProtocolVersion},
		},
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "task-integration-runtime", Revision: "v1"},
		Services: []building.ServiceMount{
			{Address: "agent.test", Component: integrationAgentComponent},
			{Address: "owner.main", Component: integrationOwnerComponent},
		},
		Recovery: building.RecoveryPolicy{SnapshotEveryEvents: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func publishTaskCommand(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime, address contract.ServiceAddress, messageType contract.MessageType, id string, payload any) {
	t.Helper()
	if _, err := runtime.Publish(ctx, contract.Message{
		ID: id, Kind: contract.MessageCommand, Type: messageType, Version: ProtocolVersion,
		From: "owner.main", To: address, ReplyTo: "owner.main", UserID: "user-1", GoalID: "goal-1",
		Payload: mustJSON(t, payload),
	}); err != nil {
		t.Fatal(err)
	}
	assertCommitted(t, runtime, ctx, address)
}

func deliverAndObserve(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime, observer contract.ServiceAddress) {
	t.Helper()
	dispatchAll(t, ctx, runtime)
	handleAll(t, ctx, runtime, observer)
}

func dispatchAll(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime) {
	t.Helper()
	for index := 0; index < 64; index++ {
		result, err := runtime.DispatchNextOutbox(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if result.Idle {
			return
		}
	}
	t.Fatal("outbox did not become idle")
}

func handleAll(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime, address contract.ServiceAddress) {
	t.Helper()
	for index := 0; index < 64; index++ {
		result, err := runtime.HandleNext(ctx, address)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status == host.HandleIdle {
			return
		}
		if result.Status != host.HandleCommitted && result.Status != host.HandleDuplicate {
			t.Fatalf("handle %s result=%#v", address, result)
		}
	}
	t.Fatalf("mailbox %s did not become idle", address)
}

func assertCommitted(t *testing.T, runtime *serviceruntime.Runtime, ctx context.Context, address contract.ServiceAddress) {
	t.Helper()
	result, err := runtime.HandleNext(ctx, address)
	if err != nil || result.Status != host.HandleCommitted {
		t.Fatalf("handle %s result=%#v err=%v", address, result, err)
	}
}

func drainObserved(messages chan contract.Message) {
	for {
		select {
		case <-messages:
		default:
			return
		}
	}
}

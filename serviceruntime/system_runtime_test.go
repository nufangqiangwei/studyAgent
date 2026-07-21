package serviceruntime

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/service"
	systemruntime "agent/serviceruntime/system"
	"context"
	"encoding/json"
	"testing"
	"time"
)

var (
	systemCallerComponent    = contract.ComponentRef{Type: "test.system-caller", Version: "v1"}
	systemUntrustedComponent = contract.ComponentRef{Type: "test.system-untrusted", Version: "v1"}
	systemWorkerComponent    = contract.ComponentRef{Type: "test.system-worker", Version: "v1"}
)

type systemCallerService struct {
	address contract.ServiceAddress
	results chan contract.Message
}

func (s *systemCallerService) Descriptor() service.Descriptor {
	if s.address == "untrusted.main" {
		return service.Descriptor{Component: systemUntrustedComponent}
	}
	return service.Descriptor{Component: systemCallerComponent}
}

func (*systemCallerService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (s *systemCallerService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	switch message.Type {
	case "test.spawn":
		var declaration instance.Declaration
		if err := json.Unmarshal(message.Payload, &declaration); err != nil {
			return service.Decision{}, err
		}
		call := systemruntime.Call{
			CallID: "spawn-call-1", Operation: systemruntime.DeclareInstanceOperation, OperationVersion: 1,
			Payload: mustRuntimeJSON(declaration),
		}
		return service.Decision{Outgoing: []service.OutgoingMessage{{
			Key: "declare-child", Kind: contract.MessageCommand,
			Type: systemruntime.CallMessageType, Version: systemruntime.CallVersion,
			To: systemruntime.Address, ReplyTo: s.address, Payload: mustRuntimeJSON(call),
		}}}, nil
	case systemruntime.ResultMessageType:
		if s.results != nil {
			s.results <- message.Clone()
		}
		return service.Decision{}, nil
	default:
		return service.Decision{}, nil
	}
}

func (*systemCallerService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state.Clone(), nil
}

type systemWorkerService struct{}

func (systemWorkerService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: systemWorkerComponent}
}
func (systemWorkerService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1}, nil
}
func (systemWorkerService) Handle(context.Context, service.State, contract.Message) (service.Decision, error) {
	return service.Decision{}, nil
}
func (systemWorkerService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state.Clone(), nil
}

func TestSystemRuntimeServiceDeclaresVirtualChildIdempotently(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC)}
	store := memory.New(clock)
	builder, err := NewBuilder(BuilderOptions{Storage: store, Clock: clock, IDs: StableIDs{}, OwnerID: "system-test-node"})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan contract.Message, 4)
	registerSystemTestServices(t, builder, results)
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "system-test", Revision: "v1"},
		Services: []building.ServiceMount{
			{Address: "supervisor.main", Component: systemCallerComponent},
			{Address: "untrusted.main", Component: systemUntrustedComponent},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, found := runtime.Plan().Service(systemruntime.Address); !found {
		t.Fatal("built-in system runtime service is not mounted")
	}
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	root, err := runtime.DeclareInstance(ctx, InstanceDeclaration{Address: "agent.root", Component: systemWorkerComponent})
	if err != nil {
		t.Fatal(err)
	}
	declaration := instance.Declaration{
		InstanceID: "agent-child-1", Address: "agent.child.1",
		Component: systemWorkerComponent, ParentID: root.InstanceID,
		Metadata: map[string]string{"spawn_id": "spawn-1"},
	}

	runSystemSpawn(t, ctx, runtime, "spawn-command-1", declaration)
	assertSystemSuccess(t, <-results, "agent-child-1")
	child, found, err := store.Instances().Get(ctx, "agent-child-1")
	if err != nil || !found {
		t.Fatalf("child found=%v err=%v", found, err)
	}
	if child.ParentID != root.InstanceID || child.RootID != root.InstanceID || child.Depth != 1 {
		t.Fatalf("child relationship = %#v", child)
	}

	// A different delivery carrying the same stable declaration returns the
	// existing logical instance instead of creating a second child.
	runSystemSpawn(t, ctx, runtime, "spawn-command-2", declaration)
	assertSystemSuccess(t, <-results, "agent-child-1")
	kind := instance.ServiceVirtual
	records, err := store.Instances().List(ctx, instance.Query{RuntimeID: "system-test", PlanRevision: "v1", Kind: &kind})
	if err != nil || len(records) != 2 {
		t.Fatalf("virtual records=%#v err=%v", records, err)
	}
}

func TestSystemRuntimeServiceRejectsUnauthorizedCaller(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 21, 11, 0, 0, 0, time.UTC)}
	builder, err := NewBuilder(BuilderOptions{Clock: clock, IDs: StableIDs{}, OwnerID: "system-auth-node"})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan contract.Message, 1)
	registerSystemTestServices(t, builder, results)
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime:  building.RuntimeSpec{ID: "system-auth", Revision: "v1"},
		Services: []building.ServiceMount{{Address: "untrusted.main", Component: systemUntrustedComponent}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	call := systemruntime.Call{
		CallID: "unauthorized-1", Operation: assembly.SystemOperationDeclareInstance, OperationVersion: 1,
		Payload: mustRuntimeJSON(instance.Declaration{Address: "agent.denied", Component: systemWorkerComponent}),
	}
	if _, err := runtime.Publish(ctx, contract.Message{
		ID: "unauthorized-message-1", Kind: contract.MessageCommand,
		Type: systemruntime.CallMessageType, Version: 1,
		From: "untrusted.main", To: systemruntime.Address, ReplyTo: "untrusted.main",
		Payload: mustRuntimeJSON(call),
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.HandleNext(ctx, systemruntime.Address); err != nil || result.Status != "committed" || len(result.EffectIDs) != 1 {
		t.Fatalf("system handle result=%#v err=%v", result, err)
	}
	if result, err := runtime.DispatchNextEffect(ctx); err != nil || result.Status != "succeeded" {
		t.Fatalf("dispatch rejection result=%#v err=%v", result, err)
	}
	if result, err := runtime.HandleNext(ctx, "untrusted.main"); err != nil || result.Status != "committed" {
		t.Fatalf("untrusted reply result=%#v err=%v", result, err)
	}
	reply := <-results
	if reply.Metadata[contract.MetadataReplyError] != "true" || reply.Metadata[systemruntime.MetadataCallID] != "unauthorized-1" {
		t.Fatalf("rejection reply = %#v", reply)
	}
}

func registerSystemTestServices(t *testing.T, builder *Builder, results chan contract.Message) {
	t.Helper()
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: systemCallerComponent, Scope: building.ScopeMounted,
		Factory: service.FactoryFunc(func(_ context.Context, request service.CreateRequest) (service.Service, error) {
			return &systemCallerService{address: request.Address, results: results}, nil
		}),
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: "test.spawn", Version: 1},
			{Kind: contract.MessageReply, Type: systemruntime.ResultMessageType, Version: 1},
		},
		Produces:         []building.MessageContract{{Kind: contract.MessageCommand, Type: systemruntime.CallMessageType, Version: 1}},
		SystemOperations: []string{assembly.SystemOperationDeclareInstance},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: systemUntrustedComponent, Scope: building.ScopeMounted,
		Factory: service.FactoryFunc(func(_ context.Context, request service.CreateRequest) (service.Service, error) {
			return &systemCallerService{address: request.Address, results: results}, nil
		}),
		Consumes: []building.MessageContract{{Kind: contract.MessageReply, Type: systemruntime.ResultMessageType, Version: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: systemWorkerComponent, Scope: building.ScopeVirtual,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return systemWorkerService{}, nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
}

func runSystemSpawn(t *testing.T, ctx context.Context, runtime *Runtime, messageID string, declaration instance.Declaration) {
	t.Helper()
	if _, err := runtime.Publish(ctx, contract.Message{
		ID: messageID, Kind: contract.MessageCommand, Type: "test.spawn", Version: 1,
		To: "supervisor.main", Payload: mustRuntimeJSON(declaration),
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.HandleNext(ctx, "supervisor.main"); err != nil || result.Status != "committed" {
		t.Fatalf("supervisor spawn result=%#v err=%v", result, err)
	}
	if result, err := runtime.DispatchNextOutbox(ctx); err != nil || result.Delivered != 1 {
		t.Fatalf("dispatch system call result=%#v err=%v", result, err)
	}
	if result, err := runtime.HandleNext(ctx, systemruntime.Address); err != nil || result.Status != "committed" || len(result.EffectIDs) != 1 {
		t.Fatalf("system call result=%#v err=%v", result, err)
	}
	if result, err := runtime.DispatchNextEffect(ctx); err != nil || result.Status != "succeeded" {
		t.Fatalf("dispatch system result=%#v err=%v", result, err)
	}
	if result, err := runtime.HandleNext(ctx, "supervisor.main"); err != nil || result.Status != "committed" {
		t.Fatalf("supervisor result=%#v err=%v", result, err)
	}
}

func assertSystemSuccess(t *testing.T, message contract.Message, wantInstanceID contract.ServiceInstanceID) {
	t.Helper()
	if message.Metadata[contract.MetadataReplyError] == "true" {
		t.Fatalf("unexpected system error reply: %s", message.Payload)
	}
	var result systemruntime.Result
	if err := json.Unmarshal(message.Payload, &result); err != nil {
		t.Fatal(err)
	}
	var declared systemruntime.DeclareInstanceResult
	if err := json.Unmarshal(result.Result, &declared); err != nil {
		t.Fatal(err)
	}
	if declared.Instance.InstanceID != wantInstanceID {
		t.Fatalf("declared instance = %#v", declared.Instance)
	}
}

func mustRuntimeJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

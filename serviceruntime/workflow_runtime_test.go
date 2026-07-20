package serviceruntime

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	sqlitestore "agent/serviceruntime/persistence/sqlite"
	"agent/serviceruntime/request"
	"agent/serviceruntime/service"
	"agent/serviceruntime/workflow"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	workflowCallerRef = contract.ComponentRef{Type: "test.workflow-caller", Version: "v1"}
	workflowValueRef  = contract.ComponentRef{Type: "test.workflow-value", Version: "v1"}
)

type workflowCallerService struct {
	runs *int
}

func (workflowCallerService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: workflowCallerRef}
}

func (workflowCallerService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (s workflowCallerService) Handle(ctx context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Type != "workflow.calculate" {
		return service.Decision{}, fmt.Errorf("unsupported workflow caller message %q", message.Type)
	}
	if s.runs != nil {
		*s.runs++
	}
	var input struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}
	var downstream struct {
		Value int `json:"value"`
	}
	if err := request.QueryKey(ctx, "double-value", "workflow-value.main", "workflow-value.double", input, &downstream); err != nil {
		return service.Decision{}, err
	}
	payload, err := json.Marshal(map[string]int{"result": downstream.Value + 1})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Reply: &service.Reply{
		Key: "workflow-calculated", Type: "workflow.calculated", Version: 1, Payload: payload,
	}}, nil
}

func (workflowCallerService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

type workflowValueService struct {
	handled chan<- contract.Message
}

func (workflowValueService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: workflowValueRef}
}

func (workflowValueService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (s workflowValueService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Type != "workflow-value.double" {
		return service.Decision{}, fmt.Errorf("unsupported workflow value message %q", message.Type)
	}
	var input struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}
	if s.handled != nil {
		s.handled <- message.Clone()
	}
	payload, err := json.Marshal(map[string]int{"value": input.Value * 2})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Reply: &service.Reply{
		Key: "workflow-doubled", Type: "workflow-value.doubled", Version: 1, Payload: payload,
	}}, nil
}

func (workflowValueService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

func TestWorkflowAdapterKeepsSequentialCodeAndPublishesAfterCommit(t *testing.T) {
	ctx := context.Background()
	handled := make(chan contract.Message, 1)
	runs := 0
	builder := newWorkflowTestBuilder(t, nil, "workflow-test", &runs, handled)
	runtime, err := builder.Build(ctx, workflowTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	serveCtx, stopServing := context.WithCancel(ctx)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- runtime.ServeWithOptions(serveCtx, ServeOptions{PollInterval: time.Millisecond})
	}()

	callCtx, cancelCall := context.WithTimeout(ctx, 3*time.Second)
	defer cancelCall()
	var response struct {
		Result int `json:"result"`
	}
	client := runtime.RequestClient("workflow-test.client")
	if client == nil {
		t.Fatal("runtime request client is nil")
	}
	if err := client.Command(callCtx, "workflow-caller.main", "workflow.calculate", map[string]int{"value": 20}, &response); err != nil {
		t.Fatal(err)
	}
	if response.Result != 41 {
		t.Fatalf("response result = %d, want 41", response.Result)
	}
	if runs != 2 {
		t.Fatalf("workflow handler runs = %d, want 2 (initial run plus deterministic replay)", runs)
	}
	select {
	case nested := <-handled:
		if nested.From != "workflow-caller.main" || nested.ReplyTo != "workflow-caller.main" {
			t.Fatalf("workflow request routing = from %q reply_to %q", nested.From, nested.ReplyTo)
		}
		if !strings.HasPrefix(string(nested.StreamID), "$workflow/workflow-caller.main/") {
			t.Fatalf("workflow request stream = %q, want an isolated workflow stream", nested.StreamID)
		}
	default:
		t.Fatal("workflow value service did not handle the nested request")
	}

	stopServing()
	if err := <-serveResult; err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowAdapterResumesAfterSQLiteReopenWithoutRepeatingDownstreamCall(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "workflow.db")
	handled := make(chan contract.Message, 2)
	firstRuns := 0

	firstStore, err := sqlitestore.Open(ctx, path, sqlitestore.Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	firstBuilder := newWorkflowTestBuilder(t, firstStore, "workflow-restart", &firstRuns, handled)
	first, err := firstBuilder.Build(ctx, workflowTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Publish(ctx, contract.Message{
		ID: "workflow-outer-1", Kind: contract.MessageCommand, Type: "workflow.calculate", Version: 1,
		To: "workflow-caller.main", ReplyTo: request.DefaultReplyAddress,
		Payload: json.RawMessage(`{"value":20}`),
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := first.HandleNext(ctx, "workflow-caller.main"); err != nil || result.Status != "committed" {
		t.Fatalf("suspend workflow result=%#v err=%v", result, err)
	}
	if _, err := first.DispatchNextOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	if result, err := first.HandleNext(ctx, "workflow-value.main"); err != nil || result.Status != "committed" {
		t.Fatalf("downstream result=%#v err=%v", result, err)
	}
	if _, err := first.DispatchNextOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	if firstRuns != 1 {
		t.Fatalf("workflow runs before restart = %d, want 1", firstRuns)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := sqlitestore.Open(ctx, path, sqlitestore.Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedStore.Close()
	secondRuns := 0
	secondBuilder := newWorkflowTestBuilder(t, reopenedStore, "workflow-restart", &secondRuns, handled)
	second, err := secondBuilder.Build(ctx, workflowTestManifest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	result, err := second.HandleNext(ctx, "workflow-caller.main")
	if err != nil || result.Status != "committed" {
		t.Fatalf("resume workflow result=%#v err=%v", result, err)
	}
	if secondRuns != 1 {
		t.Fatalf("workflow replay runs after restart = %d, want 1", secondRuns)
	}
	select {
	case <-handled:
	default:
		t.Fatal("downstream request was not handled before restart")
	}
	select {
	case duplicate := <-handled:
		t.Fatalf("downstream request was repeated after restart: %#v", duplicate)
	default:
	}
}

func newWorkflowTestBuilder(t *testing.T, storage persistence.RuntimeStorage, ownerID string, runs *int, handled chan<- contract.Message) *Builder {
	t.Helper()
	builder, err := NewBuilder(BuilderOptions{Storage: storage, IDs: StableIDs{}, OwnerID: ownerID})
	if err != nil {
		t.Fatal(err)
	}
	callerFactory := workflow.WrapFactory(service.FactoryFunc(func(_ context.Context, create service.CreateRequest) (service.Service, error) {
		if create.Requests == nil {
			return nil, fmt.Errorf("request client was not injected")
		}
		return workflowCallerService{runs: runs}, nil
	}))
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: workflowCallerRef, Scope: building.ScopeMounted, Factory: callerFactory,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: "workflow.calculate", Version: 1},
			{Kind: contract.MessageReply, Type: "workflow-value.doubled", Version: 1},
		},
		Produces: []building.MessageContract{
			{Kind: contract.MessageQuery, Type: "workflow-value.double", Version: 1},
			{Kind: contract.MessageReply, Type: "workflow.calculated", Version: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: workflowValueRef, Scope: building.ScopeMounted,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return workflowValueService{handled: handled}, nil
		}),
		Consumes: []building.MessageContract{{Kind: contract.MessageQuery, Type: "workflow-value.double", Version: 1}},
		Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: "workflow-value.doubled", Version: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	return builder
}

func workflowTestManifest() building.RuntimeManifest {
	return building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "workflow-runtime", Revision: "v1"},
		Services: []building.ServiceMount{
			{Address: "workflow-caller.main", Component: workflowCallerRef},
			{Address: "workflow-value.main", Component: workflowValueRef},
		},
	}
}

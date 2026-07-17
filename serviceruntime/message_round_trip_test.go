package serviceruntime

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/request"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

var (
	requestRoundTripARef = contract.ComponentRef{Type: "test.request-roundtrip.a", Version: "v1"}
	requestRoundTripBRef = contract.ComponentRef{Type: "test.request-roundtrip.b", Version: "v1"}
	requestRoundTripCRef = contract.ComponentRef{Type: "test.request-roundtrip.c", Version: "v1"}
)

type requestRoundTripInput struct {
	RequestID string `json:"request_id"`
	Value     int    `json:"value"`
}

type requestRoundTripReply struct {
	RequestID string `json:"request_id"`
	Value     int    `json:"value"`
	Path      string `json:"path"`
}

type requestRoundTripAState struct {
	Completed int `json:"completed"`
}

type requestRoundTripAService struct{}

func (requestRoundTripAService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: requestRoundTripARef}
}

func (requestRoundTripAService) InitialState(context.Context, service.Init) (service.State, error) {
	data, err := json.Marshal(requestRoundTripAState{})
	if err != nil {
		return service.State{}, err
	}
	return service.State{SchemaVersion: 1, Data: data}, nil
}

func (requestRoundTripAService) Handle(ctx context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Type != "request-roundtrip.start" {
		return service.Decision{}, fmt.Errorf("service A does not handle %q", message.Type)
	}
	var input requestRoundTripInput
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}

	var replyFromB requestRoundTripReply
	if err := request.Command(ctx, "request-roundtrip.b", "request-roundtrip.a-to-b", input, &replyFromB); err != nil {
		return service.Decision{}, fmt.Errorf("service A request to B: %w", err)
	}
	if replyFromB.RequestID != input.RequestID || replyFromB.Path != "c->b" {
		return service.Decision{}, fmt.Errorf("service A received unexpected reply from B: %#v", replyFromB)
	}

	payload, err := json.Marshal(requestRoundTripReply{
		RequestID: input.RequestID,
		Value:     replyFromB.Value + 1,
		Path:      replyFromB.Path + "->a",
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{{Key: "completed", Type: "request-roundtrip.a.completed", Version: 1, Payload: payload}},
		Reply:  &service.Reply{Key: "return-to-client", Type: "request-roundtrip.a.result", Version: 1, Payload: payload},
	}, nil
}

func (requestRoundTripAService) Apply(state service.State, event contract.StoredEvent) (service.State, error) {
	if event.EventType != "request-roundtrip.a.completed" {
		return service.State{}, fmt.Errorf("service A cannot apply %q", event.EventType)
	}
	var next requestRoundTripAState
	if err := json.Unmarshal(state.Data, &next); err != nil {
		return service.State{}, err
	}
	next.Completed++
	data, err := json.Marshal(next)
	if err != nil {
		return service.State{}, err
	}
	return service.State{SchemaVersion: 1, Data: data}, nil
}

type requestRoundTripBState struct {
	Completed int `json:"completed"`
}

type requestRoundTripBService struct{}

func (requestRoundTripBService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: requestRoundTripBRef}
}

func (requestRoundTripBService) InitialState(context.Context, service.Init) (service.State, error) {
	data, err := json.Marshal(requestRoundTripBState{})
	if err != nil {
		return service.State{}, err
	}
	return service.State{SchemaVersion: 1, Data: data}, nil
}

func (requestRoundTripBService) Handle(ctx context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Type != "request-roundtrip.a-to-b" {
		return service.Decision{}, fmt.Errorf("service B does not handle %q", message.Type)
	}
	if message.From != "request-roundtrip.a" {
		return service.Decision{}, fmt.Errorf("service B received a request from %q, want %q", message.From, "request-roundtrip.a")
	}
	var input requestRoundTripInput
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}

	var replyFromC requestRoundTripReply
	if err := request.Command(ctx, "request-roundtrip.c", "request-roundtrip.b-to-c", input, &replyFromC); err != nil {
		return service.Decision{}, fmt.Errorf("service B request to C: %w", err)
	}
	if replyFromC.RequestID != input.RequestID || replyFromC.Path != "c" {
		return service.Decision{}, fmt.Errorf("service B received unexpected reply from C: %#v", replyFromC)
	}

	payload, err := json.Marshal(requestRoundTripReply{
		RequestID: input.RequestID,
		Value:     replyFromC.Value + 1,
		Path:      replyFromC.Path + "->b",
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{{Key: "completed", Type: "request-roundtrip.b.completed", Version: 1, Payload: payload}},
		Reply:  &service.Reply{Key: "return-to-a", Type: "request-roundtrip.b.result", Version: 1, Payload: payload},
	}, nil
}

func (requestRoundTripBService) Apply(state service.State, event contract.StoredEvent) (service.State, error) {
	if event.EventType != "request-roundtrip.b.completed" {
		return service.State{}, fmt.Errorf("service B cannot apply %q", event.EventType)
	}
	var next requestRoundTripBState
	if err := json.Unmarshal(state.Data, &next); err != nil {
		return service.State{}, err
	}
	next.Completed++
	data, err := json.Marshal(next)
	if err != nil {
		return service.State{}, err
	}
	return service.State{SchemaVersion: 1, Data: data}, nil
}

type requestRoundTripCState struct {
	Processed int `json:"processed"`
}

type requestRoundTripCService struct{}

func (requestRoundTripCService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: requestRoundTripCRef}
}

func (requestRoundTripCService) InitialState(context.Context, service.Init) (service.State, error) {
	data, err := json.Marshal(requestRoundTripCState{})
	if err != nil {
		return service.State{}, err
	}
	return service.State{SchemaVersion: 1, Data: data}, nil
}

func (requestRoundTripCService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Type != "request-roundtrip.b-to-c" {
		return service.Decision{}, fmt.Errorf("service C does not handle %q", message.Type)
	}
	if message.From != "request-roundtrip.b" {
		return service.Decision{}, fmt.Errorf("service C received a request from %q, want %q", message.From, "request-roundtrip.b")
	}
	var input requestRoundTripInput
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}
	payload, err := json.Marshal(requestRoundTripReply{RequestID: input.RequestID, Value: input.Value * 2, Path: "c"})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{
		Events: []service.NewEvent{{Key: "processed", Type: "request-roundtrip.c.processed", Version: 1, Payload: payload}},
		Reply:  &service.Reply{Key: "return-to-b", Type: "request-roundtrip.c.result", Version: 1, Payload: payload},
	}, nil
}

func (requestRoundTripCService) Apply(state service.State, event contract.StoredEvent) (service.State, error) {
	if event.EventType != "request-roundtrip.c.processed" {
		return service.State{}, fmt.Errorf("service C cannot apply %q", event.EventType)
	}
	var next requestRoundTripCState
	if err := json.Unmarshal(state.Data, &next); err != nil {
		return service.State{}, err
	}
	next.Processed++
	data, err := json.Marshal(next)
	if err != nil {
		return service.State{}, err
	}
	return service.State{SchemaVersion: 1, Data: data}, nil
}

func TestRequestRoundTripAcrossThreeServices(t *testing.T) {
	ctx := context.Background()
	store := memory.New(nil)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})

	builder, err := NewBuilder(BuilderOptions{
		Storage: store, IDs: StableIDs{}, OwnerID: "request-roundtrip-test", RequestTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	definitions := []building.ServiceDefinition{
		{
			Component: requestRoundTripARef,
			Scope:     building.ScopeMounted,
			Factory: service.FactoryFunc(func(_ context.Context, create service.CreateRequest) (service.Service, error) {
				if create.Requests == nil {
					return nil, fmt.Errorf("service A request client was not injected")
				}
				return requestRoundTripAService{}, nil
			}),
			Consumes: []building.MessageContract{{Kind: contract.MessageCommand, Type: "request-roundtrip.start", Version: 1}},
			Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: "request-roundtrip.a.result", Version: 1}},
		},
		{
			Component: requestRoundTripBRef,
			Scope:     building.ScopeMounted,
			Factory: service.FactoryFunc(func(_ context.Context, create service.CreateRequest) (service.Service, error) {
				if create.Requests == nil {
					return nil, fmt.Errorf("service B request client was not injected")
				}
				return requestRoundTripBService{}, nil
			}),
			Consumes: []building.MessageContract{{Kind: contract.MessageCommand, Type: "request-roundtrip.a-to-b", Version: 1}},
			Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: "request-roundtrip.b.result", Version: 1}},
		},
		{
			Component: requestRoundTripCRef,
			Scope:     building.ScopeMounted,
			Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
				return requestRoundTripCService{}, nil
			}),
			Consumes: []building.MessageContract{{Kind: contract.MessageCommand, Type: "request-roundtrip.b-to-c", Version: 1}},
			Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: "request-roundtrip.c.result", Version: 1}},
		},
	}
	for _, definition := range definitions {
		if err := builder.RegisterService(definition); err != nil {
			t.Fatal(err)
		}
	}

	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "request-roundtrip-runtime", Revision: "v1"},
		Services: []building.ServiceMount{
			{Address: "request-roundtrip.a", Component: requestRoundTripARef},
			{Address: "request-roundtrip.b", Component: requestRoundTripBRef},
			{Address: "request-roundtrip.c", Component: requestRoundTripCRef},
		},
		Recovery: building.RecoveryPolicy{SnapshotEveryEvents: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	})
	if report, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	} else if report.InstancesActivated != 3 {
		t.Fatalf("activated instances = %d, want 3", report.InstancesActivated)
	}

	serveCtx, stopServing := context.WithCancel(ctx)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- runtime.ServeWithOptions(serveCtx, ServeOptions{PollInterval: time.Millisecond})
	}()
	t.Cleanup(func() {
		stopServing()
		if err := <-serveResult; err != nil {
			t.Error(err)
		}
	})

	client := runtime.RequestClient("request-roundtrip.client")
	if client == nil {
		t.Fatal("runtime request client is nil")
	}
	callCtx, cancelCall := context.WithTimeout(ctx, 3*time.Second)
	defer cancelCall()
	var response requestRoundTripReply
	if err := client.Command(callCtx, "request-roundtrip.a", "request-roundtrip.start", requestRoundTripInput{
		RequestID: "request-roundtrip-1",
		Value:     20,
	}, &response); err != nil {
		t.Fatal(err)
	}
	if response != (requestRoundTripReply{RequestID: "request-roundtrip-1", Value: 42, Path: "c->b->a"}) {
		t.Fatalf("round-trip response = %#v", response)
	}

	aSequence, aData := requestRoundTripSnapshot(t, ctx, store, "request-roundtrip-runtime", "v1", "request-roundtrip.a")
	var aState requestRoundTripAState
	if err := json.Unmarshal(aData, &aState); err != nil {
		t.Fatal(err)
	}
	if aSequence != 1 || aState != (requestRoundTripAState{Completed: 1}) {
		t.Fatalf("service A state = sequence %d, %#v; want sequence 1, %#v", aSequence, aState, requestRoundTripAState{Completed: 1})
	}

	bSequence, bData := requestRoundTripSnapshot(t, ctx, store, "request-roundtrip-runtime", "v1", "request-roundtrip.b")
	var bState requestRoundTripBState
	if err := json.Unmarshal(bData, &bState); err != nil {
		t.Fatal(err)
	}
	if bSequence != 1 || bState != (requestRoundTripBState{Completed: 1}) {
		t.Fatalf("service B state = sequence %d, %#v; want sequence 1, %#v", bSequence, bState, requestRoundTripBState{Completed: 1})
	}

	cSequence, cData := requestRoundTripSnapshot(t, ctx, store, "request-roundtrip-runtime", "v1", "request-roundtrip.c")
	var cState requestRoundTripCState
	if err := json.Unmarshal(cData, &cState); err != nil {
		t.Fatal(err)
	}
	if cSequence != 1 || cState != (requestRoundTripCState{Processed: 1}) {
		t.Fatalf("service C state = sequence %d, %#v; want sequence 1, %#v", cSequence, cState, requestRoundTripCState{Processed: 1})
	}
}

func requestRoundTripSnapshot(t *testing.T, ctx context.Context, store persistence.RuntimeStorage, runtimeID contract.RuntimeID, revision contract.PlanRevision, address contract.ServiceAddress) (uint64, json.RawMessage) {
	t.Helper()
	record, found, err := store.Instances().GetByAddress(ctx, runtimeID, revision, address)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("instance at %q was not found", address)
	}
	snapshot, found, err := store.Snapshots().LoadLatest(ctx, record.StateStreamID)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("snapshot for %q was not found", address)
	}
	return snapshot.LastSequence, snapshot.State
}

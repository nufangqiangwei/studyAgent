package serviceruntime

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/request"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

var (
	requestCallerRef = contract.ComponentRef{Type: "test.request-caller", Version: "v1"}
	requestValueRef  = contract.ComponentRef{Type: "test.request-value", Version: "v1"}
)

type requestCallerService struct{}

func (requestCallerService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: requestCallerRef}
}

func (requestCallerService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (s requestCallerService) Handle(ctx context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Type != "caller.calculate" {
		return service.Decision{}, fmt.Errorf("unsupported caller message %q", message.Type)
	}
	var input struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}
	var valueResponse struct {
		Value int `json:"value"`
	}
	// This is intentionally synchronous-looking service code. Runtime.Serve is
	// responsible for running the target service and delivering its reply.
	if err := request.Query(ctx, "value.main", "value.double", input, &valueResponse); err != nil {
		return service.Decision{}, err
	}
	payload, err := json.Marshal(map[string]int{"result": valueResponse.Value + 1})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Reply: &service.Reply{
		Key: "calculated", Type: "caller.calculated", Version: 1, Payload: payload,
	}}, nil
}

func (requestCallerService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

type requestValueService struct {
	handled chan<- contract.Message
}

func (requestValueService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: requestValueRef}
}

func (requestValueService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (s requestValueService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Type != "value.double" {
		return service.Decision{}, fmt.Errorf("unsupported value message %q", message.Type)
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
		Key: "doubled", Type: "value.doubled", Version: 1, Payload: payload,
	}}, nil
}

func (requestValueService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

func TestServiceHandleCanSynchronouslyRequestAnotherService(t *testing.T) {
	ctx := context.Background()
	handled := make(chan contract.Message, 1)
	builder, err := NewBuilder(BuilderOptions{IDs: StableIDs{}, OwnerID: "request-test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: requestCallerRef, Scope: building.ScopeMounted,
		Factory: service.FactoryFunc(func(_ context.Context, create service.CreateRequest) (service.Service, error) {
			if create.Requests == nil {
				return nil, fmt.Errorf("request client was not injected")
			}
			return requestCallerService{}, nil
		}),
		Consumes: []building.MessageContract{{Kind: contract.MessageCommand, Type: "caller.calculate", Version: 1}},
		Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: "caller.calculated", Version: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: requestValueRef, Scope: building.ScopeMounted,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return requestValueService{handled: handled}, nil
		}),
		Consumes: []building.MessageContract{{Kind: contract.MessageQuery, Type: "value.double", Version: 1}},
		Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: "value.doubled", Version: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "request-runtime", Revision: "v1"},
		Services: []building.ServiceMount{
			{Address: "caller.main", Component: requestCallerRef},
			{Address: "value.main", Component: requestValueRef},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}

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
	client := runtime.RequestClient("test.client")
	if client == nil {
		t.Fatal("runtime request client is nil")
	}
	if err := client.Command(callCtx, "caller.main", "caller.calculate", map[string]int{"value": 20}, &response); err != nil {
		t.Fatal(err)
	}
	if response.Result != 41 {
		t.Fatalf("response result = %d, want 41", response.Result)
	}
	select {
	case nested := <-handled:
		if nested.From != "caller.main" || nested.ReplyTo != request.DefaultReplyAddress {
			t.Fatalf("nested request routing = from %q reply_to %q", nested.From, nested.ReplyTo)
		}
		if nested.CausationID == "" || nested.CorrelationID == nested.ID {
			t.Fatalf("nested request did not inherit parent causation/correlation: %#v", nested)
		}
	default:
		t.Fatal("value service did not handle the nested request")
	}

	stopServing()
	if err := <-serveResult; err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

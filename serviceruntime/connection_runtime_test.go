package serviceruntime

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/connection"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	connectionOwnerOperation contract.MessageType = "test.connection-owner.operation"
	connectionOwnerReply     contract.MessageType = "test.connection-owner.reply"
)

var connectionOwnerComponent = contract.ComponentRef{Type: "test-connection-owner", Version: "v1"}

type connectionOwnerRequest struct {
	Action string                  `json:"action"`
	Open   connection.OpenRequest  `json:"open,omitempty"`
	Send   connection.SendRequest  `json:"send,omitempty"`
	Close  connection.CloseRequest `json:"close,omitempty"`
	Get    connection.GetRequest   `json:"get,omitempty"`
}

type connectionOwnerResponse struct {
	Info  connection.Info `json:"info,omitempty"`
	Error string          `json:"error,omitempty"`
}

type connectionOwnerService struct {
	address contract.ServiceAddress
	inbound chan connection.InboundEvent
}

func (s *connectionOwnerService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: connectionOwnerComponent}
}

func (s *connectionOwnerService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1}, nil
}

func (s *connectionOwnerService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

func (s *connectionOwnerService) Handle(ctx context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Kind == contract.MessageEvent {
		var input connection.InboundEvent
		if err := json.Unmarshal(message.Payload, &input); err != nil {
			return service.Decision{}, err
		}
		select {
		case s.inbound <- input:
		default:
		}
		return service.Decision{}, nil
	}
	var input connectionOwnerRequest
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}
	response := connectionOwnerResponse{}
	switch input.Action {
	case "open":
		response.Info, response.Error = connectionResult(connection.Open(ctx, input.Open))
	case "send":
		response.Error = errorText(connection.Send(ctx, input.Send))
	case "close":
		response.Error = errorText(connection.Close(ctx, input.Close))
	case "get":
		response.Info, response.Error = connectionResult(connection.Get(ctx, input.Get))
	default:
		response.Error = fmt.Sprintf("unknown action %q", input.Action)
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Reply: &service.Reply{
		Key: "owner-result", Type: connectionOwnerReply, Version: 1, Payload: payload,
	}}, nil
}

func connectionResult(info connection.Info, err error) (connection.Info, string) {
	return info, errorText(err)
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type testManagedDriver struct {
	mu       sync.Mutex
	opens    int
	closes   int
	sessions map[string]*testManagedSession
}

type testManagedSession struct {
	driver *testManagedDriver
	id     string
	emit   connection.EmitFunc
	sent   chan connection.Frame
}

func newTestManagedDriver() *testManagedDriver {
	return &testManagedDriver{sessions: make(map[string]*testManagedSession)}
}

func (d *testManagedDriver) Open(_ context.Context, input connection.DriverOpenRequest, emit connection.EmitFunc) (connection.Session, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opens++
	session := &testManagedSession{driver: d, id: input.ConnectionID, emit: emit, sent: make(chan connection.Frame, 4)}
	d.sessions[input.ConnectionID] = session
	return session, nil
}

func (s *testManagedSession) Send(_ context.Context, frame connection.Frame) error {
	s.sent <- frame
	return nil
}

func (s *testManagedSession) Close(context.Context) error {
	s.driver.mu.Lock()
	s.driver.closes++
	delete(s.driver.sessions, s.id)
	s.driver.mu.Unlock()
	return nil
}

func (d *testManagedDriver) session(connectionID string) *testManagedSession {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[connectionID]
}

func (d *testManagedDriver) openCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.opens
}

func TestConnectionManagerUsesBusAndEnforcesServiceOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clock := fixedClock{now: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)}
	store := memory.New(&clock)
	driver := newTestManagedDriver()
	inbound := map[contract.ServiceAddress]chan connection.InboundEvent{
		"owner.a": make(chan connection.InboundEvent, 2),
		"owner.b": make(chan connection.InboundEvent, 2),
	}
	runtime := newConnectionTestRuntime(t, ctx, store, driver, inbound, "connection-owner-node-1")
	if mounted, found := runtime.Plan().Service(connection.ManagerAddress); !found || mounted.Component != connection.ManagerComponent {
		t.Fatalf("runtime connection manager mount = %#v, found=%v", mounted, found)
	}
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	serveCtx, stopServe := context.WithCancel(ctx)
	serveDone := make(chan error, 1)
	go func() { serveDone <- runtime.Serve(serveCtx) }()

	client := runtime.RequestClient("test.client")
	var opened connectionOwnerResponse
	if err := client.Command(ctx, "owner.a", connectionOwnerOperation, connectionOwnerRequest{
		Action: "open", Open: connection.OpenRequest{Key: "primary", Driver: "test", Config: json.RawMessage(`{"endpoint":"example"}`)},
	}, &opened); err != nil {
		t.Fatal(err)
	}
	if opened.Error != "" || opened.Info.ConnectionID == "" || opened.Info.Status != persistence.ConnectionOpen {
		t.Fatalf("open response = %#v", opened)
	}
	if driver.openCount() != 1 {
		t.Fatalf("driver opens = %d, want 1", driver.openCount())
	}

	var denied connectionOwnerResponse
	if err := client.Command(ctx, "owner.b", connectionOwnerOperation, connectionOwnerRequest{
		Action: "get", Get: connection.GetRequest{ConnectionID: opened.Info.ConnectionID},
	}, &denied); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(denied.Error, "another service") {
		t.Fatalf("cross-service get error = %q", denied.Error)
	}

	var sent connectionOwnerResponse
	if err := client.Command(ctx, "owner.a", connectionOwnerOperation, connectionOwnerRequest{
		Action: "send", Send: connection.SendRequest{ConnectionID: opened.Info.ConnectionID, Data: []byte("hello")},
	}, &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Error != "" {
		t.Fatalf("owner send error = %q", sent.Error)
	}
	session := driver.session(opened.Info.ConnectionID)
	if session == nil {
		t.Fatal("driver session was not retained")
	}
	select {
	case frame := <-session.sent:
		if string(frame.Data) != "hello" || frame.ID == "" {
			t.Fatalf("sent frame = %#v", frame)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for driver send")
	}

	if err := session.emit(ctx, connection.Event{Kind: connection.EventData, Data: []byte("world")}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-inbound["owner.a"]:
		if event.ConnectionID != opened.Info.ConnectionID || string(event.Data) != "world" {
			t.Fatalf("owner inbound event = %#v", event)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for owner inbound event")
	}
	select {
	case event := <-inbound["owner.b"]:
		t.Fatalf("other service received private connection event: %#v", event)
	case <-time.After(50 * time.Millisecond):
	}

	stopServe()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionManagerRestoresDesiredConnectionsWhenRuntimeIsRebuilt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clock := fixedClock{now: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)}
	store := memory.New(&clock)
	inbound := map[contract.ServiceAddress]chan connection.InboundEvent{"owner.a": make(chan connection.InboundEvent, 1), "owner.b": make(chan connection.InboundEvent, 1)}
	firstDriver := newTestManagedDriver()
	first := newConnectionTestRuntime(t, ctx, store, firstDriver, inbound, "connection-recovery-node-1")
	if _, err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	serveCtx, stopServe := context.WithCancel(ctx)
	serveDone := make(chan error, 1)
	go func() { serveDone <- first.Serve(serveCtx) }()
	var opened connectionOwnerResponse
	if err := first.RequestClient("test.client").Command(ctx, "owner.a", connectionOwnerOperation, connectionOwnerRequest{
		Action: "open", Open: connection.OpenRequest{Key: "recover-me", Driver: "test"},
	}, &opened); err != nil {
		t.Fatal(err)
	}
	stopServe()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	secondDriver := newTestManagedDriver()
	second := newConnectionTestRuntime(t, ctx, store, secondDriver, inbound, "connection-recovery-node-2")
	report, err := second.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.ConnectionsRestored != 1 || report.ConnectionsFailed != 0 {
		t.Fatalf("connection recovery report = %#v", report)
	}
	if secondDriver.openCount() != 1 || secondDriver.session(opened.Info.ConnectionID) == nil {
		t.Fatalf("restored driver opens = %d, connection = %#v", secondDriver.openCount(), opened.Info)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func newConnectionTestRuntime(
	t *testing.T,
	ctx context.Context,
	store persistence.RuntimeStorage,
	driver connection.Driver,
	inbound map[contract.ServiceAddress]chan connection.InboundEvent,
	ownerID string,
) *Runtime {
	t.Helper()
	builder, err := NewBuilder(BuilderOptions{Storage: store, IDs: StableIDs{}, OwnerID: ownerID})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterConnectionDriver("test", driver); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: connectionOwnerComponent,
		Factory: service.FactoryFunc(func(_ context.Context, create service.CreateRequest) (service.Service, error) {
			return &connectionOwnerService{address: create.Address, inbound: inbound[create.Address]}, nil
		}),
		Scope: building.ScopeMounted,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: connectionOwnerOperation, Version: 1},
			{Kind: contract.MessageEvent, Type: connection.DataEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: connection.ClosedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: connection.ErrorEventType, Version: 1},
		},
		Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: connectionOwnerReply, Version: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "connection-runtime", Revision: "v1"},
		Services: []building.ServiceMount{
			{Address: "owner.a", Component: connectionOwnerComponent},
			{Address: "owner.b", Component: connectionOwnerComponent},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

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
	"strings"
	"sync"
	"testing"
	"time"
)

var connectionOwnerComponent = contract.ComponentRef{Type: "test-connection-owner", Version: "v1"}

type connectionOwnerService struct {
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

func (s *connectionOwnerService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
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

	publishConnectionManagerMessage(t, ctx, runtime, "owner.a", connection.OpenMessageType, connection.OpenRequest{
		Key: "primary", Driver: "test", Config: json.RawMessage(`{"endpoint":"example"}`),
	})
	if result, err := runtime.HandleNext(ctx, connection.ManagerAddress); err != nil || result.Status != "committed" {
		t.Fatalf("open connection result=%#v err=%v", result, err)
	}
	records, err := store.Connections().List(ctx, "connection-runtime")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Status != persistence.ConnectionOpen {
		t.Fatalf("connection records = %#v", records)
	}
	connectionID := records[0].ConnectionID
	if driver.openCount() != 1 {
		t.Fatalf("driver opens = %d, want 1", driver.openCount())
	}

	publishConnectionManagerMessage(t, ctx, runtime, "owner.a", connection.SendMessageType, connection.SendRequest{
		ConnectionID: connectionID, Data: []byte("hello"),
	})
	if result, err := runtime.HandleNext(ctx, connection.ManagerAddress); err != nil || result.Status != "committed" {
		t.Fatalf("send connection result=%#v err=%v", result, err)
	}
	session := driver.session(connectionID)
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
	if result, err := runtime.HandleNext(ctx, "owner.a"); err != nil || result.Status != "committed" {
		t.Fatalf("owner inbound result=%#v err=%v", result, err)
	}
	select {
	case event := <-inbound["owner.a"]:
		if event.ConnectionID != connectionID || string(event.Data) != "world" {
			t.Fatalf("owner inbound event = %#v", event)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for owner inbound event")
	}
	select {
	case event := <-inbound["owner.b"]:
		t.Fatalf("other service received private connection event: %#v", event)
	default:
	}

	publishConnectionManagerMessage(t, ctx, runtime, "owner.b", connection.GetMessageType, connection.GetRequest{ConnectionID: connectionID})
	if _, err := runtime.HandleNext(ctx, connection.ManagerAddress); err == nil || !strings.Contains(err.Error(), "another service") {
		t.Fatalf("cross-service access error = %v", err)
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
	inbound := map[contract.ServiceAddress]chan connection.InboundEvent{
		"owner.a": make(chan connection.InboundEvent, 1),
		"owner.b": make(chan connection.InboundEvent, 1),
	}
	firstDriver := newTestManagedDriver()
	first := newConnectionTestRuntime(t, ctx, store, firstDriver, inbound, "connection-recovery-node-1")
	if _, err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	publishConnectionManagerMessage(t, ctx, first, "owner.a", connection.OpenMessageType, connection.OpenRequest{Key: "recover-me", Driver: "test"})
	if result, err := first.HandleNext(ctx, connection.ManagerAddress); err != nil || result.Status != "committed" {
		t.Fatalf("open connection result=%#v err=%v", result, err)
	}
	records, err := store.Connections().List(ctx, "connection-runtime")
	if err != nil || len(records) != 1 {
		t.Fatalf("connection records=%#v err=%v", records, err)
	}
	connectionID := records[0].ConnectionID
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
	if secondDriver.openCount() != 1 || secondDriver.session(connectionID) == nil {
		t.Fatalf("restored driver opens = %d, connection_id = %q", secondDriver.openCount(), connectionID)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func publishConnectionManagerMessage(t *testing.T, ctx context.Context, runtime *Runtime, from contract.ServiceAddress, messageType contract.MessageType, input any) {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	kind := contract.MessageCommand
	if messageType == connection.GetMessageType || messageType == connection.ListMessageType {
		kind = contract.MessageQuery
	}
	if _, err := runtime.Publish(ctx, contract.Message{
		Kind: kind, Type: messageType, Version: 1,
		From: from, To: connection.ManagerAddress, Payload: payload,
	}); err != nil {
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
			return &connectionOwnerService{inbound: inbound[create.Address]}, nil
		}),
		Scope: building.ScopeMounted,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageEvent, Type: connection.DataEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: connection.ClosedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: connection.ErrorEventType, Version: 1},
		},
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

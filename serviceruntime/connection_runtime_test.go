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

type connectionOwnerService struct{ inbound chan connection.InboundEvent }

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

func (d *testManagedDriver) closeCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closes
}

func TestConnectionModuleUsesEffectsAndDeliversTargetedEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clock := fixedClock{now: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)}
	store := memory.New(&clock)
	driver := newTestManagedDriver()
	inbound := map[contract.ServiceAddress]chan connection.InboundEvent{
		"owner.a": make(chan connection.InboundEvent, 4),
		"owner.b": make(chan connection.InboundEvent, 4),
	}
	runtime := newConnectionTestRuntime(t, ctx, store, driver, inbound, &clock, "connection-owner-node-1")
	if mounted, found := runtime.Plan().Service(connection.DefaultAddress); !found || mounted.Component != connection.ManagerComponent {
		t.Fatalf("explicit connection service mount = %#v, found=%v", mounted, found)
	}
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}

	publishConnectionMessage(t, ctx, runtime, "owner.a", connection.OpenMessageType, connection.OpenRequest{
		Key: "primary", Driver: "test", Config: json.RawMessage(`{"endpoint":"example"}`),
	})
	if result, err := runtime.HandleNext(ctx, connection.DefaultAddress); err != nil || result.Status != "committed" || len(result.EffectIDs) != 1 {
		t.Fatalf("plan open connection result=%#v err=%v", result, err)
	}
	if driver.openCount() != 0 {
		t.Fatalf("Service.Handle opened driver before effect commit: opens=%d", driver.openCount())
	}
	if result, err := runtime.DispatchNextEffect(ctx); err != nil || result.Status != persistence.EffectSucceeded {
		t.Fatalf("dispatch open effect result=%#v err=%v", result, err)
	}
	connectionID := StableIDs{}.Derive("connection", "connection-runtime", "v1", "owner.a", "primary")
	if driver.openCount() != 1 || driver.session(connectionID) == nil {
		t.Fatalf("driver opens=%d connection_id=%q", driver.openCount(), connectionID)
	}

	// DriverOpened entered the manager Inbox. Handling it atomically creates a
	// targeted public event in the Runtime Outbox.
	if result, err := runtime.HandleNext(ctx, connection.DefaultAddress); err != nil || result.Status != "committed" || len(result.OutboxIDs) != 1 {
		t.Fatalf("handle driver-opened event result=%#v err=%v", result, err)
	}
	dispatchAndHandleOwnerEvent(t, ctx, runtime, "owner.a")
	opened := receiveEvent(t, ctx, inbound["owner.a"])
	if opened.Kind != connection.EventOpened || opened.ConnectionID != connectionID {
		t.Fatalf("opened event = %#v", opened)
	}

	publishConnectionMessage(t, ctx, runtime, "owner.a", connection.SendMessageType, connection.SendRequest{
		ConnectionID: connectionID, Data: []byte("hello"),
	})
	if result, err := runtime.HandleNext(ctx, connection.DefaultAddress); err != nil || result.Status != "committed" || len(result.EffectIDs) != 1 {
		t.Fatalf("plan send connection result=%#v err=%v", result, err)
	}
	session := driver.session(connectionID)
	select {
	case frame := <-session.sent:
		t.Fatalf("Service.Handle sent frame before effect dispatch: %#v", frame)
	default:
	}
	if result, err := runtime.DispatchNextEffect(ctx); err != nil || result.Status != persistence.EffectSucceeded {
		t.Fatalf("dispatch send effect result=%#v err=%v", result, err)
	}
	select {
	case frame := <-session.sent:
		if string(frame.Data) != "hello" || frame.ID == "" {
			t.Fatalf("sent frame = %#v", frame)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for driver send")
	}

	if err := session.emit(ctx, connection.Event{ID: "remote-frame-1", Kind: connection.EventData, Data: []byte("world")}); err != nil {
		t.Fatal(err)
	}
	if result, err := runtime.HandleNext(ctx, connection.DefaultAddress); err != nil || result.Status != "committed" || len(result.OutboxIDs) != 1 {
		t.Fatalf("handle driver frame result=%#v err=%v", result, err)
	}
	dispatchAndHandleOwnerEvent(t, ctx, runtime, "owner.a")
	event := receiveEvent(t, ctx, inbound["owner.a"])
	if event.Kind != connection.EventData || event.ConnectionID != connectionID || event.FrameID != "remote-frame-1" || string(event.Data) != "world" {
		t.Fatalf("owner inbound event = %#v", event)
	}
	select {
	case event := <-inbound["owner.b"]:
		t.Fatalf("other service received private connection event: %#v", event)
	default:
	}

	publishConnectionMessage(t, ctx, runtime, "owner.b", connection.GetMessageType, connection.GetRequest{ConnectionID: connectionID})
	if _, err := runtime.HandleNext(ctx, connection.DefaultAddress); err == nil || !strings.Contains(err.Error(), "another service") {
		t.Fatalf("cross-service access error = %v", err)
	}

	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if driver.closeCount() != 1 {
		t.Fatalf("activation passivation closed %d sessions, want 1", driver.closeCount())
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionModuleRestoresActivationResourcesFromJournal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clock := fixedClock{now: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)}
	store := memory.New(&clock)
	inbound := map[contract.ServiceAddress]chan connection.InboundEvent{
		"owner.a": make(chan connection.InboundEvent, 4),
		"owner.b": make(chan connection.InboundEvent, 4),
	}
	firstDriver := newTestManagedDriver()
	first := newConnectionTestRuntime(t, ctx, store, firstDriver, inbound, &clock, "connection-recovery-node-1")
	if _, err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	publishConnectionMessage(t, ctx, first, "owner.a", connection.OpenMessageType, connection.OpenRequest{Key: "recover-me", Driver: "test"})
	if result, err := first.HandleNext(ctx, connection.DefaultAddress); err != nil || result.Status != "committed" {
		t.Fatalf("plan open connection result=%#v err=%v", result, err)
	}
	if _, err := first.DispatchNextEffect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.HandleNext(ctx, connection.DefaultAddress); err != nil {
		t.Fatal(err)
	}
	connectionID := StableIDs{}.Derive("connection", "connection-runtime", "v1", "owner.a", "recover-me")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	secondDriver := newTestManagedDriver()
	second := newConnectionTestRuntime(t, ctx, store, secondDriver, inbound, &clock, "connection-recovery-node-2")
	report, err := second.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.InstancesActivated == 0 {
		t.Fatalf("recovery report = %#v", report)
	}
	if secondDriver.openCount() != 1 || secondDriver.session(connectionID) == nil {
		t.Fatalf("restored driver opens=%d connection_id=%q", secondDriver.openCount(), connectionID)
	}
	// RestoreResources publishes DriverOpened while live delivery is paused;
	// durable ingress must retain it for normal handling after Start.
	if result, handleErr := second.HandleNext(ctx, connection.DefaultAddress); handleErr != nil || result.Status != "committed" {
		t.Fatalf("handle restored driver-opened event result=%#v err=%v", result, handleErr)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeDoesNotInstallConnectionModuleImplicitly(t *testing.T) {
	ctx := context.Background()
	builder, err := NewBuilder(BuilderOptions{IDs: StableIDs{}, OwnerID: "generic-runtime"})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{Runtime: building.RuntimeSpec{ID: "generic", Revision: "v1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, found := runtime.Plan().Service(connection.DefaultAddress); found {
		t.Fatal("generic Runtime implicitly installed the connection module")
	}
}

func publishConnectionMessage(t *testing.T, ctx context.Context, runtime *Runtime, from contract.ServiceAddress, messageType contract.MessageType, input any) {
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
		From: from, To: connection.DefaultAddress, Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
}

func dispatchAndHandleOwnerEvent(t *testing.T, ctx context.Context, runtime *Runtime, owner contract.ServiceAddress) {
	t.Helper()
	if result, err := runtime.DispatchNextOutbox(ctx); err != nil || result.Delivered != 1 {
		t.Fatalf("dispatch connection owner event result=%#v err=%v", result, err)
	}
	if result, err := runtime.HandleNext(ctx, owner); err != nil || result.Status != "committed" {
		t.Fatalf("handle connection owner event result=%#v err=%v", result, err)
	}
}

func receiveEvent(t *testing.T, ctx context.Context, values <-chan connection.InboundEvent) connection.InboundEvent {
	t.Helper()
	select {
	case event := <-values:
		return event
	case <-ctx.Done():
		t.Fatal("timed out waiting for connection event")
		return connection.InboundEvent{}
	}
}

func newConnectionTestRuntime(
	t *testing.T,
	ctx context.Context,
	store persistence.RuntimeStorage,
	driver connection.Driver,
	inbound map[contract.ServiceAddress]chan connection.InboundEvent,
	clock contract.Clock,
	ownerID string,
) *Runtime {
	t.Helper()
	builder, err := NewBuilder(BuilderOptions{Storage: store, Clock: clock, IDs: StableIDs{}, OwnerID: ownerID})
	if err != nil {
		t.Fatal(err)
	}
	module := connection.NewModule(connection.ModuleOptions{})
	if err := module.RegisterDriver("test", driver); err != nil {
		t.Fatal(err)
	}
	if err := module.Register(builder); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: connectionOwnerComponent,
		Factory: service.FactoryFunc(func(_ context.Context, create service.CreateRequest) (service.Service, error) {
			return &connectionOwnerService{inbound: inbound[create.Address]}, nil
		}),
		Scope: building.ScopeMounted,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageEvent, Type: connection.OpenedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: connection.MessageReceivedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: connection.ClosedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: connection.ErrorEventType, Version: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "connection-runtime", Revision: "v1"},
		Services: []building.ServiceMount{
			module.Mount(connection.DefaultAddress),
			{Address: "owner.a", Component: connectionOwnerComponent},
			{Address: "owner.b", Component: connectionOwnerComponent},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

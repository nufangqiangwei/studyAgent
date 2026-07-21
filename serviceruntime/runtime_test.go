package serviceruntime

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/host"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type conflictOnceStore struct {
	persistence.RuntimeStorage
	failed bool
}

func (s *conflictOnceStore) Committer() persistence.MessageCommitStore { return s }

func (s *conflictOnceStore) CommitMessage(ctx context.Context, commit persistence.MessageCommit) (persistence.CommitResult, error) {
	if !s.failed {
		s.failed = true
		return persistence.CommitResult{}, persistence.ErrSequenceConflict
	}
	return s.RuntimeStorage.Committer().CommitMessage(ctx, commit)
}

var (
	counterRef = contract.ComponentRef{Type: "test.counter", Version: "v1"}
	sinkRef    = contract.ComponentRef{Type: "test.sink", Version: "v1"}
	virtualRef = contract.ComponentRef{Type: "test.virtual", Version: "v1"}
)

type counterState struct {
	Count int `json:"count"`
}

type counterService struct{}

func (counterService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: counterRef}
}
func (counterService) InitialState(context.Context, service.Init) (service.State, error) {
	data, _ := json.Marshal(counterState{})
	return service.State{SchemaVersion: 1, Data: data}, nil
}
func (counterService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Type != "counter.increment" {
		return service.Decision{}, fmt.Errorf("unsupported message %q", message.Type)
	}
	var input struct {
		Amount int `json:"amount"`
	}
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}
	payload, _ := json.Marshal(input)
	return service.Decision{
		Events:   []service.NewEvent{{Key: "increment", Type: "counter.incremented", Version: 1, Payload: payload}},
		Outgoing: []service.OutgoingMessage{{Key: "changed", Kind: contract.MessageEvent, Type: "counter.changed", Version: 1, Payload: payload}},
		Effects: []service.PlannedEffect{{
			Key: "audit", Type: "audit.write", Version: 1, ExecutorRef: "audit.local",
			IdempotencyKey: "audit:" + message.ID, Payload: payload,
		}},
	}, nil
}
func (counterService) Apply(state service.State, event contract.StoredEvent) (service.State, error) {
	var current counterState
	if err := json.Unmarshal(state.Data, &current); err != nil {
		return service.State{}, err
	}
	var input struct {
		Amount int `json:"amount"`
	}
	if err := json.Unmarshal(event.Payload, &input); err != nil {
		return service.State{}, err
	}
	current.Count += input.Amount
	data, _ := json.Marshal(current)
	return service.State{SchemaVersion: 1, Data: data}, nil
}

type sinkState struct {
	Observed int `json:"observed"`
}
type sinkService struct{}

func (sinkService) Descriptor() service.Descriptor { return service.Descriptor{Component: sinkRef} }
func (sinkService) InitialState(context.Context, service.Init) (service.State, error) {
	data, _ := json.Marshal(sinkState{})
	return service.State{SchemaVersion: 1, Data: data}, nil
}

type virtualService struct{}

func (virtualService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: virtualRef}
}
func (virtualService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{"handled":0}`)}, nil
}
func (virtualService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	return service.Decision{Events: []service.NewEvent{{Key: "handled", Type: "virtual.handled", Version: 1, Payload: message.Payload}}}, nil
}
func (virtualService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	var current struct {
		Handled int `json:"handled"`
	}
	if err := json.Unmarshal(state.Data, &current); err != nil {
		return service.State{}, err
	}
	current.Handled++
	data, _ := json.Marshal(current)
	return service.State{SchemaVersion: 1, Data: data}, nil
}
func (sinkService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	return service.Decision{Events: []service.NewEvent{{Key: "observed", Type: "sink.observed", Version: 1, Payload: message.Payload}}}, nil
}
func (sinkService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	var current sinkState
	if err := json.Unmarshal(state.Data, &current); err != nil {
		return service.State{}, err
	}
	current.Observed++
	data, _ := json.Marshal(current)
	return service.State{SchemaVersion: 1, Data: data}, nil
}

type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fixedClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func TestRuntimeEndToEndAndRecovery(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 16, 8, 0, 0, 0, time.UTC)}
	store := memory.New(clock)
	auditCalls := 0
	builder := newTestBuilder(t, store, clock, &auditCalls)
	manifest := testManifest()

	first, err := builder.Build(ctx, manifest)
	if err != nil {
		t.Fatal(err)
	}
	report, err := first.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.InstancesActivated != 2 || first.Status() != RuntimeLive {
		t.Fatalf("recovery report=%#v status=%q", report, first.Status())
	}
	payload := json.RawMessage(`{"amount":2}`)
	published, err := first.Publish(ctx, contract.Message{Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(published.Targets) != 1 || published.Targets[0].Address != "counter.main" {
		t.Fatalf("publish result = %#v", published)
	}
	processed, err := first.HandleNext(ctx, "counter.main")
	if err != nil {
		t.Fatal(err)
	}
	if processed.LastSequence != 1 || len(processed.OutboxIDs) != 1 || len(processed.EffectIDs) != 1 {
		t.Fatalf("counter handle = %#v", processed)
	}
	effectResult, err := first.DispatchNextEffect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if effectResult.Status != persistence.EffectSucceeded || auditCalls != 1 {
		t.Fatalf("effect result=%#v audit_calls=%d", effectResult, auditCalls)
	}
	dispatched, err := first.DispatchNextOutbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched.Delivered != 1 {
		t.Fatalf("outbox dispatch = %#v", dispatched)
	}
	sinkResult, err := first.HandleNext(ctx, "sink.main")
	if err != nil {
		t.Fatal(err)
	}
	if sinkResult.LastSequence != 1 {
		t.Fatalf("sink handle = %#v", sinkResult)
	}
	assertStreamState(t, ctx, first, "counter.main", 1, `{"count":2}`)
	assertStreamState(t, ctx, first, "sink.main", 1, `{"observed":1}`)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := builder.Build(ctx, manifest)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := second.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.StreamsRestored != 2 {
		t.Fatalf("recovered = %#v", recovered)
	}
	if _, err := second.Publish(ctx, contract.Message{Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, Payload: json.RawMessage(`{"amount":3}`)}); err != nil {
		t.Fatal(err)
	}
	secondResult, err := second.HandleNext(ctx, "counter.main")
	if err != nil {
		t.Fatal(err)
	}
	if secondResult.LastSequence != 2 {
		t.Fatalf("second counter handle = %#v", secondResult)
	}
	assertStreamState(t, ctx, second, "counter.main", 2, `{"count":5}`)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeDeclaresAndAddressesVirtualInstances(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 16, 9, 0, 0, 0, time.UTC)}
	store := memory.New(clock)
	auditCalls := 0
	builder := newTestBuilder(t, store, clock, &auditCalls)
	runtime, err := builder.Build(ctx, testManifest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	root, err := runtime.DeclareInstance(ctx, InstanceDeclaration{Address: "virtual.root", Component: virtualRef, Metadata: map[string]string{"role": "root"}})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := runtime.DeclareInstance(ctx, InstanceDeclaration{Address: "virtual.root", Component: virtualRef})
	if err != nil || duplicate.InstanceID != root.InstanceID {
		t.Fatalf("idempotent declaration record=%#v err=%v", duplicate, err)
	}
	child, err := runtime.DeclareInstance(ctx, InstanceDeclaration{Address: "virtual.child", Component: virtualRef, ParentID: root.InstanceID})
	if err != nil {
		t.Fatal(err)
	}
	if child.ParentID != root.InstanceID || child.RootID != root.InstanceID || child.Depth != 1 {
		t.Fatalf("child relationship = %#v", child)
	}
	if _, err := runtime.Publish(ctx, contract.Message{
		Kind: contract.MessageCommand, Type: "virtual.handle", Version: 1,
		To: "virtual.root", Payload: json.RawMessage(`{"value":"ok"}`),
	}); err != nil {
		t.Fatal(err)
	}
	handled, err := runtime.HandleNext(ctx, "virtual.root")
	if err != nil {
		t.Fatal(err)
	}
	if handled.LastSequence != 1 {
		t.Fatalf("virtual handle = %#v", handled)
	}
	if err := runtime.TerminateInstance(ctx, root.InstanceID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Publish(ctx, contract.Message{Kind: contract.MessageCommand, Type: "virtual.handle", Version: 1, To: "virtual.root"}); err == nil {
		t.Fatal("expected terminated virtual address to reject delivery")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSequenceConflictPassivatesAndRestoresBeforeRetry(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)}
	store := &conflictOnceStore{RuntimeStorage: memory.New(clock)}
	var auditCalls int
	builder := newTestBuilder(t, store, clock, &auditCalls)
	runtime, err := builder.Build(ctx, testManifest())
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Publish(ctx, contract.Message{Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, Payload: json.RawMessage(`{"amount":1}`)}); err != nil {
		t.Fatal(err)
	}
	first, err := runtime.HandleNext(ctx, "counter.main")
	if !errors.Is(err, persistence.ErrSequenceConflict) || first.Status != host.HandleRetry {
		t.Fatalf("first result=%#v err=%v", first, err)
	}
	if _, active := runtime.activator.Lookup(first.InstanceID); active {
		t.Fatal("conflicting activation must be passivated before retry")
	}
	second, err := runtime.HandleNext(ctx, "counter.main")
	if err != nil || second.Status != host.HandleCommitted || second.LastSequence != 1 {
		t.Fatalf("second result=%#v err=%v", second, err)
	}
}

func TestRuntimeClosePassivatesActivationsAndReleasesLeases(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)}
	store := memory.New(clock)
	var auditCalls int
	builder := newTestBuilder(t, store, clock, &auditCalls)
	runtime, err := builder.Build(ctx, testManifest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	record, found, err := store.Instances().GetByAddress(ctx, "test-runtime", "v1", "counter.main")
	if err != nil || !found {
		t.Fatalf("record found=%v err=%v", found, err)
	}
	if _, found, err := store.Leases().Current(ctx, record.InstanceID); err != nil || !found {
		t.Fatalf("active lease found=%v err=%v", found, err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Leases().Current(ctx, record.InstanceID); err != nil || found {
		t.Fatalf("lease remains after close: found=%v err=%v", found, err)
	}
	record, _, _ = store.Instances().Get(ctx, record.InstanceID)
	if record.Lifecycle != instance.Passivated {
		t.Fatalf("lifecycle=%q, want passivated", record.Lifecycle)
	}
}

func TestRuntimeRejectsChangedManifestForExistingRevision(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)}
	store := memory.New(clock)
	var auditCalls int
	firstBuilder := newTestBuilder(t, store, clock, &auditCalls)
	first, err := firstBuilder.Build(ctx, testManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	changed := testManifest()
	changed.Recovery.MaxDeliveryAttempts++
	secondBuilder := newTestBuilder(t, store, clock, &auditCalls)
	if _, err := secondBuilder.Build(ctx, changed); !errors.Is(err, persistence.ErrPlanConflict) {
		t.Fatalf("error=%v, want plan revision conflict", err)
	}
}

func TestRuntimeRecoversOldRevisionOutboxWithOldRouting(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)}
	store := memory.New(clock)
	var auditCalls int
	firstBuilder := newTestBuilder(t, store, clock, &auditCalls)
	first, err := firstBuilder.Build(ctx, testManifest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Publish(ctx, contract.Message{Kind: contract.MessageCommand, Type: "counter.increment", Version: 1, Payload: json.RawMessage(`{"amount":2}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.HandleNext(ctx, "counter.main"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	manifestV2 := testManifest()
	manifestV2.Runtime.Revision = "v2"
	secondBuilder := newTestBuilder(t, store, clock, &auditCalls)
	second, err := secondBuilder.Build(ctx, manifestV2)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	dispatched, err := second.DispatchNextOutbox(ctx)
	if err != nil || dispatched.Delivered != 1 {
		t.Fatalf("dispatch=%#v err=%v", dispatched, err)
	}
	oldSink, found, err := store.Instances().GetByAddress(ctx, "test-runtime", "v1", "sink.main")
	if err != nil || !found {
		t.Fatalf("old sink found=%v err=%v", found, err)
	}
	result, err := second.host.HandleNext(ctx, oldSink.InstanceID)
	if err != nil || result.Status != host.HandleCommitted {
		t.Fatalf("old revision sink result=%#v err=%v", result, err)
	}
}

func TestRuntimeUsesFrozenDefinitionCatalog(t *testing.T) {
	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)}
	store := memory.New(clock)
	var auditCalls int
	builder := newTestBuilder(t, store, clock, &auditCalls)
	runtime, err := builder.Build(ctx, testManifest())
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	lateRef := contract.ComponentRef{Type: "late", Version: "v1"}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: lateRef, Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) { return virtualService{}, nil }),
		Scope: building.ScopeVirtual,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.DeclareInstance(ctx, InstanceDeclaration{Address: "late.1", Component: lateRef}); err == nil {
		t.Fatal("runtime accepted a definition registered after plan compilation")
	}
}

func newTestBuilder(t *testing.T, store persistence.RuntimeStorage, clock contract.Clock, auditCalls *int) *Builder {
	t.Helper()
	builder, err := NewBuilder(BuilderOptions{Storage: store, Clock: clock, IDs: StableIDs{}, OwnerID: "test-node"})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterEffect(effect.Spec{
		Ref: "audit.local", Type: "audit.write",
		Executor: effect.ExecutorFunc(func(context.Context, persistence.EffectRecord) (effect.ExecutionResult, error) {
			*auditCalls++
			return effect.ExecutionResult{Payload: json.RawMessage(`{"ok":true}`)}, nil
		}),
		Reconciler: effect.ReconcilerFunc(func(context.Context, persistence.EffectRecord) (effect.ReconciliationResult, error) {
			return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: json.RawMessage(`{"ok":true}`)}, nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: counterRef, Scope: building.ScopeMounted,
		Factory:         service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) { return counterService{}, nil }),
		Consumes:        []building.MessageContract{{Kind: contract.MessageCommand, Type: "counter.increment", Version: 1}},
		Produces:        []building.MessageContract{{Kind: contract.MessageEvent, Type: "counter.changed", Version: 1}},
		EffectExecutors: []string{"audit.local"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: sinkRef, Scope: building.ScopeMounted,
		Factory:  service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) { return sinkService{}, nil }),
		Consumes: []building.MessageContract{{Kind: contract.MessageEvent, Type: "counter.changed", Version: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: virtualRef, Scope: building.ScopeVirtual,
		Factory:  service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) { return virtualService{}, nil }),
		Consumes: []building.MessageContract{{Kind: contract.MessageCommand, Type: "virtual.handle", Version: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	return builder
}

func testManifest() building.RuntimeManifest {
	return building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "test-runtime", Revision: "v1"},
		Services: []building.ServiceMount{
			{Address: "counter.main", Component: counterRef},
			{Address: "sink.main", Component: sinkRef},
		},
		Routes: building.RouteManifest{
			Commands: map[contract.MessageType]contract.ServiceAddress{"counter.increment": "counter.main"},
			Events:   map[contract.MessageType][]contract.ServiceAddress{"counter.changed": {"sink.main"}},
		},
		Recovery: building.RecoveryPolicy{SnapshotEveryEvents: 1},
	}
}

func assertStreamState(t *testing.T, ctx context.Context, runtime *Runtime, address contract.ServiceAddress, sequence uint64, want string) {
	t.Helper()
	spec := runtime.plan.Runtime()
	target, err := runtime.directory.ResolveAddress(ctx, spec.ID, spec.Revision, address)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := runtime.storage.Instances().Get(ctx, target.InstanceID)
	if err != nil || !found {
		t.Fatalf("instance found=%v err=%v", found, err)
	}
	snapshot, found, err := runtime.storage.Snapshots().LoadLatest(ctx, record.StateStreamID)
	if err != nil || !found {
		t.Fatalf("snapshot found=%v err=%v", found, err)
	}
	if snapshot.LastSequence != sequence || string(snapshot.State) != want {
		t.Fatalf("snapshot sequence=%d state=%s, want sequence=%d state=%s", snapshot.LastSequence, snapshot.State, sequence, want)
	}
}

package webgateway

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/host"
	persistencememory "agent/serviceruntime/persistence/memory"
	"agent/services/task"
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestCreateSagaAdvancesToRunningAcrossRuntimeRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := fixedClock{fixedTime()}
	storage := persistencememory.New(clock)
	defer storage.Close()
	presented := make(chan Presentation, 8)

	runtime1 := buildGatewayRuntime(t, ctx, storage, clock, presented)
	if _, err := runtime1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	createPayload, _ := json.Marshal(CreateTaskRequest{
		RequestID: "request-1", TaskID: "task-1", GoalID: "goal-1", Title: "demo", Input: "hello",
	})
	if _, err := runtime1.Publish(ctx, contract.Message{
		ID: "web-create-1", Kind: contract.MessageCommand, Type: CreateTaskMessageType, Version: ProtocolVersion,
		From: "web.adapter", To: DefaultAddress, UserID: "user-1", GoalID: "goal-1", Payload: createPayload,
	}); err != nil {
		t.Fatal(err)
	}
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	// The request event and system.call are durable. Restart to prove recovery.
	if err := runtime1.Close(); err != nil {
		t.Fatal(err)
	}

	runtime2 := buildGatewayRuntime(t, ctx, storage, clock, presented)
	defer runtime2.Close()
	if _, err := runtime2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	taskAddress, _ := stableTaskIdentity("task-1", "request-1")

	// Step 1: Deliver system.call → system.runtime handles → dispatch effects → Gateway handles system result
	dispatchOutboxUntilIdle(t, ctx, runtime2)
	handleGatewayCommitted(t, ctx, runtime2, "system.runtime")
	dispatchEffectsUntilIdle(t, ctx, runtime2)
	handleGatewayCommitted(t, ctx, runtime2, DefaultAddress)

	// Step 2: task.create → task instance handles → reply status (Created) → Gateway advances to mark_ready
	sagaStep(t, ctx, runtime2, taskAddress, DefaultAddress)

	// Step 3: task.mark_ready → task instance handles → reply status (Ready) → Gateway advances to assign
	sagaStep(t, ctx, runtime2, taskAddress, DefaultAddress)

	// Step 4: task.assign → task instance handles → reply status (Ready+Assigned) → Gateway advances to start
	sagaStep(t, ctx, runtime2, taskAddress, DefaultAddress)

	// Step 5: task.start → task instance handles → reply status (Running) → Gateway succeeds
	sagaStep(t, ctx, runtime2, taskAddress, DefaultAddress)

	// Step 6: Presentation effect
	dispatchEffectsUntilIdle(t, ctx, runtime2)

	select {
	case presentation := <-presented:
		if presentation.Created == nil || presentation.Created.Task.TaskID != "task-1" ||
			presentation.Created.Task.Phase != task.PhaseRunning {
			t.Fatalf("unexpected create presentation: %#v", presentation)
		}
		if presentation.Created.Task.ActiveRunID == "" {
			t.Fatalf("presentation missing active run id: %#v", presentation)
		}
	default:
		t.Fatal("create presentation was not delivered")
	}

	// Verify get still works
	getPayload, _ := json.Marshal(GetTaskRequest{RequestID: "get-1", TaskID: "task-1"})
	if _, err := runtime2.Publish(ctx, contract.Message{
		ID: "web-get-1", Kind: contract.MessageCommand, Type: GetTaskMessageType, Version: ProtocolVersion,
		From: "web.adapter", To: DefaultAddress, UserID: "user-1", Payload: getPayload,
	}); err != nil {
		t.Fatal(err)
	}
	handleGatewayCommitted(t, ctx, runtime2, DefaultAddress)
	dispatchOutboxUntilIdle(t, ctx, runtime2)
	handleGatewayCommitted(t, ctx, runtime2, taskAddress)
	dispatchOutboxUntilIdle(t, ctx, runtime2)
	handleGatewayCommitted(t, ctx, runtime2, DefaultAddress)
	dispatchEffectsUntilIdle(t, ctx, runtime2)

	select {
	case presentation := <-presented:
		if presentation.Found == nil || presentation.Found.Task.TaskID != "task-1" {
			t.Fatalf("unexpected get presentation: %#v", presentation)
		}
	default:
		t.Fatal("get presentation was not delivered")
	}
}

// sagaStep runs one round-trip of the Gateway→Task→Gateway cycle:
// dispatch outbox (command to task) → task handles → dispatch outbox (status to gateway) → gateway handles
func sagaStep(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime, taskAddress, gatewayAddress contract.ServiceAddress) {
	t.Helper()
	dispatchOutboxUntilIdle(t, ctx, runtime)
	handleGatewayCommitted(t, ctx, runtime, taskAddress)
	dispatchOutboxUntilIdle(t, ctx, runtime)
	handleGatewayCommitted(t, ctx, runtime, gatewayAddress)
}

func buildGatewayRuntime(
	t *testing.T,
	ctx context.Context,
	storage *persistencememory.Store,
	clock contract.Clock,
	presented chan Presentation,
) *serviceruntime.Runtime {
	t.Helper()
	builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
		Storage: storage, Clock: clock, IDs: serviceruntime.StableIDs{}, OwnerID: "web-gateway-integration-node",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := task.NewModule(clock).Register(builder); err != nil {
		t.Fatal(err)
	}
	module, err := NewModule(ModuleOptions{
		Clock: clock,
		Presenter: PresenterFunc(func(_ context.Context, presentation Presentation) error {
			presented <- presentation.clone()
			return nil
		}),
		DefaultAgent: "agent.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.Register(builder); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime:  building.RuntimeSpec{ID: "web-gateway-integration-runtime", Revision: "v1"},
		Services: []building.ServiceMount{module.Mount(DefaultAddress)},
		Recovery: building.RecoveryPolicy{SnapshotEveryEvents: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func handleGatewayCommitted(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime, address contract.ServiceAddress) {
	t.Helper()
	result, err := runtime.HandleNext(ctx, address)
	if err != nil || result.Status != host.HandleCommitted {
		t.Fatalf("handle %s result=%#v err=%v", address, result, err)
	}
}

func dispatchOutboxUntilIdle(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime) {
	t.Helper()
	for index := 0; index < 64; index++ {
		result, err := runtime.DispatchNextOutbox(ctx)
		if err != nil {
			// Tolerate undeliverable messages (e.g. agent.execute to unregistered agent)
			// as they will be dead-lettered. The Gateway saga does not depend on agent
			// execution completing for the task to enter Running.
			continue
		}
		if result.Idle {
			return
		}
	}
	t.Fatal("outbox did not become idle")
}

func dispatchEffectsUntilIdle(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime) {
	t.Helper()
	for index := 0; index < 32; index++ {
		result, err := runtime.DispatchNextEffect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if result.Idle {
			return
		}
	}
	t.Fatal("effects did not become idle")
}

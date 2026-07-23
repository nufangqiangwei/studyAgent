package webgateway

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/host"
	"agent/serviceruntime/persistence"
	persistencememory "agent/serviceruntime/persistence/memory"
	persistencesqlite "agent/serviceruntime/persistence/sqlite"
	"agent/serviceruntime/service"
	runtimesystem "agent/serviceruntime/system"
	agentservice "agent/services/agent"
	"agent/services/task"
	"context"
	"encoding/json"
	"path/filepath"
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

func TestRecoveredOldPlanRevisionUsesItsDefaultAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := fixedClock{fixedTime()}
	storage := persistencememory.New(clock)
	defer storage.Close()
	presented := make(chan Presentation, 8)

	runtime1 := buildGatewayRuntimeRevision(t, ctx, storage, clock, presented, "v1", "agent.old")
	if _, err := runtime1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	publishCreate(t, ctx, runtime1, "web-create-old", "request-old", "task-old")
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	if err := runtime1.Close(); err != nil {
		t.Fatal(err)
	}

	runtime2 := buildGatewayRuntimeRevision(t, ctx, storage, clock, presented, "v2", "agent.new")
	defer runtime2.Close()
	if _, err := runtime2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopServe, serveDone := serveGatewayRuntime(t, ctx, runtime2)
	presentation := waitForPresentation(t, ctx, presented, "request-old")
	stopGatewayRuntime(t, stopServe, serveDone)

	if presentation.Created == nil {
		t.Fatalf("old revision presentation=%#v", presentation)
	}
	if presentation.Created.Task.Phase != task.PhaseRunning ||
		presentation.Created.Task.AssignedTo != "agent.old" {
		t.Fatalf("old revision used the wrong mount config: %#v", presentation.Created.Task)
	}
}

func TestSQLiteRecoveryMigratesLegacyEmptyMountWithoutLeakingIntoNewRevision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := fixedClock{fixedTime()}
	databasePath := filepath.Join(t.TempDir(), "runtime.db")
	presented := make(chan Presentation, 16)

	storage1, err := persistencesqlite.Open(ctx, databasePath, persistencesqlite.Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	runtime1 := buildGatewayRuntimeConfig(
		t, ctx, storage1, clock, presented,
		"v1", "agent.legacy", "agent.legacy", true,
	)
	if _, err := runtime1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	publishCreate(t, ctx, runtime1, "web-create-legacy", "request-legacy", "task-legacy")
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	if err := runtime1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storage1.Close(); err != nil {
		t.Fatal(err)
	}

	storage2, err := persistencesqlite.Open(ctx, databasePath, persistencesqlite.Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer storage2.Close()
	runtime2 := buildGatewayRuntimeConfig(
		t, ctx, storage2, clock, presented,
		"v2", "agent.new", "agent.legacy", false,
	)
	defer runtime2.Close()
	if _, err := runtime2.Start(ctx); err != nil {
		t.Fatalf("start with persisted empty-config Plan: %v", err)
	}
	stopServe, serveDone := serveGatewayRuntime(t, ctx, runtime2)

	legacyPresentation := waitForPresentation(t, ctx, presented, "request-legacy")
	if legacyPresentation.Created == nil ||
		legacyPresentation.Created.Task.AssignedTo != "agent.legacy" {
		t.Fatalf("legacy empty mount did not use its explicit migration fallback: %#v", legacyPresentation)
	}

	publishCreate(t, ctx, runtime2, "web-create-new", "request-new", "task-new")
	newPresentation := waitForPresentation(t, ctx, presented, "request-new")
	stopGatewayRuntime(t, stopServe, serveDone)
	if newPresentation.Created == nil ||
		newPresentation.Created.Task.AssignedTo != "agent.new" {
		t.Fatalf("versioned current mount leaked to legacy fallback: %#v", newPresentation)
	}
}

func TestRecoveredInFlightCreateStillRejectsDifferentCreateForSameTaskID(t *testing.T) {
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
	publishCreate(t, ctx, runtime1, "web-create-1", "request-1", "task-1")
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	if err := runtime1.Close(); err != nil {
		t.Fatal(err)
	}

	runtime2 := buildGatewayRuntime(t, ctx, storage, clock, presented)
	defer runtime2.Close()
	if _, err := runtime2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	publishCreate(t, ctx, runtime2, "web-create-2", "request-2", "task-1")
	handleGatewayCommitted(t, ctx, runtime2, DefaultAddress)
	dispatchEffectsUntilIdle(t, ctx, runtime2)

	presentation := waitForPresentation(t, ctx, presented, "request-2")
	if presentation.Error == nil || presentation.Error.Code != errRequestConflict {
		t.Fatalf("recovered reservation did not reject conflicting create: %#v", presentation)
	}
}

func TestTaskOwnershipAndFailedCreateRemainReachableAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := fixedClock{fixedTime()}
	storage := persistencememory.New(clock)
	defer storage.Close()
	presented := make(chan Presentation, 16)

	runtime1 := buildGatewayRuntime(t, ctx, storage, clock, presented)
	if _, err := runtime1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	publishCreate(t, ctx, runtime1, "web-create-owned", "request-owned", "task-owned")
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	dispatchOutboxUntilIdle(t, ctx, runtime1)
	handleGatewayCommitted(t, ctx, runtime1, runtimesystem.Address)
	dispatchEffectsUntilIdle(t, ctx, runtime1)
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	taskAddress, _ := stableTaskIdentity("task-owned", "request-owned")
	sagaStep(t, ctx, runtime1, taskAddress, DefaultAddress)

	errorPayload, err := json.Marshal(service.ReplyError{
		Code: "task_stage_failed", Message: "mark ready failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime1.Publish(ctx, contract.Message{
		ID:   "task-stage-failed-owned",
		Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion,
		From: taskAddress, To: DefaultAddress, CorrelationID: "request-owned", Payload: errorPayload,
		Metadata: map[string]string{contract.MetadataReplyError: "true"},
	}); err != nil {
		t.Fatal(err)
	}
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	dispatchEffectsUntilIdle(t, ctx, runtime1)
	failed := waitForPresentation(t, ctx, presented, "request-owned")
	if failed.Error == nil || failed.Error.Code != errTaskRequestFailed ||
		failed.Error.TaskID != "task-owned" {
		t.Fatalf("post-create failure lost task identity: %#v", failed)
	}
	if err := runtime1.Close(); err != nil {
		t.Fatal(err)
	}

	runtime2 := buildGatewayRuntime(t, ctx, storage, clock, presented)
	defer runtime2.Close()
	if _, err := runtime2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	getPayload, err := json.Marshal(GetTaskRequest{
		RequestID: "get-owned-after-restart", TaskID: "task-owned",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime2.Publish(ctx, contract.Message{
		ID:   "get-owned-after-restart",
		Kind: contract.MessageCommand, Type: GetTaskMessageType, Version: ProtocolVersion,
		From: "web.adapter", To: DefaultAddress, UserID: "user-1", Payload: getPayload,
	}); err != nil {
		t.Fatal(err)
	}
	stopServe, serveDone := serveGatewayRuntime(t, ctx, runtime2)
	found := waitForPresentation(t, ctx, presented, "get-owned-after-restart")
	hiddenPayload, err := json.Marshal(GetTaskRequest{
		RequestID: "get-owned-other-user", TaskID: "task-owned",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime2.Publish(ctx, contract.Message{
		ID:   "get-owned-other-user",
		Kind: contract.MessageCommand, Type: GetTaskMessageType, Version: ProtocolVersion,
		From: "web.adapter", To: DefaultAddress, UserID: "user-2", Payload: hiddenPayload,
	}); err != nil {
		t.Fatal(err)
	}
	hidden := waitForPresentation(t, ctx, presented, "get-owned-other-user")
	stopGatewayRuntime(t, stopServe, serveDone)
	if found.Found == nil || found.Found.Task.TaskID != "task-owned" {
		t.Fatalf("owned task was unreachable after restart: %#v", found)
	}
	if hidden.Error == nil || hidden.Error.Code != errTaskNotFound || hidden.Error.TaskID != "" {
		t.Fatalf("cross-user recovery lookup leaked task identity: %#v", hidden)
	}
}

func TestTerminalEventBeforeRunningReplySurvivesRestartAndPresentsTerminalTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := fixedClock{fixedTime()}
	storage := persistencememory.New(clock)
	defer storage.Close()
	presented := make(chan Presentation, 16)

	runtime1 := buildGatewayRuntime(t, ctx, storage, clock, presented)
	if _, err := runtime1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	publishCreate(t, ctx, runtime1, "web-create-race", "request-race", "task-race")
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	taskAddress, _ := stableTaskIdentity("task-race", "request-race")

	dispatchOutboxUntilIdle(t, ctx, runtime1)
	handleGatewayCommitted(t, ctx, runtime1, "system.runtime")
	dispatchEffectsUntilIdle(t, ctx, runtime1)
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	sagaStep(t, ctx, runtime1, taskAddress, DefaultAddress)
	sagaStep(t, ctx, runtime1, taskAddress, DefaultAddress)
	sagaStep(t, ctx, runtime1, taskAddress, DefaultAddress)

	// Commit task.start, but deliberately leave its Running status reply in
	// Outbox. A fast Agent then completes the authoritative Task first.
	dispatchOutboxUntilIdle(t, ctx, runtime1)
	handleGatewayCommitted(t, ctx, runtime1, taskAddress)
	resultRef := contract.ArtifactRef{
		Store: "test", Key: "tasks/task-race/result.txt", ContentType: "text/plain", Size: 4,
	}
	runID := "task-race/attempt/1"
	completedPayload, err := json.Marshal(agentservice.ExecuteResult{
		RunID: runID, Phase: agentservice.PhaseCompleted, Output: &resultRef,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime1.Publish(ctx, contract.Message{
		ID:   "agent-completed-race",
		Kind: contract.MessageReply, Type: agentservice.CompletedMessageType, Version: agentservice.ProtocolVersion,
		From: "agent.test", To: taskAddress, CorrelationID: runID, Payload: completedPayload,
	}); err != nil {
		t.Fatal(err)
	}
	handleGatewayCommitted(t, ctx, runtime1, taskAddress)

	// Publish the same terminal owner fact directly before the older Running
	// reply is dispatched. Task Service's own durable terminal event remains
	// pending and later proves duplicate handling.
	terminalPayload, err := json.Marshal(task.Result{
		TaskID: "task-race", GoalID: "goal-1", Phase: task.PhaseCompleted,
		Attempt: 1, ResultRef: &resultRef, CompletedAt: fixedTime(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime1.Publish(ctx, contract.Message{
		ID:   "task-terminal-race-first",
		Kind: contract.MessageEvent, Type: task.CompletedEventType, Version: task.ProtocolVersion,
		From: taskAddress, To: DefaultAddress, CorrelationID: "task-race", Payload: terminalPayload,
	}); err != nil {
		t.Fatal(err)
	}
	handleGatewayCommitted(t, ctx, runtime1, DefaultAddress)
	if err := runtime1.Close(); err != nil {
		t.Fatal(err)
	}

	runtime2 := buildGatewayRuntime(t, ctx, storage, clock, presented)
	defer runtime2.Close()
	if _, err := runtime2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopServe, serveDone := serveGatewayRuntime(t, ctx, runtime2)
	presentation := waitForPresentation(t, ctx, presented, "request-race")
	stopGatewayRuntime(t, stopServe, serveDone)

	if presentation.Created == nil ||
		presentation.Created.Task.Phase != task.PhaseCompleted ||
		presentation.Created.Task.ResultRef == nil ||
		presentation.Created.Task.ResultRef.Key != resultRef.Key {
		t.Fatalf("restart presented stale Running state: %#v", presentation)
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
	storage persistence.RuntimeStorage,
	clock contract.Clock,
	presented chan Presentation,
) *serviceruntime.Runtime {
	return buildGatewayRuntimeRevision(t, ctx, storage, clock, presented, "v1", "agent.test")
}

func buildGatewayRuntimeRevision(
	t *testing.T,
	ctx context.Context,
	storage persistence.RuntimeStorage,
	clock contract.Clock,
	presented chan Presentation,
	revision contract.PlanRevision,
	defaultAgent contract.ServiceAddress,
) *serviceruntime.Runtime {
	return buildGatewayRuntimeConfig(
		t, ctx, storage, clock, presented,
		revision, defaultAgent, defaultAgent, false,
	)
}

func buildGatewayRuntimeConfig(
	t *testing.T,
	ctx context.Context,
	storage persistence.RuntimeStorage,
	clock contract.Clock,
	presented chan Presentation,
	revision contract.PlanRevision,
	defaultAgent contract.ServiceAddress,
	legacyDefaultAgent contract.ServiceAddress,
	legacyEmptyMount bool,
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
		DefaultAgent: defaultAgent, LegacyDefaultAgent: legacyDefaultAgent,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.Register(builder); err != nil {
		t.Fatal(err)
	}
	mount := module.Mount(DefaultAddress)
	if legacyEmptyMount {
		mount.Config = nil
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime:  building.RuntimeSpec{ID: "web-gateway-integration-runtime", Revision: revision},
		Services: []building.ServiceMount{mount},
		Recovery: building.RecoveryPolicy{SnapshotEveryEvents: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func publishCreate(
	t *testing.T,
	ctx context.Context,
	runtime *serviceruntime.Runtime,
	messageID string,
	requestID string,
	taskID string,
) {
	t.Helper()
	payload, err := json.Marshal(CreateTaskRequest{
		RequestID: requestID, TaskID: taskID, GoalID: "goal-1", Title: "demo", Input: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Publish(ctx, contract.Message{
		ID: messageID, Kind: contract.MessageCommand, Type: CreateTaskMessageType, Version: ProtocolVersion,
		From: "web.adapter", To: DefaultAddress, UserID: "user-1", GoalID: "goal-1", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
}

func serveGatewayRuntime(
	t *testing.T,
	ctx context.Context,
	runtime *serviceruntime.Runtime,
) (context.CancelFunc, chan error) {
	t.Helper()
	serveContext, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- runtime.ServeWithOptions(serveContext, serviceruntime.ServeOptions{PollInterval: time.Millisecond})
	}()
	return cancel, done
}

func waitForPresentation(
	t *testing.T,
	ctx context.Context,
	presented <-chan Presentation,
	requestID string,
) Presentation {
	t.Helper()
	for {
		select {
		case presentation := <-presented:
			if presentation.RequestID == requestID {
				return presentation
			}
		case <-ctx.Done():
			t.Fatalf("wait for presentation %q: %v", requestID, ctx.Err())
		}
	}
}

func stopGatewayRuntime(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve Runtime: %v", err)
	}
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

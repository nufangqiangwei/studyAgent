package runtime

import (
	"agent/internal/runtime/agents"
	eventbus2 "agent/internal/runtime/eventbus"
	statemachine2 "agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewDoesNotRegisterHardcodedAgent(t *testing.T) {
	rt, err := New(WithSyncEffects(), WithResultDelivery(eventbus2.DeliverySync))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer rt.Close()

	if runtimes := rt.RuntimeRegistry().List(); len(runtimes) != 0 {
		t.Fatalf("runtimes = %#v, want empty registry after New", runtimes)
	}
	_, err = rt.StartTask(context.Background(), Task{TaskID: "task_1", Input: "run"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("StartTask error = %v, want unregistered task runtime error", err)
	}
}

func TestRegisterAgentRequiresTaskID(t *testing.T) {
	rt, err := New()
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer rt.Close()

	err = rt.RegisterAgent(context.Background(), "", newCompletingAgent("worker"))
	if err == nil || !strings.Contains(err.Error(), "task_id") {
		t.Fatalf("RegisterAgent error = %v, want task_id requirement", err)
	}
}

func TestRuntimeRoutesSameAgentNameByTaskID(t *testing.T) {
	ctx := context.Background()
	rt, err := New(WithSyncEffects(), WithResultDelivery(eventbus2.DeliverySync))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer rt.Close()

	taskOneAgent := newCompletingAgent("worker")
	taskTwoAgent := newCompletingAgent("worker")
	if err := rt.RegisterAgent(ctx, "task_1", taskOneAgent); err != nil {
		t.Fatalf("RegisterAgent task_1 returned error: %v", err)
	}
	if err := rt.RegisterAgent(ctx, "task_2", taskTwoAgent); err != nil {
		t.Fatalf("RegisterAgent task_2 returned error: %v", err)
	}

	if _, err := rt.StartTask(ctx, Task{TaskID: "task_1", Input: "first"}); err != nil {
		t.Fatalf("StartTask task_1 returned error: %v", err)
	}
	if _, err := rt.StartTask(ctx, Task{TaskID: "task_2", Input: "second"}); err != nil {
		t.Fatalf("StartTask task_2 returned error: %v", err)
	}

	if got := taskOneAgent.startedTasks(); len(got) != 1 || got[0] != "task_1" {
		t.Fatalf("task one agent started tasks = %#v, want task_1 only", got)
	}
	if got := taskTwoAgent.startedTasks(); len(got) != 1 || got[0] != "task_2" {
		t.Fatalf("task two agent started tasks = %#v, want task_2 only", got)
	}
	assertCompletedState(t, rt, "task_1")
	assertCompletedState(t, rt, "task_2")
	if runtimes := rt.RuntimeRegistry().List(); len(runtimes) != 2 {
		t.Fatalf("runtimes = %#v, want separate runtimes for two task ids", runtimes)
	}
}

func TestCreateTaskRuntimeReturnsTaskScopedFacade(t *testing.T) {
	ctx := context.Background()
	rt, err := New(WithSyncEffects(), WithResultDelivery(eventbus2.DeliverySync))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer rt.Close()

	delivered := make(chan eventbus2.Event, 1)
	if _, err := rt.EventBus().SubscribeReadOnlyFunc(eventbus2.Filter{Topic: "external", TaskID: "task_1"}, func(_ context.Context, event eventbus2.Event) error {
		delivered <- event
		return nil
	}); err != nil {
		t.Fatalf("SubscribeReadOnlyFunc returned error: %v", err)
	}

	agent := newCompletingAgent("worker")
	taskRuntime, err := rt.CreateTaskRuntime(ctx, "task_1", agent)
	if err != nil {
		t.Fatalf("CreateTaskRuntime returned error: %v", err)
	}
	if taskRuntime.TaskID() != "task_1" || taskRuntime.AgentName() != "worker" {
		t.Fatalf("task runtime task=%q agent=%q, want task_1 worker", taskRuntime.TaskID(), taskRuntime.AgentName())
	}

	if _, err := taskRuntime.Start(ctx, TaskStart{Input: "first"}); err != nil {
		t.Fatalf("task runtime Start returned error: %v", err)
	}
	state, ok, err := taskRuntime.State(ctx)
	if err != nil || !ok {
		t.Fatalf("task runtime State ok=%v err=%v, want state", ok, err)
	}
	if state.TaskID != "task_1" || state.Phase != statemachine2.PhaseCompleted {
		t.Fatalf("state = %#v, want task_1 Completed", state)
	}
	snapshot, ok, err := taskRuntime.AgentSnapshot(ctx)
	if err != nil || !ok {
		t.Fatalf("task runtime AgentSnapshot ok=%v err=%v, want snapshot", ok, err)
	}
	if snapshot.TaskID != "task_1" || snapshot.Agent != "worker" {
		t.Fatalf("snapshot = %#v, want task_1 worker", snapshot)
	}

	event, err := eventbus2.NewEvent("external", "probe", nil)
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	if _, err := taskRuntime.Publish(ctx, event); err != nil {
		t.Fatalf("task runtime Publish returned error: %v", err)
	}
	select {
	case got := <-delivered:
		if got.TaskID != "task_1" || got.Type != "probe" {
			t.Fatalf("delivered event = %#v, want task-scoped probe", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task-scoped event")
	}
}

func assertCompletedState(t *testing.T, rt *Runtime, taskID string) {
	t.Helper()
	state, ok, err := rt.State(context.Background(), taskID)
	if err != nil || !ok {
		t.Fatalf("State(%s) ok=%v err=%v, want state", taskID, ok, err)
	}
	if state.Phase != statemachine2.PhaseCompleted {
		t.Fatalf("State(%s) = %#v, want Completed", taskID, state)
	}
}

type completingAgent struct {
	name      string
	mu        sync.Mutex
	started   []string
	snapshots map[string]agents.AgentSnapshot
}

func newCompletingAgent(name string) *completingAgent {
	return &completingAgent{name: name, snapshots: make(map[string]agents.AgentSnapshot)}
}

func (a *completingAgent) Name() string {
	return a.name
}

func (a *completingAgent) Start(_ context.Context, input agents.AgentStartInput) (agents.AgentResult, error) {
	a.mu.Lock()
	a.started = append(a.started, input.TaskID)
	snapshot := agents.NewAgentSnapshot(a.name, input, time.Now().UTC())
	snapshot.Phase = agents.BusinessPhaseCompleted
	a.snapshots[input.TaskID] = snapshot.Clone()
	a.mu.Unlock()

	result, err := json.Marshal(map[string]string{"task_id": input.TaskID})
	if err != nil {
		return agents.AgentResult{}, err
	}
	event, err := eventbus2.NewEvent(statemachine2.TopicTask, statemachine2.EventAgentCompleted,
		statemachine2.AgentCompletedPayload{Result: json.RawMessage(result)},
		eventbus2.WithTaskID(input.TaskID),
		eventbus2.WithSource("test.agent"),
	)
	if err != nil {
		return agents.AgentResult{}, err
	}
	return agents.AgentResult{
		TaskID:   input.TaskID,
		Agent:    a.name,
		Snapshot: snapshot.Clone(),
		Events:   []eventbus2.Event{event},
	}, nil
}

func (a *completingAgent) Resume(_ context.Context, input agents.AgentResumeInput) (agents.AgentResult, error) {
	return agents.AgentResult{TaskID: input.TaskID, Agent: a.name}, nil
}

func (a *completingAgent) Snapshot(_ context.Context, taskID string) (agents.AgentSnapshot, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	snapshot, ok := a.snapshots[taskID]
	return snapshot.Clone(), ok, nil
}

func (a *completingAgent) startedTasks() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.started...)
}

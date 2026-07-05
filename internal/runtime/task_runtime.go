package runtime

import (
	"agent/internal/runtime/agents"
	eventbus2 "agent/internal/runtime/eventbus"
	"agent/internal/runtime/statemachine"
	"context"
	"fmt"
	"strings"
)

type TaskStart struct {
	Input       string            `json:"input,omitempty"`
	MaxFailures int               `json:"max_failures,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type TaskRuntime struct {
	runtime   *Runtime
	taskID    string
	agentName string
}

func (r *Runtime) CreateTaskRuntime(ctx context.Context, taskID string, agent agents.Agent, options ...RegisterAgentOption) (*TaskRuntime, error) {
	if r == nil {
		return nil, fmt.Errorf("runtime is nil")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, fmt.Errorf("create task runtime: task_id is required")
	}
	if agent == nil {
		return nil, fmt.Errorf("create task runtime %q: agent is required", taskID)
	}
	agentName := strings.TrimSpace(agent.Name())
	if agentName == "" {
		return nil, fmt.Errorf("create task runtime %q: agent name is required", taskID)
	}
	if err := r.RegisterAgent(ctx, taskID, agent, options...); err != nil {
		return nil, err
	}
	return &TaskRuntime{
		runtime:   r,
		taskID:    taskID,
		agentName: agentName,
	}, nil
}

func (r *Runtime) TaskRuntime(taskID string, agentName string) (*TaskRuntime, error) {
	if r == nil {
		return nil, fmt.Errorf("runtime is nil")
	}
	taskID = strings.TrimSpace(taskID)
	agentName = strings.TrimSpace(agentName)
	if taskID == "" {
		return nil, fmt.Errorf("task runtime: task_id is required")
	}
	if agentName == "" {
		runtime, err := r.runtimes.ResolveRuntime(context.Background(), eventbus2.Event{TaskID: taskID})
		if err != nil {
			return nil, err
		}
		agentName = runtime.Agent
	}
	if _, ok := r.taskAgents.Lookup(taskID, agentName); !ok {
		return nil, fmt.Errorf("task runtime: agent %q for task %q not found", agentName, taskID)
	}
	return &TaskRuntime{
		runtime:   r,
		taskID:    taskID,
		agentName: agentName,
	}, nil
}

func (t *TaskRuntime) TaskID() string {
	if t == nil {
		return ""
	}
	return t.taskID
}

func (t *TaskRuntime) AgentName() string {
	if t == nil {
		return ""
	}
	return t.agentName
}

func (t *TaskRuntime) Runtime() *Runtime {
	if t == nil {
		return nil
	}
	return t.runtime
}

func (t *TaskRuntime) Start(ctx context.Context, input TaskStart) (eventbus2.PublishResult, error) {
	if t == nil || t.runtime == nil {
		return eventbus2.PublishResult{}, fmt.Errorf("task runtime is nil")
	}
	return t.runtime.StartTask(ctx, Task{
		TaskID:      t.taskID,
		Agent:       t.agentName,
		Input:       input.Input,
		MaxFailures: input.MaxFailures,
		Metadata:    cloneStringMap(input.Metadata),
	})
}

func (t *TaskRuntime) StartInput(ctx context.Context, input string) (eventbus2.PublishResult, error) {
	return t.Start(ctx, TaskStart{Input: input})
}

func (t *TaskRuntime) Publish(ctx context.Context, event eventbus2.Event) (eventbus2.PublishResult, error) {
	if t == nil || t.runtime == nil {
		return eventbus2.PublishResult{}, fmt.Errorf("task runtime is nil")
	}
	event, err := t.scopedEvent(event)
	if err != nil {
		return eventbus2.PublishResult{}, err
	}
	return t.runtime.Publish(ctx, event)
}

func (t *TaskRuntime) PublishAsync(ctx context.Context, event eventbus2.Event) (eventbus2.PublishResult, error) {
	if t == nil || t.runtime == nil {
		return eventbus2.PublishResult{}, fmt.Errorf("task runtime is nil")
	}
	event, err := t.scopedEvent(event)
	if err != nil {
		return eventbus2.PublishResult{}, err
	}
	return t.runtime.PublishAsync(ctx, event)
}

func (t *TaskRuntime) State(ctx context.Context) (statemachine.TaskState, bool, error) {
	if t == nil || t.runtime == nil {
		return statemachine.TaskState{}, false, fmt.Errorf("task runtime is nil")
	}
	return t.runtime.State(ctx, t.taskID)
}

func (t *TaskRuntime) AgentSnapshot(ctx context.Context) (agents.AgentSnapshot, bool, error) {
	if t == nil || t.runtime == nil {
		return agents.AgentSnapshot{}, false, fmt.Errorf("task runtime is nil")
	}
	return t.runtime.AgentSnapshot(ctx, t.agentName, t.taskID)
}

func (t *TaskRuntime) AgentSnapshotFor(ctx context.Context, agentName string) (agents.AgentSnapshot, bool, error) {
	if t == nil || t.runtime == nil {
		return agents.AgentSnapshot{}, false, fmt.Errorf("task runtime is nil")
	}
	agentName = strings.TrimSpace(agentName)
	if agentName == "" {
		agentName = t.agentName
	}
	return t.runtime.AgentSnapshot(ctx, agentName, t.taskID)
}

func (t *TaskRuntime) Cancel() bool {
	if t == nil || t.runtime == nil || t.runtime.reactor == nil {
		return false
	}
	return t.runtime.reactor.CancelTask(t.taskID)
}

func (t *TaskRuntime) scopedEvent(event eventbus2.Event) (eventbus2.Event, error) {
	if t == nil {
		return eventbus2.Event{}, fmt.Errorf("task runtime is nil")
	}
	taskID := strings.TrimSpace(t.taskID)
	if taskID == "" {
		return eventbus2.Event{}, fmt.Errorf("task runtime task_id is required")
	}
	event = event.Clone()
	if strings.TrimSpace(event.TaskID) != "" && strings.TrimSpace(event.TaskID) != taskID {
		return eventbus2.Event{}, fmt.Errorf("event task_id %q does not match task runtime %q", event.TaskID, taskID)
	}
	event.TaskID = taskID
	return event, nil
}

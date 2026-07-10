package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/agents/builtinagents/prompt"
	"context"
	"fmt"
)

const TaskIntakeAgentName = "task-intake"

type TaskIntakeAgentOption = AgentOption

type TaskIntakeAgent struct {
	runtime *agentRuntime
}

func NewTaskIntakeAgent(options ...AgentOption) (*TaskIntakeAgent, error) {
	base := []AgentOption{
		WithAgentSource("agent.task_intake"),
	}
	base = append(base, options...)
	base = append(base,
		withAgentName(TaskIntakeAgentName),
		WithSystemPrompt(prompt.TaskIntakePrompt),
	)
	runtime, err := newAgentRuntime(agentRuntimeDefaults{
		name:        TaskIntakeAgentName,
		source:      "agent.task_intake",
		errorPrefix: "task intake agent",
	}, base...)
	if err != nil {
		return nil, err
	}
	return &TaskIntakeAgent{runtime: runtime}, nil
}

func (a *TaskIntakeAgent) Name() string {
	if a == nil || a.runtime == nil {
		return TaskIntakeAgentName
	}
	return a.runtime.agentName()
}

func (a *TaskIntakeAgent) Start(ctx context.Context, input agents2.AgentStartInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.start(ctx, input)
}

func (a *TaskIntakeAgent) Resume(ctx context.Context, input agents2.AgentResumeInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.resume(ctx, input)
}

func (a *TaskIntakeAgent) Snapshot(ctx context.Context, taskID string) (agents2.AgentSnapshot, bool, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentSnapshot{}, false, err
	}
	return runtime.snapshot(ctx, taskID)
}

func (a *TaskIntakeAgent) core() (*agentRuntime, error) {
	if a == nil || a.runtime == nil {
		return nil, fmt.Errorf("task intake agent is nil")
	}
	return a.runtime, nil
}

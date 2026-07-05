package builtinagents

import (
	"agent/internal/prompt"
	agents2 "agent/internal/runtime/agents"
	"context"
	"fmt"
)

const ToolsTesterAgentName = "tool-tester"

type ToolsTesterAgent struct {
	runtime *agentRuntime
}

func NewToolsTesterAgent(options ...AgentOption) (*ToolsTesterAgent, error) {
	base := []AgentOption{
		WithAgentSource("agent.tool_tester"),
	}
	base = append(base, options...)
	base = append(base,
		withAgentName(ToolsTesterAgentName),
		WithSystemPrompt(prompt.ToolsSystemPrompt),
	)
	runtime, err := newAgentRuntime(agentRuntimeDefaults{
		name:        ToolsTesterAgentName,
		source:      "agent.tool_tester",
		errorPrefix: "tool tester agent",
	}, base...)
	if err != nil {
		return nil, err
	}
	return &ToolsTesterAgent{runtime: runtime}, nil
}

func (a *ToolsTesterAgent) Name() string {
	if a == nil || a.runtime == nil {
		return ToolsTesterAgentName
	}
	return a.runtime.agentName()
}

func (a *ToolsTesterAgent) Start(ctx context.Context, input agents2.AgentStartInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.start(ctx, input)
}

func (a *ToolsTesterAgent) Resume(ctx context.Context, input agents2.AgentResumeInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.resume(ctx, input)
}

func (a *ToolsTesterAgent) Snapshot(ctx context.Context, taskID string) (agents2.AgentSnapshot, bool, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentSnapshot{}, false, err
	}
	return runtime.snapshot(ctx, taskID)
}

func (a *ToolsTesterAgent) core() (*agentRuntime, error) {
	if a == nil || a.runtime == nil {
		return nil, fmt.Errorf("tool tester agent is nil")
	}
	return a.runtime, nil
}

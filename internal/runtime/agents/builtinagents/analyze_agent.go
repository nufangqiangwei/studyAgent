package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"context"
	"fmt"
)

const AnalyzeAgentName = "analyze"

type AnalyzeAgent struct {
	runtime *agentRuntime
}

func NewAnalyzeAgent(options ...AgentOption) (*AnalyzeAgent, error) {
	runtime, err := newAgentRuntime(agentRuntimeDefaults{
		name:        AnalyzeAgentName,
		source:      "agent.analyze",
		errorPrefix: "analyze agent",
	}, options...)
	if err != nil {
		return nil, err
	}
	return &AnalyzeAgent{runtime: runtime}, nil
}

func (a *AnalyzeAgent) Name() string {
	if a == nil || a.runtime == nil {
		return AnalyzeAgentName
	}
	return a.runtime.agentName()
}

func (a *AnalyzeAgent) Start(ctx context.Context, input agents2.AgentStartInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.start(ctx, input)
}

func (a *AnalyzeAgent) Resume(ctx context.Context, input agents2.AgentResumeInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.resume(ctx, input)
}

func (a *AnalyzeAgent) Snapshot(ctx context.Context, taskID string) (agents2.AgentSnapshot, bool, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentSnapshot{}, false, err
	}
	return runtime.snapshot(ctx, taskID)
}

func (a *AnalyzeAgent) core() (*agentRuntime, error) {
	if a == nil || a.runtime == nil {
		return nil, fmt.Errorf("analyze agent is nil")
	}
	return a.runtime, nil
}

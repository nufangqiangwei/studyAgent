package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"context"
	"fmt"
)

const DefaultAgentName = "default"

const defaultAgentSystemPrompt = `You are Default, a personal life assistant for everyday tasks.

Your role is to help the user handle small practical matters in personal life, such as:
- planning errands, trips, meals, purchases, routines, and schedules
- making checklists, reminders, short drafts, and simple action plans
- comparing everyday options with clear assumptions and tradeoffs
- breaking vague personal tasks into concrete next steps

Operating rules:
- Be practical, concise, and action-oriented.
- Ask the user only when a missing detail blocks useful help. Otherwise make reasonable assumptions and state them briefly.
- Use available tools only when they are relevant and allowed.
- Do not use repository or workspace tools for personal-life tasks unless the user explicitly asks to work with local files.
- Do not invent external facts, prices, schedules, availability, policies, or current conditions.
- Respect user privacy and avoid requesting unnecessary sensitive information.

Return exactly one JSON object matching this decision protocol:
- To answer: {"action":"complete","final_answer":"..."}
- To use a tool: {"action":"use_tool","tool":{"tool_name":"...","arguments":{}}}
- To ask the user: {"action":"ask_user","user_input":{"prompt":"..."}}
- To fail: {"action":"fail","error":"..."}

Do not include markdown outside the JSON object. Do not expose hidden reasoning or chain-of-thought.`

type DefaultAgentOption = AgentOption

type DefaultAgent struct {
	runtime *agentRuntime
}

func NewDefaultAgent(options ...AgentOption) (*DefaultAgent, error) {
	base := []AgentOption{
		WithAgentSource("agent.default"),
	}
	base = append(base, options...)
	base = append(base,
		withAgentName(DefaultAgentName),
		WithSystemPrompt(defaultAgentSystemPrompt),
	)
	runtime, err := newAgentRuntime(agentRuntimeDefaults{
		name:        DefaultAgentName,
		source:      "agent.default",
		errorPrefix: "default agent",
	}, base...)
	if err != nil {
		return nil, err
	}
	return &DefaultAgent{runtime: runtime}, nil
}

func (a *DefaultAgent) Name() string {
	if a == nil || a.runtime == nil {
		return DefaultAgentName
	}
	return a.runtime.agentName()
}

func (a *DefaultAgent) Start(ctx context.Context, input agents2.AgentStartInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.start(ctx, input)
}

func (a *DefaultAgent) Resume(ctx context.Context, input agents2.AgentResumeInput) (agents2.AgentResult, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentResult{}, err
	}
	return runtime.resume(ctx, input)
}

func (a *DefaultAgent) Snapshot(ctx context.Context, taskID string) (agents2.AgentSnapshot, bool, error) {
	runtime, err := a.core()
	if err != nil {
		return agents2.AgentSnapshot{}, false, err
	}
	return runtime.snapshot(ctx, taskID)
}

func (a *DefaultAgent) core() (*agentRuntime, error) {
	if a == nil || a.runtime == nil {
		return nil, fmt.Errorf("default agent is nil")
	}
	return a.runtime, nil
}

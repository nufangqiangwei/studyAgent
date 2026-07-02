package agent

import (
	"agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/prompt"
	"context"
	"fmt"
)

const ToolsTesterAgentName = "tool-tester"

type ToolsTesterAgent struct {
	parts runtimeAgentParts
}

func NewToolsTesterAgent(ctx context.Context, opts CreatAgentOptions) (Agent, error) {
	parts, err := newRuntimeAgentParts(ctx, opts, ToolsTesterAgentName, prompt.Options{
		SystemPrompt: prompt.ToolsSystemPrompt,
		Model:        opts.Model,
	})
	if err != nil {
		return nil, err
	}
	return &ToolsTesterAgent{parts: parts}, nil
}

func (a *ToolsTesterAgent) Name() string {
	return ToolsTesterAgentName
}

func (a *ToolsTesterAgent) Tools() []tool.Tool {
	if a == nil {
		return nil
	}
	return cloneTools(a.parts.tools)
}

func (a *ToolsTesterAgent) Run(ctx context.Context, userInput string) error {
	if a == nil {
		return fmt.Errorf("tool tester agent: not initialized")
	}
	return runRuntimeAgent(ctx, a.parts, userInput)
}

func (a *ToolsTesterAgent) Submit(ctx context.Context, userInput string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("tool tester agent: not initialized")
	}
	return submitRuntimeAgent(ctx, a.parts, userInput)
}

func (a *ToolsTesterAgent) Recover(ctx context.Context) (content.AsyncRecoverResult, error) {
	if a == nil {
		return content.AsyncRecoverResult{}, fmt.Errorf("tool tester agent: not initialized")
	}
	return recoverRuntimeAgent(ctx, a.parts)
}

func (a *ToolsTesterAgent) Work(ctx context.Context) (content.AsyncWorkResult, error) {
	if a == nil {
		return content.AsyncWorkResult{}, fmt.Errorf("tool tester agent: not initialized")
	}
	return workRuntimeAgent(ctx, a.parts)
}

func (a *ToolsTesterAgent) Advance(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("tool tester agent: not initialized")
	}
	return advanceRuntimeAgent(ctx, a.parts, runID)
}

func (a *ToolsTesterAgent) DispatchNextEffect(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("tool tester agent: not initialized")
	}
	return dispatchRuntimeAgentEffect(ctx, a.parts, runID)
}

func (a *ToolsTesterAgent) SubmitUserInput(ctx context.Context, runID string, answer string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("tool tester agent: not initialized")
	}
	return submitRuntimeAgentUserInput(ctx, a.parts, runID, answer)
}

func (a *ToolsTesterAgent) SubmitUserApproval(ctx context.Context, runID string, approved bool, reason string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("tool tester agent: not initialized")
	}
	return submitRuntimeAgentUserApproval(ctx, a.parts, runID, approved, reason)
}

func (a *ToolsTesterAgent) Result(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("tool tester agent: not initialized")
	}
	return runtimeAgentResult(ctx, a.parts, runID)
}

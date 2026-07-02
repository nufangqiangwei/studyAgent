package agent

import (
	"agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/prompt"
	"context"
	"fmt"
)

const DefaultAgentName = "default"

type DefaultAgent struct {
	parts runtimeAgentParts
}

func NewDefaultAgent(ctx context.Context, opts CreatAgentOptions) (Agent, error) {
	parts, err := newRuntimeAgentParts(ctx, opts, DefaultAgentName, prompt.Options{
		Model: opts.Model,
	})
	if err != nil {
		return nil, err
	}
	return &DefaultAgent{parts: parts}, nil
}

func (a *DefaultAgent) Name() string {
	return DefaultAgentName
}

func (a *DefaultAgent) Tools() []tool.Tool {
	if a == nil {
		return nil
	}
	return cloneTools(a.parts.tools)
}

func (a *DefaultAgent) Run(ctx context.Context, userInput string) error {
	if a == nil {
		return fmt.Errorf("default agent: not initialized")
	}
	return runRuntimeAgent(ctx, a.parts, userInput)
}

func (a *DefaultAgent) Submit(ctx context.Context, userInput string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("default agent: not initialized")
	}
	return submitRuntimeAgent(ctx, a.parts, userInput)
}

func (a *DefaultAgent) Recover(ctx context.Context) (content.AsyncRecoverResult, error) {
	if a == nil {
		return content.AsyncRecoverResult{}, fmt.Errorf("default agent: not initialized")
	}
	return recoverRuntimeAgent(ctx, a.parts)
}

func (a *DefaultAgent) Work(ctx context.Context) (content.AsyncWorkResult, error) {
	if a == nil {
		return content.AsyncWorkResult{}, fmt.Errorf("default agent: not initialized")
	}
	return workRuntimeAgent(ctx, a.parts)
}

func (a *DefaultAgent) Advance(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("default agent: not initialized")
	}
	return advanceRuntimeAgent(ctx, a.parts, runID)
}

func (a *DefaultAgent) DispatchNextEffect(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("default agent: not initialized")
	}
	return dispatchRuntimeAgentEffect(ctx, a.parts, runID)
}

func (a *DefaultAgent) SubmitUserInput(ctx context.Context, runID string, answer string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("default agent: not initialized")
	}
	return submitRuntimeAgentUserInput(ctx, a.parts, runID, answer)
}

func (a *DefaultAgent) SubmitUserApproval(ctx context.Context, runID string, approved bool, reason string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("default agent: not initialized")
	}
	return submitRuntimeAgentUserApproval(ctx, a.parts, runID, approved, reason)
}

func (a *DefaultAgent) Result(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("default agent: not initialized")
	}
	return runtimeAgentResult(ctx, a.parts, runID)
}

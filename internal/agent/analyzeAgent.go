package agent

import (
	"agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/prompt"
	"context"
	"fmt"
)

const AnalyzeAgentName = "analyze"

type AnalyzeAgent struct {
	parts runtimeAgentParts
}

func NewAnalyzeAgent(ctx context.Context, opts CreatAgentOptions) (Agent, error) {
	parts, err := newRuntimeAgentParts(ctx, opts, AnalyzeAgentName, prompt.Options{
		SystemPrompt: prompt.AnalyzeSystemPrompt,
		Model:        opts.Model,
	})
	if err != nil {
		return nil, err
	}
	return &AnalyzeAgent{parts: parts}, nil
}

func (a *AnalyzeAgent) Name() string {
	return AnalyzeAgentName
}

func (a *AnalyzeAgent) Tools() []tool.Tool {
	if a == nil {
		return nil
	}
	return cloneTools(a.parts.tools)
}

func (a *AnalyzeAgent) Run(ctx context.Context, userInput string) error {
	if a == nil {
		return fmt.Errorf("analyze agent: not initialized")
	}
	return runRuntimeAgent(ctx, a.parts, userInput)
}

func (a *AnalyzeAgent) Submit(ctx context.Context, userInput string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("analyze agent: not initialized")
	}
	return submitRuntimeAgent(ctx, a.parts, userInput)
}

func (a *AnalyzeAgent) Recover(ctx context.Context) (content.AsyncRecoverResult, error) {
	if a == nil {
		return content.AsyncRecoverResult{}, fmt.Errorf("analyze agent: not initialized")
	}
	return recoverRuntimeAgent(ctx, a.parts)
}

func (a *AnalyzeAgent) Work(ctx context.Context) (content.AsyncWorkResult, error) {
	if a == nil {
		return content.AsyncWorkResult{}, fmt.Errorf("analyze agent: not initialized")
	}
	return workRuntimeAgent(ctx, a.parts)
}

func (a *AnalyzeAgent) Advance(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("analyze agent: not initialized")
	}
	return advanceRuntimeAgent(ctx, a.parts, runID)
}

func (a *AnalyzeAgent) DispatchNextEffect(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("analyze agent: not initialized")
	}
	return dispatchRuntimeAgentEffect(ctx, a.parts, runID)
}

func (a *AnalyzeAgent) SubmitUserInput(ctx context.Context, runID string, answer string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("analyze agent: not initialized")
	}
	return submitRuntimeAgentUserInput(ctx, a.parts, runID, answer)
}

func (a *AnalyzeAgent) SubmitUserApproval(ctx context.Context, runID string, approved bool, reason string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("analyze agent: not initialized")
	}
	return submitRuntimeAgentUserApproval(ctx, a.parts, runID, approved, reason)
}

func (a *AnalyzeAgent) Result(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	if a == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("analyze agent: not initialized")
	}
	return runtimeAgentResult(ctx, a.parts, runID)
}

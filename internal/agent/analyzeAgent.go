package agent

import (
	"agent/internal/capability/tool"
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

package agent

import (
	"agent/internal/capability/tool"
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

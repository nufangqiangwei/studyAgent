package agent

import (
	"agent/internal/capability/tool"
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

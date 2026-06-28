package agent

import (
	"agent/internal/capability/defaulttools"
	"agent/internal/capability/tool"
	"context"
	"fmt"

	"agent/internal/prompt"
)

const DefaultAgentName = "default"

type DefaultAgent struct {
	loop     *NativeLoop
	tools    []tool.Tool
	workPath string
}

func NewDefaultAgent(ctx context.Context, opts CreatAgentOptions) (Agent, error) {
	toolRegistry, err := defaulttools.NewRegistry(tool.WithPolicy(opts.Policy))
	if err != nil {
		return nil, fmt.Errorf("default agent: register default tool: %w", err)
	}
	registeredTools := toolRegistry.List()

	loop, err := NewNativeLoop(Options{
		LLM: opts.LLM,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{
			Model: opts.Model,
		}),
		Tools:    toolRegistry,
		Logger:   opts.Logger,
		MaxSteps: opts.MaxSteps,
		Out:      opts.Out,
		Session:  opts.Session,
	})
	if err != nil {
		return nil, fmt.Errorf("default agent: create native loop: %w", err)
	}

	return &DefaultAgent{
		loop:     loop,
		tools:    registeredTools,
		workPath: opts.WorkDir,
	}, nil
}

func (a *DefaultAgent) Name() string {
	return DefaultAgentName
}

func (a *DefaultAgent) Tools() []tool.Tool {
	if a == nil {
		return nil
	}
	return append([]tool.Tool(nil), a.tools...)
}

func (a *DefaultAgent) Run(ctx context.Context, userInput string) error {
	if a == nil || a.loop == nil {
		return fmt.Errorf("default agent: not initialized")
	}
	userTask := Task{
		Input:     userInput,
		WorkDir:   a.workPath,
		AgentName: a.Name(),
	}
	_, err := a.loop.Run(ctx, userTask)
	return err
}

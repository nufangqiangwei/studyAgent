package agent

import (
	"agent/internal/capability/defaulttools"
	"agent/internal/capability/tool"
	"agent/internal/prompt"
	"context"
	"fmt"
)

const AnalyzeAgentName = "analyze"

type AnalyzeAgent struct {
	loop     *NativeLoop
	tools    []tool.Tool
	workPath string
}

func NewAnalyzeAgent(ctx context.Context, opts CreatAgentOptions) (Agent, error) {
	toolRegistry, err := defaulttools.NewRegistry(tool.WithPolicy(opts.Policy))
	if err != nil {
		return nil, fmt.Errorf("analyze agent: register default tool: %w", err)
	}
	registeredTools := toolRegistry.List()

	loop, err := NewNativeLoop(Options{
		LLM: opts.LLM,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{
			SystemPrompt: prompt.AnalyzeSystemPrompt,
			Model:        opts.Model,
		}),
		Tools:    toolRegistry,
		Logger:   opts.Logger,
		MaxSteps: opts.MaxSteps,
		Out:      opts.Out,
		Session:  opts.Session,
	})
	if err != nil {
		return nil, fmt.Errorf("analyze agent: create native loop: %w", err)
	}

	return &AnalyzeAgent{
		loop:     loop,
		tools:    registeredTools,
		workPath: opts.WorkDir,
	}, nil
}

func (a *AnalyzeAgent) Name() string {
	return AnalyzeAgentName
}

func (a *AnalyzeAgent) Tools() []tool.Tool {
	if a == nil {
		return nil
	}
	return append([]tool.Tool(nil), a.tools...)
}

func (a *AnalyzeAgent) Run(ctx context.Context, userInput string) error {
	if a == nil || a.loop == nil {
		return fmt.Errorf("analyze agent: not initialized")
	}
	userTask := Task{
		Input:     userInput,
		WorkDir:   a.workPath,
		AgentName: a.Name(),
	}
	_, err := a.loop.Run(ctx, userTask)
	return err
}

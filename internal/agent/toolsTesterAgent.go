package agent

import (
	"context"
	"fmt"

	"agent/internal/prompt"
	"agent/internal/session"
	"agent/internal/tools"
)

const ToolsTesterAgentName = "tools-tester"

type ToolsTesterAgent struct {
	loop     *NativeLoop
	tools    []tools.Tool
	workPath string
}

func NewToolsTesterAgent(ctx context.Context, opts CreatAgentOptions) (Agent, error) {
	toolRegistry, err := tools.NewDefaultRegistry(tools.WithPolicy(opts.Policy))
	if err != nil {
		return nil, fmt.Errorf("tools tester agent: register default tools: %w", err)
	}
	registeredTools := toolRegistry.List()
	if opts.MaxSteps < 100 {
		opts.MaxSteps = 100
	}
	loop, err := NewNativeLoop(Options{
		LLM: opts.LLM,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{
			SystemPrompt: prompt.ToolsSystemPrompt,
			Model:        opts.Model,
		}),
		Tools:    toolRegistry,
		Logger:   opts.Logger,
		MaxSteps: opts.MaxSteps,
		Out:      opts.Out,
		Session:  opts.Session,
	})
	if err != nil {
		return nil, fmt.Errorf("tools tester agent: create native loop: %w", err)
	}

	return &ToolsTesterAgent{
		loop:     loop,
		tools:    registeredTools,
		workPath: opts.WorkDir,
	}, nil
}

func (a *ToolsTesterAgent) Name() string {
	return ToolsTesterAgentName
}

func (a *ToolsTesterAgent) Tools() []tools.Tool {
	if a == nil {
		return nil
	}
	return append([]tools.Tool(nil), a.tools...)
}

func (a *ToolsTesterAgent) Run(ctx context.Context, userInput string) error {
	if a == nil || a.loop == nil {
		return fmt.Errorf("tools tester agent: not initialized")
	}
	userTask := Task{
		Input:     userInput,
		WorkDir:   a.workPath,
		AgentName: a.Name(),
	}
	_, err := a.loop.Run(ctx, userTask)
	return err
}

func (a *ToolsTesterAgent) Resume(ctx context.Context, checkpoint session.ResumeCheckpoint) error {
	if a == nil || a.loop == nil {
		return fmt.Errorf("tools tester agent: not initialized")
	}
	_, err := a.loop.Resume(ctx, checkpoint)
	return err
}

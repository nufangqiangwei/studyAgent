package agent

import (
	"agent/internal/capability/tool"
	"agent/internal/runtime"
	"context"
	"fmt"

	"agent/internal/prompt"
)

const ToolsTesterAgentName = "tool-tester"

type ToolsTesterAgent struct {
	loop     *runtime.NativeLoop
	tools    []tool.Tool
	workPath string
}

func NewToolsTesterAgent(ctx context.Context, opts CreatAgentOptions) (Agent, error) {
	toolManage, err := tool.NewDefaultManage(tool.WithPolicy(opts.Policy))
	if err != nil {
		return nil, fmt.Errorf("tool tester agent: select tools: %w", err)
	}
	registeredTools := toolManage.List()
	if opts.MaxSteps < 100 {
		opts.MaxSteps = 100
	}
	loop, err := runtime.NewNativeLoop(runtime.Options{
		LLM: opts.LLM,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{
			SystemPrompt: prompt.ToolsSystemPrompt,
			Model:        opts.Model,
		}),
		Tools:    toolManage,
		Logger:   opts.Logger,
		MaxSteps: opts.MaxSteps,
		Out:      opts.Out,
		Session:  opts.Session,
	})
	if err != nil {
		return nil, fmt.Errorf("tool tester agent: create native loop: %w", err)
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

func (a *ToolsTesterAgent) Tools() []tool.Tool {
	if a == nil {
		return nil
	}
	return append([]tool.Tool(nil), a.tools...)
}

func (a *ToolsTesterAgent) Run(ctx context.Context, userInput string) error {
	if a == nil || a.loop == nil {
		return fmt.Errorf("tool tester agent: not initialized")
	}
	userTask := runtime.Task{
		Input:     userInput,
		WorkDir:   a.workPath,
		AgentName: a.Name(),
	}
	_, err := a.loop.Run(ctx, userTask)
	return err
}

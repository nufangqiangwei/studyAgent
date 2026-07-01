package agent

import (
	"agent/internal/agent/runner"
	"agent/internal/capability/tool"
	"agent/internal/foundation/llmClient"
	"agent/internal/llm"
	"agent/internal/prompt"
	"agent/internal/state"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

type runtimeAgentParts struct {
	runtime          *llm.Runtime
	promptBuilder    *prompt.NativeBuilder
	toolRegistry     *tool.Manage
	tools            []tool.Tool
	profile          llm.AgentProfile
	workPath         string
	out              io.Writer
	maxSteps         int
	runtimeStoreRoot string
}

func newRuntimeAgentParts(ctx context.Context, opts CreatAgentOptions, agentName string, promptOptions prompt.Options) (runtimeAgentParts, error) {
	_ = ctx
	toolManage, err := tool.NewDefaultManage(tool.WithPolicy(opts.Policy))
	if err != nil {
		return runtimeAgentParts{}, fmt.Errorf("%s agent: select tools: %w", agentName, err)
	}
	registeredTools := toolManage.List()

	rt, err := llm.NewRuntime(llm.Options{
		LLM:    opts.LLM,
		Logger: opts.Logger,
	})
	if err != nil {
		return runtimeAgentParts{}, fmt.Errorf("%s agent: create llm: %w", agentName, err)
	}

	if promptOptions.Model == "" {
		promptOptions.Model = opts.Model
	}
	builder := prompt.NewNativeBuilder(promptOptions)

	return runtimeAgentParts{
		runtime:       rt,
		promptBuilder: builder,
		toolRegistry:  toolManage,
		tools:         registeredTools,
		profile: llm.AgentProfile{
			Name:  agentName,
			Model: opts.Model,
			Tools: toolDefinitions(registeredTools),
		},
		workPath:         opts.WorkDir,
		out:              opts.Out,
		maxSteps:         opts.MaxSteps,
		runtimeStoreRoot: resolveRuntimeStoreRoot(opts.Session),
	}, nil
}

func runRuntimeAgent(ctx context.Context, parts runtimeAgentParts, userInput string) error {
	if parts.runtime == nil || parts.promptBuilder == nil {
		return fmt.Errorf("agent llm: not initialized")
	}

	promptOutput, err := parts.promptBuilder.Build(ctx, prompt.Input{
		Task:    userInput,
		WorkDir: parts.workPath,
	})
	if err != nil {
		return err
	}

	profile := parts.profile
	profile.Model = promptOutput.Model
	profile.Temperature = promptOutput.Temperature

	runnerOpts := runner.Options{
		LLM:          parts.runtime,
		ToolRegistry: parts.toolRegistry,
		MaxSteps:     parts.maxSteps,
	}
	if parts.runtimeStoreRoot != "" {
		stores, err := state.NewFileStore(parts.runtimeStoreRoot)
		if err != nil {
			return fmt.Errorf("create runtime state store: %w", err)
		}
		runnerOpts.StateStore = stores.States
		runnerOpts.EventStore = stores.Events
		runnerOpts.EffectStore = stores.Effects
		runnerOpts.EventInbox = stores.Inbox
	}

	reactRunner, err := runner.NewAgentRunner(runnerOpts)
	if err != nil {
		return err
	}

	result, err := reactRunner.Run(ctx, runner.Task{
		Input:    userInput,
		Agent:    profile,
		Messages: promptOutput.Messages,
		MaxSteps: parts.maxSteps,
	})
	if err != nil {
		return err
	}
	if parts.out != nil && strings.TrimSpace(result.FinalAnswer) != "" {
		if _, err := fmt.Fprintln(parts.out, result.FinalAnswer); err != nil {
			return fmt.Errorf("write agent output: %w", err)
		}
	}
	return nil
}

type sessionDirProvider interface {
	SessionDir() string
}

func resolveRuntimeStoreRoot(recorder interface{}) string {
	provider, ok := recorder.(sessionDirProvider)
	if !ok {
		return ""
	}
	sessionDir := strings.TrimSpace(provider.SessionDir())
	if sessionDir == "" {
		return ""
	}
	return filepath.Join(sessionDir, "runtime")
}

func toolDefinitions(tools []tool.Tool) []llmClient.ToolDefinition {
	if len(tools) == 0 {
		return nil
	}
	defs := make([]llmClient.ToolDefinition, 0, len(tools))
	for _, t := range tools {
		defs = append(defs, llmClient.ToolDefinition{
			Name:        t.Name(),
			Description: t.Description(),
			InputSchema: t.InputSchema(),
		})
	}
	return defs
}

func cloneTools(tools []tool.Tool) []tool.Tool {
	if len(tools) == 0 {
		return nil
	}
	return append([]tool.Tool(nil), tools...)
}

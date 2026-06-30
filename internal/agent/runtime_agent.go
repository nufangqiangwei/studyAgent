package agent

import (
	"agent/internal/capability/tool"
	"agent/internal/foundation/llmClient"
	"agent/internal/llm"
	"agent/internal/prompt"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"
)

type runtimeAgentParts struct {
	runtime       *llm.Runtime
	promptBuilder *prompt.NativeBuilder
	tools         []tool.Tool
	profile       llm.AgentProfile
	workPath      string
	out           io.Writer
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
		tools:         registeredTools,
		profile: llm.AgentProfile{
			Name:  agentName,
			Model: opts.Model,
			Tools: toolDefinitions(registeredTools),
		},
		workPath: opts.WorkDir,
		out:      opts.Out,
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

	result, err := parts.runtime.CallModel(ctx, llm.ModelCallInput{
		RunID:    newRunID(),
		Step:     1,
		Agent:    profile,
		Messages: promptOutput.Messages,
	})
	if err != nil {
		return err
	}
	if len(result.ToolCalls) > 0 {
		return fmt.Errorf("agent %s: model requested %d tool call(s), but tool execution is handled by the external runner", profile.Name, len(result.ToolCalls))
	}
	if parts.out != nil && strings.TrimSpace(result.Response.Content) != "" {
		if _, err := fmt.Fprintln(parts.out, result.Response.Content); err != nil {
			return fmt.Errorf("write agent output: %w", err)
		}
	}
	return nil
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

func newRunID() string {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "run_" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return "run_" + hex.EncodeToString(raw[:])
}

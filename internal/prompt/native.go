package prompt

import (
	"context"
	"fmt"
	"strings"

	"agent/internal/llm"
)

type Options struct {
	SystemPrompt string
	Model        string
	Temperature  float64
}

type Input struct {
	Task    string
	WorkDir string
}

type Output struct {
	Model       string
	Messages    []llm.Message
	Temperature float64
	DebugText   string
}

type NativeBuilder struct {
	systemPrompt string
	model        string
	temperature  float64
}

func NewNativeBuilder(opts Options) *NativeBuilder {
	systemPrompt := opts.SystemPrompt
	if systemPrompt == "" {
		systemPrompt = defaultSystemPrompt
	}

	model := opts.Model
	if model == "" {
		model = "mock-native"
	}

	temperature := opts.Temperature
	if temperature == 0 {
		temperature = 0.2
	}

	return &NativeBuilder{
		systemPrompt: systemPrompt,
		model:        model,
		temperature:  temperature,
	}
}

func (b *NativeBuilder) Build(_ context.Context, input Input) (Output, error) {
	task := strings.TrimSpace(input.Task)
	if task == "" {
		return Output{}, fmt.Errorf("prompt: task is required")
	}

	userPrompt := fmt.Sprintf(`Task:
%s

Workspace:
%s

Respond with the next best action or final answer. Keep module boundaries and testability in mind.`, task, input.WorkDir)

	messages := []llm.Message{
		{Role: llm.RoleSystem, Content: b.systemPrompt},
		{Role: llm.RoleUser, Content: userPrompt},
	}

	return Output{
		Model:       b.model,
		Messages:    messages,
		Temperature: b.temperature,
		DebugText:   b.systemPrompt + "\n\n" + userPrompt,
	}, nil
}

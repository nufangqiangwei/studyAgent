package agent

import (
	"context"
	"encoding/json"
	"fmt"
	systemIO "io"
	"strings"
	"sync"
	"time"

	"agent/internal/llm"
	"agent/internal/prompt"
	"agent/internal/session"
	"agent/internal/tools"
)

type LLMClient interface {
	Complete(ctx context.Context, req llm.Request) (llm.Response, error)
}

type PromptBuilder interface {
	Build(ctx context.Context, input prompt.Input) (prompt.Output, error)
}

type ToolRegistry interface {
	Execute(ctx context.Context, name string, input json.RawMessage) (tools.Result, error)
	List() []tools.Tool
}

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type Options struct {
	LLM           LLMClient
	PromptBuilder PromptBuilder
	Tools         ToolRegistry
	Logger        Logger
	MaxSteps      int
	Out           systemIO.Writer
	Session       session.Recorder
}

type NativeLoop struct {
	mu            sync.Mutex
	llm           LLMClient
	promptBuilder PromptBuilder
	tools         ToolRegistry
	toolDefs      []llm.ToolDefinition
	logger        Logger
	maxSteps      int
	out           systemIO.Writer
	session       session.Recorder
	history       []llm.Message
}

type Task struct {
	Input     string
	WorkDir   string
	AgentName string
}

type Result struct {
	Content string
	Steps   []Step
}

type Step struct {
	Index       int
	StartedAt   time.Time
	CompletedAt time.Time
	PromptText  string
	Output      string
	ToolCalls   []ToolCall
	ToolResults []ToolResult
}

type ToolCall struct {
	ID    string
	Name  string
	Input json.RawMessage
}

type ToolResult struct {
	Name        string
	StartedAt   time.Time
	CompletedAt time.Time
	Content     string
	Metadata    map[string]any
	Error       string
}

func NewNativeLoop(opts Options) (*NativeLoop, error) {
	if opts.LLM == nil {
		return nil, fmt.Errorf("native loop: llm client is required")
	}
	if opts.PromptBuilder == nil {
		return nil, fmt.Errorf("native loop: prompt builder is required")
	}
	maxSteps := opts.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 1
	}
	toolDefs := toolDefinitions(opts.Tools)
	return &NativeLoop{
		llm:           opts.LLM,
		promptBuilder: opts.PromptBuilder,
		tools:         opts.Tools,
		toolDefs:      toolDefs,
		logger:        opts.Logger,
		maxSteps:      maxSteps,
		out:           opts.Out,
		session:       opts.Session,
	}, nil
}

func (l *NativeLoop) Run(ctx context.Context, task Task) (Result, error) {
	if task.Input == "" {
		return Result{}, fmt.Errorf("native loop: task input is required")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	result := Result{}
	promptOutput, err := l.promptBuilder.Build(ctx, prompt.Input{
		Task:    task.Input,
		WorkDir: task.WorkDir,
	})
	if err != nil {
		return Result{}, fmt.Errorf("build prompt: %w", err)
	}
	turnID, err := session.NewID()
	if err != nil {
		return Result{}, err
	}
	messages, generatedMessages := l.initialMessages(promptOutput.Messages)
	for _, msg := range generatedMessages {
		if err := l.saveMessage(ctx, task, turnID, 0, msg); err != nil {
			return Result{}, err
		}
	}

	var totalUsage llm.Usage
	llmCalls := 0

	for step := 1; step <= l.maxSteps; step++ {
		if l.logger != nil {
			l.logger.Debugf("native loop step %d", step)
		}

		stepStartedAt := time.Now().UTC()
		response, err := l.llm.Complete(ctx, llm.Request{
			Model:       promptOutput.Model,
			Messages:    messages,
			Tools:       l.toolDefs,
			Temperature: promptOutput.Temperature,
			Metadata: map[string]string{
				"loop": "native",
				"step": fmt.Sprintf("%d", step),
			},
		})
		if err != nil {
			return Result{}, fmt.Errorf("llm complete: %w", err)
		}
		stepCompletedAt := time.Now().UTC()
		llmCalls++
		if response.Usage != nil {
			totalUsage = totalUsage.Add(*response.Usage)
		}

		currentStep := Step{
			Index:       step,
			StartedAt:   stepStartedAt,
			CompletedAt: stepCompletedAt,
			PromptText:  promptOutput.DebugText,
			Output:      response.Content,
		}
		result.Content = response.Content
		toolCalls := normalizeToolCalls(response.ToolCalls, step)
		if msg, ok := assistantMessage(response, toolCalls); ok {
			messages = append(messages, msg)
			if err := l.saveMessage(ctx, task, turnID, step, msg); err != nil {
				return Result{}, err
			}
		}

		if len(toolCalls) == 0 {
			result.Steps = append(result.Steps, currentStep)
			l.history = append([]llm.Message(nil), messages...)
			if err := l.saveUsageSummary(ctx, task, turnID, totalUsage, llmCalls); err != nil {
				return Result{}, err
			}
			if err := l.writeOutput(response.Content); err != nil {
				return Result{}, err
			}
			return result, nil
		}

		if l.tools == nil {
			return Result{}, fmt.Errorf("native loop: tool registry is required for tool calls")
		}

		for _, call := range toolCalls {
			currentStep.ToolCalls = append(currentStep.ToolCalls, ToolCall{
				ID:    call.ID,
				Name:  call.Name,
				Input: call.Input,
			})

			toolStartedAt := time.Now().UTC()
			toolResult, err := l.tools.Execute(ctx, call.Name, call.Input)
			toolCompletedAt := time.Now().UTC()
			recorded := ToolResult{
				Name:        call.Name,
				StartedAt:   toolStartedAt,
				CompletedAt: toolCompletedAt,
			}
			if err != nil {
				recorded.Error = err.Error()
			} else {
				recorded.Content = toolResult.Content
				recorded.Metadata = toolResult.Metadata
			}
			currentStep.ToolResults = append(currentStep.ToolResults, recorded)
			toolMessage := toolResultMessage(call, recorded)
			messages = append(messages, toolMessage)
			if err := l.saveMessage(ctx, task, turnID, step, toolMessage); err != nil {
				return Result{}, err
			}
		}
		result.Steps = append(result.Steps, currentStep)

		if step == l.maxSteps {
			l.history = append([]llm.Message(nil), messages...)
			if err := l.saveUsageSummary(ctx, task, turnID, totalUsage, llmCalls); err != nil {
				return Result{}, err
			}
			return Result{}, fmt.Errorf("native loop: reached max steps after tool calls")
		}
	}

	return result, nil
}

func (l *NativeLoop) saveMessage(ctx context.Context, task Task, turnID string, stepIndex int, message llm.Message) error {
	if l.session == nil {
		return nil
	}
	msg := cloneMessage(message)
	err := l.session.Save(ctx, session.Record{
		AgentName: task.AgentName,
		Task:      task.Input,
		WorkDir:   task.WorkDir,
		TurnID:    turnID,
		StepIndex: stepIndex,
		Kind:      session.RecordKindMessage,
		Timestamp: time.Now().UTC(),
		Message:   &msg,
		Usage:     cloneUsage(message.Usage),
	})
	if err != nil {
		return fmt.Errorf("save session message: %w", err)
	}
	return nil
}

func (l *NativeLoop) saveUsageSummary(ctx context.Context, task Task, turnID string, usage llm.Usage, llmCalls int) error {
	if l.session == nil {
		return nil
	}
	err := l.session.Save(ctx, session.Record{
		AgentName:    task.AgentName,
		Task:         task.Input,
		WorkDir:      task.WorkDir,
		TurnID:       turnID,
		Kind:         session.RecordKindUsageSummary,
		Timestamp:    time.Now().UTC(),
		UsageSummary: &usage,
		LLMCalls:     llmCalls,
	})
	if err != nil {
		return fmt.Errorf("save session usage summary: %w", err)
	}
	return nil
}

func (l *NativeLoop) writeOutput(content string) error {
	if l.out == nil || strings.TrimSpace(content) == "" {
		return nil
	}
	if _, err := fmt.Fprintln(l.out, content); err != nil {
		return fmt.Errorf("write agent output: %w", err)
	}
	return nil
}

func (l *NativeLoop) initialMessages(promptMessages []llm.Message) ([]llm.Message, []llm.Message) {
	messages := append([]llm.Message(nil), l.history...)
	generated := []llm.Message{}
	if len(messages) == 0 {
		for _, msg := range promptMessages {
			if msg.Role == llm.RoleSystem {
				messages = append(messages, msg)
				generated = append(generated, msg)
			}
		}
	}

	for _, msg := range promptMessages {
		if msg.Role != llm.RoleSystem {
			messages = append(messages, msg)
			generated = append(generated, msg)
		}
	}
	return messages, generated
}

func assistantMessage(response llm.Response, toolCalls []llm.ToolCall) (llm.Message, bool) {
	if strings.TrimSpace(response.Content) == "" && len(toolCalls) == 0 {
		return llm.Message{}, false
	}
	return llm.Message{
		Role:      llm.RoleAssistant,
		Content:   response.Content,
		ToolCalls: toolCalls,
		Usage:     cloneUsage(response.Usage),
	}, true
}

func normalizeToolCalls(calls []llm.ToolCall, step int) []llm.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	normalized := make([]llm.ToolCall, 0, len(calls))
	for i, call := range calls {
		if call.ID == "" {
			call.ID = fmt.Sprintf("call_%d_%d", step, i+1)
		}
		if strings.TrimSpace(string(call.Input)) == "" {
			call.Input = json.RawMessage(`{}`)
		}
		normalized = append(normalized, call)
	}
	return normalized
}

func toolResultMessage(call llm.ToolCall, result ToolResult) llm.Message {
	content := result.Content
	if result.Error != "" {
		content = "error: " + result.Error
	}
	return llm.Message{
		Role:       llm.RoleTool,
		Name:       call.Name,
		Content:    content,
		ToolCallID: call.ID,
	}
}

func cloneMessage(message llm.Message) llm.Message {
	cloned := message
	cloned.ToolCalls = cloneLLMToolCalls(message.ToolCalls)
	cloned.Usage = cloneUsage(message.Usage)
	return cloned
}

func cloneLLMToolCalls(calls []llm.ToolCall) []llm.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	cloned := make([]llm.ToolCall, 0, len(calls))
	for _, call := range calls {
		cloned = append(cloned, llm.ToolCall{
			ID:    call.ID,
			Name:  call.Name,
			Input: append(json.RawMessage(nil), call.Input...),
		})
	}
	return cloned
}

func cloneUsage(usage *llm.Usage) *llm.Usage {
	if usage == nil {
		return nil
	}
	cloned := *usage
	return &cloned
}

func toolDefinitions(registry ToolRegistry) []llm.ToolDefinition {
	if registry == nil {
		return nil
	}

	registered := registry.List()
	defs := make([]llm.ToolDefinition, 0, len(registered))
	for _, tool := range registered {
		defs = append(defs, llm.ToolDefinition{
			Name:        tool.Name(),
			Description: tool.Description(),
			InputSchema: tool.InputSchema(),
		})
	}
	return defs
}

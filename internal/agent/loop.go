package agent

import (
	"agent/internal/content"
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
	LLM            LLMClient
	PromptBuilder  PromptBuilder
	ContextBuilder ContextBuilder
	Tools          ToolRegistry
	Logger         Logger
	MaxSteps       int
	Out            systemIO.Writer
	Session        session.Recorder
}

type NativeLoop struct {
	mu             sync.Mutex
	llm            LLMClient
	promptBuilder  PromptBuilder
	contextBuilder ContextBuilder
	tools          ToolRegistry
	toolDefs       []llm.ToolDefinition
	logger         Logger
	maxSteps       int
	out            systemIO.Writer
	session        session.Recorder
	history        []llm.Message
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

type llmRequestEventPayload struct {
	Request llm.Request `json:"request"`
}

type llmResponseEventPayload struct {
	Response    llm.Response `json:"response"`
	StartedAt   time.Time    `json:"started_at"`
	CompletedAt time.Time    `json:"completed_at"`
}

type toolCallEventPayload struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type toolResultEventPayload struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	StartedAt   time.Time      `json:"started_at"`
	CompletedAt time.Time      `json:"completed_at"`
	Content     string         `json:"content,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Error       string         `json:"error,omitempty"`
}

type summaryEventPayload struct {
	Usage    llm.Usage `json:"usage"`
	LLMCalls int       `json:"llm_calls"`
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
	contextBuilder := opts.ContextBuilder
	if contextBuilder == nil {
		contextBuilder = NewNativeContextBuilder()
	}
	toolDefs := toolDefinitions(opts.Tools)
	return &NativeLoop{
		llm:            opts.LLM,
		promptBuilder:  opts.PromptBuilder,
		contextBuilder: contextBuilder,
		tools:          opts.Tools,
		toolDefs:       toolDefs,
		logger:         opts.Logger,
		maxSteps:       maxSteps,
		out:            opts.Out,
		session:        opts.Session,
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
	sessionRecords, err := l.loadSessionRecords(ctx)
	if err != nil {
		return Result{}, err
	}
	llmContext, err := l.contextBuilder.Build(ctx, ContextInput{
		Prompt:         promptOutput,
		SessionRecords: sessionRecords,
		History:        l.history,
		Tools:          l.toolDefs,
	})
	if err != nil {
		return Result{}, fmt.Errorf("build llm context: %w", err)
	}
	if llmContext == nil {
		return Result{}, fmt.Errorf("build llm context: nil context")
	}
	for _, msg := range llmContext.InitialMessages() {
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
		request := llmContext.BuildRequest(RunState{
			Task:      task,
			TurnID:    turnID,
			StepIndex: step,
		})
		if err := l.saveEvent(ctx, task, turnID, step, session.EventTypeLLMRequest, llmRequestEventPayload{
			Request: request,
		}); err != nil {
			return Result{}, err
		}
		response, err := l.llm.Complete(ctx, request)
		if err != nil {
			return Result{}, fmt.Errorf("llm complete: %w", err)
		}
		stepCompletedAt := time.Now().UTC()
		llmCalls++
		if response.Usage != nil {
			totalUsage = totalUsage.Add(*response.Usage)
		}
		if err := l.saveEvent(ctx, task, turnID, step, session.EventTypeLLMResponse, llmResponseEventPayload{
			Response:    response,
			StartedAt:   stepStartedAt,
			CompletedAt: stepCompletedAt,
		}); err != nil {
			return Result{}, err
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
		if msg, ok := llmContext.AddAssistantResponse(response, toolCalls); ok {
			if err := l.saveMessage(ctx, task, turnID, step, msg); err != nil {
				return Result{}, err
			}
		}

		if len(toolCalls) == 0 {
			result.Steps = append(result.Steps, currentStep)
			l.history = llmContext.History()
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
			if err := l.saveEvent(ctx, task, turnID, step, session.EventTypeToolCall, toolCallEventPayload{
				ID:    call.ID,
				Name:  call.Name,
				Input: append(json.RawMessage(nil), call.Input...),
			}); err != nil {
				return Result{}, err
			}

			toolStartedAt := time.Now().UTC()
			toolCtx := l.toolExecutionContext(ctx, task, turnID, step)
			toolResult, err := l.tools.Execute(toolCtx, call.Name, call.Input)
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
			if err := l.saveEvent(ctx, task, turnID, step, session.EventTypeToolResult, toolResultEventPayload{
				ID:          call.ID,
				Name:        call.Name,
				StartedAt:   toolStartedAt,
				CompletedAt: toolCompletedAt,
				Content:     recorded.Content,
				Metadata:    recorded.Metadata,
				Error:       recorded.Error,
			}); err != nil {
				return Result{}, err
			}
			toolMessage := llmContext.AddToolResult(call, recorded)
			if err := l.saveMessage(ctx, task, turnID, step, toolMessage); err != nil {
				return Result{}, err
			}
		}
		result.Steps = append(result.Steps, currentStep)

		if step == l.maxSteps {
			l.history = llmContext.History()
			if err := l.saveUsageSummary(ctx, task, turnID, totalUsage, llmCalls); err != nil {
				return Result{}, err
			}
			return Result{}, fmt.Errorf("native loop: reached max steps after tool calls")
		}
	}

	return result, nil
}

func (l *NativeLoop) loadSessionRecords(ctx context.Context) ([]session.Record, error) {
	if len(l.history) > 0 || l.session == nil {
		return nil, nil
	}
	loader, ok := l.session.(session.Loader)
	if !ok {
		return nil, nil
	}
	records, err := loader.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load session history: %w", err)
	}
	return records, nil
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
	if err := l.saveEvent(ctx, task, turnID, 0, session.EventTypeSummary, summaryEventPayload{
		Usage:    usage,
		LLMCalls: llmCalls,
	}); err != nil {
		return err
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

func (l *NativeLoop) saveEvent(ctx context.Context, task Task, turnID string, step int, eventType string, payload any) error {
	if l.session == nil {
		return nil
	}
	err := session.SaveEvent(ctx, l.session, session.EventScope{
		TurnID:    turnID,
		Task:      task.Input,
		WorkDir:   task.WorkDir,
		AgentName: task.AgentName,
		Step:      step,
	}, eventType, payload)
	if err != nil {
		return fmt.Errorf("save session event %s: %w", eventType, err)
	}
	return nil
}

func (l *NativeLoop) toolExecutionContext(ctx context.Context, task Task, turnID string, step int) context.Context {
	scope := session.EventScope{
		TurnID:    turnID,
		Task:      task.Input,
		WorkDir:   task.WorkDir,
		AgentName: task.AgentName,
		Step:      step,
	}
	nextCtx, env := content.WithUpdatedEnv(ctx, func(env *content.Env) {
		if env.Session == nil {
			env.Session = l.session
		}
		env.EventScope = scope
	})
	if env != nil && env.Config.AgentName == "" {
		env.Config.AgentName = task.AgentName
	}
	return nextCtx
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

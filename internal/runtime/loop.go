package runtime

import (
	"agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/foundation/llmClient"
	"context"
	"encoding/json"
	"fmt"
	systemIO "io"
	"strings"
	"sync"
	"time"

	"agent/internal/prompt"
	"agent/internal/session"
)

type LLMClient interface {
	Complete(ctx context.Context, req llmClient.Request) (llmClient.Response, error)
}

type PromptBuilder interface {
	Build(ctx context.Context, input prompt.Input) (prompt.Output, error)
}

type ToolRegistry interface {
	Execute(ctx context.Context, name string, input json.RawMessage) (tool.Result, error)
	List() []tool.Tool
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
	toolDefs       []llmClient.ToolDefinition
	logger         Logger
	maxSteps       int
	out            systemIO.Writer
	session        session.Recorder
	history        []llmClient.Message
	states         map[string]RunState
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
	Request llmClient.Request `json:"request"`
}

type llmResponseEventPayload struct {
	Response    llmClient.Response `json:"response"`
	StartedAt   time.Time          `json:"started_at"`
	CompletedAt time.Time          `json:"completed_at"`
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
	Usage    llmClient.Usage `json:"usage"`
	LLMCalls int             `json:"llm_calls"`
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
		states:         make(map[string]RunState),
	}, nil
}

// Run is a synchronous compatibility wrapper over the event-driven runtime.
// The actual loop progresses through HandleEvent and action result events.
func (l *NativeLoop) Run(ctx context.Context, task Task) (Result, error) {
	advance, err := l.HandleEvent(ctx, NewRunStartedEvent(task))
	if err != nil {
		return Result{}, err
	}

	for {
		if len(advance.Actions) > 0 {
			actions := advance.Actions
			advance.Actions = nil
			for _, action := range actions {
				nextEvent, err := l.executeAction(ctx, action)
				if err != nil {
					return Result{}, err
				}
				if nextEvent == nil {
					continue
				}
				advance, err = l.HandleEvent(ctx, nextEvent)
				if err != nil {
					return Result{}, err
				}
			}
			continue
		}

		if advance.Result != nil && advance.Status == RunStatusCompleted {
			return *advance.Result, nil
		}
		if advance.Status == RunStatusStepLimitReached {
			return Result{}, fmt.Errorf("native loop: reached max steps after tool calls")
		}
		if advance.Status == RunStatusFailed {
			if advance.State.Summary != "" {
				return Result{}, fmt.Errorf("native loop: %s", advance.State.Summary)
			}
			return Result{}, fmt.Errorf("native loop: run failed")
		}
		return Result{}, fmt.Errorf("native loop: suspended with status %s", advance.Status)
	}
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

func (l *NativeLoop) saveMessage(ctx context.Context, task Task, turnID string, stepIndex int, message llmClient.Message) error {
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

func (l *NativeLoop) saveUsageSummary(ctx context.Context, task Task, turnID string, usage llmClient.Usage, llmCalls int) error {
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

func normalizeToolCalls(calls []llmClient.ToolCall, step int) []llmClient.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	normalized := make([]llmClient.ToolCall, 0, len(calls))
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

func toolDefinitions(registry ToolRegistry) []llmClient.ToolDefinition {
	if registry == nil {
		return nil
	}

	registered := registry.List()
	defs := make([]llmClient.ToolDefinition, 0, len(registered))
	for _, tool := range registered {
		defs = append(defs, llmClient.ToolDefinition{
			Name:        tool.Name(),
			Description: tool.Description(),
			InputSchema: tool.InputSchema(),
		})
	}
	return defs
}

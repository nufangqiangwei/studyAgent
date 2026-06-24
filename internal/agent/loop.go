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
	Compressor     SessionCompressor
	TokenCounter   ContextTokenCounter
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
	compressor     SessionCompressor
	tokenCounter   ContextTokenCounter
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
	compressor := opts.Compressor
	if compressor == nil {
		compressor = NewLLMSessionCompressor(opts.LLM)
	}
	tokenCounter := opts.TokenCounter
	if tokenCounter == nil {
		tokenCounter = EstimatedContextTokenCounter{}
	}
	toolDefs := toolDefinitions(opts.Tools)
	return &NativeLoop{
		llm:            opts.LLM,
		promptBuilder:  opts.PromptBuilder,
		contextBuilder: contextBuilder,
		compressor:     compressor,
		tokenCounter:   tokenCounter,
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

	return l.runFromStep(ctx, task, turnID, 1, promptOutput, llmContext, llm.Usage{}, 0)
}

func (l *NativeLoop) Resume(ctx context.Context, checkpoint session.ResumeCheckpoint) (Result, error) {
	if checkpoint.TurnID == "" {
		return Result{}, fmt.Errorf("native loop resume: turn id is required")
	}
	if checkpoint.Task == "" {
		return Result{}, fmt.Errorf("native loop resume: task is required")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	task := Task{
		Input:     checkpoint.Task,
		WorkDir:   checkpoint.WorkDir,
		AgentName: checkpoint.AgentName,
	}
	promptOutput, err := l.promptBuilder.Build(ctx, prompt.Input{
		Task:    task.Input,
		WorkDir: task.WorkDir,
	})
	if err != nil {
		return Result{}, fmt.Errorf("build prompt: %w", err)
	}
	resumePrompt := promptOutput
	resumePrompt.Messages = nil

	records := cloneSessionRecords(checkpoint.TurnRecords)
	if len(records) == 0 {
		records = recordsForTurn(checkpoint.Records, checkpoint.TurnID)
	}
	if len(records) == 0 {
		return Result{}, fmt.Errorf("native loop resume: no records for turn %s", checkpoint.TurnID)
	}
	records, err = l.materializeResumeMessages(ctx, task, checkpoint.TurnID, records)
	if err != nil {
		return Result{}, err
	}

	history := messagesFromSession(records)
	if len(history) == 0 {
		return Result{}, fmt.Errorf("native loop resume: no restorable messages for turn %s", checkpoint.TurnID)
	}
	llmContext, err := l.contextBuilder.Build(ctx, ContextInput{
		Prompt:  resumePrompt,
		History: history,
		Tools:   l.toolDefs,
	})
	if err != nil {
		return Result{}, fmt.Errorf("build llm context: %w", err)
	}
	if llmContext == nil {
		return Result{}, fmt.Errorf("build llm context: nil context")
	}

	totalUsage, llmCalls := usageFromResumeRecords(records)
	requestStep := latestEventStep(records, session.EventTypeLLMRequest)
	responseStep, response, hasResponse, err := latestLLMResponse(records)
	if err != nil {
		return Result{}, err
	}
	if requestStep == 0 {
		return Result{}, fmt.Errorf("native loop resume: turn %s has no llm request", checkpoint.TurnID)
	}
	if !hasResponse || requestStep > responseStep {
		return l.runFromStep(ctx, task, checkpoint.TurnID, requestStep, promptOutput, llmContext, totalUsage, llmCalls)
	}

	toolCalls := resumeToolCalls(records, responseStep, response)
	if len(toolCalls) == 0 {
		result := Result{Content: response.Content}
		if err := l.finalizeRun(ctx, task, checkpoint.TurnID, responseStep, llmContext, response.Usage, totalUsage, llmCalls, response.Content); err != nil {
			return Result{}, err
		}
		return result, nil
	}

	pending := pendingResumeToolCalls(records, responseStep, toolCalls)
	if len(pending) > 0 {
		if l.tools == nil {
			return Result{}, fmt.Errorf("native loop: tool registry is required for tool calls")
		}
		if err := l.executeResumeToolCalls(ctx, task, checkpoint.TurnID, responseStep, llmContext, records, pending); err != nil {
			return Result{}, err
		}
	}

	if responseStep == l.maxSteps {
		if err := l.finalizeRun(ctx, task, checkpoint.TurnID, responseStep, llmContext, response.Usage, totalUsage, llmCalls, ""); err != nil {
			return Result{}, err
		}
		return Result{}, fmt.Errorf("native loop: reached max steps after tool calls")
	}
	totalUsage, llmCalls = l.updateHistoryAndMaybeCompress(ctx, task, checkpoint.TurnID, responseStep, llmContext, response.Usage, totalUsage, llmCalls)
	return l.runFromStep(ctx, task, checkpoint.TurnID, responseStep+1, promptOutput, llmContext, totalUsage, llmCalls)
}

func (l *NativeLoop) runFromStep(ctx context.Context, task Task, turnID string, startStep int, promptOutput prompt.Output, llmContext LLMContext, totalUsage llm.Usage, llmCalls int) (Result, error) {
	result := Result{}

	for step := startStep; step <= l.maxSteps; step++ {
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

		// 这个退出逻辑，后续需要确定是否存在。
		if len(toolCalls) == 0 {
			result.Steps = append(result.Steps, currentStep)
			if err := l.finalizeRun(ctx, task, turnID, step, llmContext, response.Usage, totalUsage, llmCalls, response.Content); err != nil {
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
			if err := l.finalizeRun(ctx, task, turnID, step, llmContext, response.Usage, totalUsage, llmCalls, ""); err != nil {
				return Result{}, err
			}
			return Result{}, fmt.Errorf("native loop: reached max steps after tool calls")
		}
		totalUsage, llmCalls = l.updateHistoryAndMaybeCompress(ctx, task, turnID, step, llmContext, response.Usage, totalUsage, llmCalls)
	}

	return result, nil
}

func (l *NativeLoop) finalizeRun(ctx context.Context, task Task, turnID string, step int, llmContext LLMContext, latestUsage *llm.Usage, totalUsage llm.Usage, llmCalls int, output string) error {
	totalUsage, llmCalls = l.updateHistoryAndMaybeCompress(ctx, task, turnID, step, llmContext, latestUsage, totalUsage, llmCalls)
	if err := l.saveUsageSummary(ctx, task, turnID, totalUsage, llmCalls); err != nil {
		return err
	}
	return l.writeOutput(output)
}

func (l *NativeLoop) updateHistoryAndMaybeCompress(ctx context.Context, task Task, turnID string, step int, llmContext LLMContext, latestUsage *llm.Usage, totalUsage llm.Usage, llmCalls int) (llm.Usage, int) {
	l.history = llmContext.History()
	compressionUsage, compressionCalls := l.maybeCompressContext(ctx, task, turnID, step, llmContext, latestUsage)
	if compressionUsage != nil {
		totalUsage = totalUsage.Add(*compressionUsage)
	}
	llmCalls += compressionCalls
	l.history = llmContext.History()
	return totalUsage, llmCalls
}

func (l *NativeLoop) maybeCompressContext(ctx context.Context, task Task, turnID string, step int, llmContext LLMContext, latestUsage *llm.Usage) (*llm.Usage, int) {
	if l.compressor == nil || llmContext == nil {
		return nil, 0
	}
	request := llmContext.BuildRequest(RunState{
		Task:      task,
		TurnID:    turnID,
		StepIndex: step,
	})
	decision := contextCompressionDecision(request, latestUsage, l.tokenCounter)
	if !decision.ShouldCompress {
		return nil, 0
	}

	originalMessages := llmContext.History()
	compression, err := l.compressor.Compress(ctx, CompressionInput{
		Task:                task,
		TurnID:              turnID,
		StepIndex:           step,
		Model:               request.Model,
		Messages:            originalMessages,
		TriggerTokens:       decision.TriggerTokens,
		ContextWindowTokens: decision.ContextWindowTokens,
	})
	if err != nil {
		l.warnCompression("context compression failed at step %d: %v", step, err)
		l.saveContextCompressionEvent(ctx, task, turnID, step, contextCompressionEventPayload{
			Status:               "failed",
			Reason:               "compressor_error",
			Error:                err.Error(),
			EstimatedTokens:      decision.EstimatedTokens,
			UsageInputTokens:     decision.UsageInputTokens,
			TriggerTokens:        decision.TriggerTokens,
			ThresholdTokens:      decision.ThresholdTokens,
			ContextWindowTokens:  decision.ContextWindowTokens,
			OriginalMessageCount: len(originalMessages),
		})
		return nil, 0
	}
	if len(compression.Messages) == 0 {
		err := fmt.Errorf("context compression: empty compressed messages")
		l.warnCompression("context compression failed at step %d: %v", step, err)
		l.saveContextCompressionEvent(ctx, task, turnID, step, contextCompressionEventPayload{
			Status:               "failed",
			Reason:               "empty_result",
			Error:                err.Error(),
			EstimatedTokens:      decision.EstimatedTokens,
			UsageInputTokens:     decision.UsageInputTokens,
			TriggerTokens:        decision.TriggerTokens,
			ThresholdTokens:      decision.ThresholdTokens,
			ContextWindowTokens:  decision.ContextWindowTokens,
			OriginalMessageCount: len(originalMessages),
			Usage:                cloneUsage(compression.Usage),
		})
		return compression.Usage, 1
	}

	llmContext.ReplaceHistory(compression.Messages)
	if err := l.saveContextSnapshot(ctx, task, turnID, step, compression, decision, len(originalMessages)); err != nil {
		l.warnCompression("save context compression snapshot failed at step %d: %v", step, err)
		l.saveContextCompressionEvent(ctx, task, turnID, step, contextCompressionEventPayload{
			Status:               "snapshot_failed",
			Reason:               "save_snapshot",
			Error:                err.Error(),
			EstimatedTokens:      decision.EstimatedTokens,
			UsageInputTokens:     decision.UsageInputTokens,
			TriggerTokens:        decision.TriggerTokens,
			ThresholdTokens:      decision.ThresholdTokens,
			ContextWindowTokens:  decision.ContextWindowTokens,
			OriginalMessageCount: len(originalMessages),
			CompressedMessages:   len(compression.Messages),
			Summary:              compression.Summary,
			Usage:                cloneUsage(compression.Usage),
		})
		return compression.Usage, 1
	}

	l.saveContextCompressionEvent(ctx, task, turnID, step, contextCompressionEventPayload{
		Status:               "success",
		Reason:               "threshold_reached",
		EstimatedTokens:      decision.EstimatedTokens,
		UsageInputTokens:     decision.UsageInputTokens,
		TriggerTokens:        decision.TriggerTokens,
		ThresholdTokens:      decision.ThresholdTokens,
		ContextWindowTokens:  decision.ContextWindowTokens,
		OriginalMessageCount: len(originalMessages),
		CompressedMessages:   len(compression.Messages),
		Summary:              compression.Summary,
		Usage:                cloneUsage(compression.Usage),
	})
	return compression.Usage, 1
}

func (l *NativeLoop) materializeResumeMessages(ctx context.Context, task Task, turnID string, records []session.Record) ([]session.Record, error) {
	materialized := cloneSessionRecords(records)
	for _, record := range records {
		if record.TurnID != turnID || record.Kind != session.RecordKindEvent || record.Event == nil {
			continue
		}
		switch record.Event.Type {
		case session.EventTypeLLMResponse:
			if hasAssistantMessage(materialized, record.StepIndex) {
				continue
			}
			var payload llmResponseEventPayload
			if err := json.Unmarshal(record.Event.Payload, &payload); err != nil {
				return nil, fmt.Errorf("resume parse llm response event: %w", err)
			}
			toolCalls := resumeToolCalls(materialized, record.StepIndex, payload.Response)
			msg, ok := assistantMessage(payload.Response, toolCalls)
			if !ok {
				continue
			}
			if err := l.saveMessage(ctx, task, turnID, record.StepIndex, msg); err != nil {
				return nil, err
			}
			materialized = append(materialized, session.Record{
				Kind:      session.RecordKindMessage,
				TurnID:    turnID,
				StepIndex: record.StepIndex,
				Message:   &msg,
			})
		case session.EventTypeToolResult:
			var payload toolResultEventPayload
			if err := json.Unmarshal(record.Event.Payload, &payload); err != nil {
				return nil, fmt.Errorf("resume parse tool result event: %w", err)
			}
			if hasToolMessage(materialized, record.StepIndex, payload.ID) {
				continue
			}
			call := resumeToolCallByID(materialized, record.StepIndex, payload.ID, payload.Name)
			msg := toolResultMessage(call, ToolResult{
				Name:        payload.Name,
				StartedAt:   payload.StartedAt,
				CompletedAt: payload.CompletedAt,
				Content:     payload.Content,
				Metadata:    payload.Metadata,
				Error:       payload.Error,
			})
			if err := l.saveMessage(ctx, task, turnID, record.StepIndex, msg); err != nil {
				return nil, err
			}
			materialized = append(materialized, session.Record{
				Kind:      session.RecordKindMessage,
				TurnID:    turnID,
				StepIndex: record.StepIndex,
				Message:   &msg,
			})
		}
	}
	return materialized, nil
}

func usageFromResumeRecords(records []session.Record) (llm.Usage, int) {
	var total llm.Usage
	llmCalls := 0
	for _, record := range records {
		if record.Kind != session.RecordKindEvent || record.Event == nil {
			continue
		}
		switch record.Event.Type {
		case session.EventTypeLLMResponse:
			var payload llmResponseEventPayload
			if json.Unmarshal(record.Event.Payload, &payload) == nil && payload.Response.Usage != nil {
				total = total.Add(*payload.Response.Usage)
			}
			llmCalls++
		case session.EventTypeContextCompression:
			var payload contextCompressionEventPayload
			if json.Unmarshal(record.Event.Payload, &payload) == nil && payload.Usage != nil {
				total = total.Add(*payload.Usage)
				llmCalls++
			}
		}
	}
	return total, llmCalls
}

func latestEventStep(records []session.Record, eventType string) int {
	step := 0
	for _, record := range records {
		if record.Kind == session.RecordKindEvent && record.Event != nil && record.Event.Type == eventType {
			step = record.StepIndex
		}
	}
	return step
}

func latestLLMResponse(records []session.Record) (int, llm.Response, bool, error) {
	var response llm.Response
	step := 0
	found := false
	for _, record := range records {
		if record.Kind != session.RecordKindEvent || record.Event == nil || record.Event.Type != session.EventTypeLLMResponse {
			continue
		}
		var payload llmResponseEventPayload
		if err := json.Unmarshal(record.Event.Payload, &payload); err != nil {
			return 0, llm.Response{}, false, fmt.Errorf("resume parse llm response event: %w", err)
		}
		step = record.StepIndex
		response = payload.Response
		found = true
	}
	return step, response, found, nil
}

func resumeToolCalls(records []session.Record, step int, response llm.Response) []llm.ToolCall {
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.StepIndex == step && record.Kind == session.RecordKindMessage && record.Message != nil && record.Message.Role == llm.RoleAssistant && len(record.Message.ToolCalls) > 0 {
			return cloneLLMToolCalls(record.Message.ToolCalls)
		}
	}

	var calls []llm.ToolCall
	for _, record := range records {
		if record.StepIndex != step || record.Kind != session.RecordKindEvent || record.Event == nil || record.Event.Type != session.EventTypeToolCall {
			continue
		}
		var payload toolCallEventPayload
		if json.Unmarshal(record.Event.Payload, &payload) == nil {
			calls = append(calls, llm.ToolCall{
				ID:    payload.ID,
				Name:  payload.Name,
				Input: append(json.RawMessage(nil), payload.Input...),
			})
		}
	}
	if len(calls) > 0 {
		return calls
	}
	return normalizeToolCalls(response.ToolCalls, step)
}

func pendingResumeToolCalls(records []session.Record, step int, calls []llm.ToolCall) []llm.ToolCall {
	var pending []llm.ToolCall
	for _, call := range calls {
		if hasToolResult(records, step, call.ID) {
			continue
		}
		pending = append(pending, call)
	}
	return pending
}

func (l *NativeLoop) executeResumeToolCalls(ctx context.Context, task Task, turnID string, step int, llmContext LLMContext, records []session.Record, calls []llm.ToolCall) error {
	for _, call := range calls {
		if !hasToolCallEvent(records, step, call.ID) {
			if err := l.saveEvent(ctx, task, turnID, step, session.EventTypeToolCall, toolCallEventPayload{
				ID:    call.ID,
				Name:  call.Name,
				Input: append(json.RawMessage(nil), call.Input...),
			}); err != nil {
				return err
			}
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
		if err := l.saveEvent(ctx, task, turnID, step, session.EventTypeToolResult, toolResultEventPayload{
			ID:          call.ID,
			Name:        call.Name,
			StartedAt:   toolStartedAt,
			CompletedAt: toolCompletedAt,
			Content:     recorded.Content,
			Metadata:    recorded.Metadata,
			Error:       recorded.Error,
		}); err != nil {
			return err
		}
		toolMessage := llmContext.AddToolResult(call, recorded)
		if err := l.saveMessage(ctx, task, turnID, step, toolMessage); err != nil {
			return err
		}
	}
	return nil
}

func hasAssistantMessage(records []session.Record, step int) bool {
	for _, record := range records {
		if record.StepIndex == step && record.Kind == session.RecordKindMessage && record.Message != nil && record.Message.Role == llm.RoleAssistant {
			return true
		}
	}
	return false
}

func hasToolMessage(records []session.Record, step int, callID string) bool {
	for _, record := range records {
		if record.StepIndex == step && record.Kind == session.RecordKindMessage && record.Message != nil && record.Message.Role == llm.RoleTool && record.Message.ToolCallID == callID {
			return true
		}
	}
	return false
}

func hasToolResult(records []session.Record, step int, callID string) bool {
	if hasToolMessage(records, step, callID) {
		return true
	}
	for _, record := range records {
		if record.StepIndex != step || record.Kind != session.RecordKindEvent || record.Event == nil || record.Event.Type != session.EventTypeToolResult {
			continue
		}
		var payload toolResultEventPayload
		if json.Unmarshal(record.Event.Payload, &payload) == nil && payload.ID == callID {
			return true
		}
	}
	return false
}

func hasToolCallEvent(records []session.Record, step int, callID string) bool {
	for _, record := range records {
		if record.StepIndex != step || record.Kind != session.RecordKindEvent || record.Event == nil || record.Event.Type != session.EventTypeToolCall {
			continue
		}
		var payload toolCallEventPayload
		if json.Unmarshal(record.Event.Payload, &payload) == nil && payload.ID == callID {
			return true
		}
	}
	return false
}

func resumeToolCallByID(records []session.Record, step int, callID, name string) llm.ToolCall {
	calls := resumeToolCalls(records, step, llm.Response{})
	for _, call := range calls {
		if call.ID == callID {
			return call
		}
	}
	return llm.ToolCall{
		ID:    callID,
		Name:  name,
		Input: json.RawMessage(`{}`),
	}
}

func recordsForTurn(records []session.Record, turnID string) []session.Record {
	var filtered []session.Record
	for _, record := range records {
		if record.TurnID == turnID {
			filtered = append(filtered, cloneSessionRecord(record))
		}
	}
	return filtered
}

func cloneSessionRecords(records []session.Record) []session.Record {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]session.Record, 0, len(records))
	for _, record := range records {
		cloned = append(cloned, cloneSessionRecord(record))
	}
	return cloned
}

func cloneSessionRecord(record session.Record) session.Record {
	if record.Message != nil {
		msg := cloneMessage(*record.Message)
		record.Message = &msg
	}
	if record.Event != nil {
		event := *record.Event
		event.Payload = append(json.RawMessage(nil), record.Event.Payload...)
		record.Event = &event
	}
	record.Usage = cloneUsage(record.Usage)
	record.UsageSummary = cloneUsage(record.UsageSummary)
	if record.ContextSnapshot != nil {
		snapshot := *record.ContextSnapshot
		snapshot.Messages = cloneMessages(record.ContextSnapshot.Messages)
		record.ContextSnapshot = &snapshot
	}
	return record
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

func (l *NativeLoop) saveContextSnapshot(ctx context.Context, task Task, turnID string, step int, result CompressionResult, decision compressionDecision, originalMessageCount int) error {
	if l.session == nil {
		return nil
	}
	messages := cloneMessages(result.Messages)
	err := l.session.Save(ctx, session.Record{
		AgentName: task.AgentName,
		Task:      task.Input,
		WorkDir:   task.WorkDir,
		TurnID:    turnID,
		StepIndex: step,
		Kind:      session.RecordKindContextSnapshot,
		Timestamp: time.Now().UTC(),
		ContextSnapshot: &session.ContextSnapshot{
			Messages:             messages,
			Summary:              result.Summary,
			TriggerTokens:        decision.TriggerTokens,
			ContextWindowTokens:  decision.ContextWindowTokens,
			OriginalMessageCount: originalMessageCount,
		},
	})
	if err != nil {
		return fmt.Errorf("save context snapshot: %w", err)
	}
	return nil
}

func (l *NativeLoop) saveContextCompressionEvent(ctx context.Context, task Task, turnID string, step int, payload contextCompressionEventPayload) {
	if err := l.saveEvent(ctx, task, turnID, step, session.EventTypeContextCompression, payload); err != nil {
		l.warnCompression("save context compression event failed at step %d: %v", step, err)
	}
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

func (l *NativeLoop) warnCompression(format string, args ...any) {
	if l.logger != nil {
		l.logger.Warnf(format, args...)
	}
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

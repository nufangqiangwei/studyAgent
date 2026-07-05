package contextmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"agent/internal/foundation/llmClient"
)

type LLMClient interface {
	Complete(ctx context.Context, req llmClient.Request) (llmClient.Response, error)
}

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type Options struct {
	LLM    LLMClient
	Logger Logger
}

type Manager struct {
	llm    LLMClient
	logger Logger
}

type AgentProfile struct {
	Name        string
	Model       string
	Temperature float64
	Tools       []llmClient.ToolDefinition
	Metadata    map[string]string
}

type ModelCallInput struct {
	RunID         string
	Step          int
	Agent         AgentProfile
	Messages      []llmClient.Message
	CorrelationID string
	CausationID   string
}

type ModelCallResult struct {
	Request          llmClient.Request
	Response         llmClient.Response
	AssistantMessage *llmClient.Message
	ToolCalls        []llmClient.ToolCall
	Usage            *llmClient.Usage
	StartedAt        time.Time
	CompletedAt      time.Time
}

type ToolResult struct {
	Name     string         `json:"name,omitempty"`
	Content  string         `json:"content,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Error    string         `json:"error,omitempty"`
}

func NewManager(opts Options) (*Manager, error) {
	if opts.LLM == nil {
		return nil, fmt.Errorf("context manager: llm client is required")
	}
	return &Manager{llm: opts.LLM, logger: opts.Logger}, nil
}

func (r *Manager) BuildRequest(_ context.Context, input ModelCallInput) (llmClient.Request, error) {
	if r == nil {
		return llmClient.Request{}, fmt.Errorf("context manager: manager is nil")
	}
	if strings.TrimSpace(input.Agent.Model) == "" {
		return llmClient.Request{}, fmt.Errorf("context manager: agent model is required")
	}
	if len(input.Messages) == 0 {
		return llmClient.Request{}, fmt.Errorf("context manager: messages are required")
	}

	metadata := cloneStringMap(input.Agent.Metadata)
	metadata["run_id"] = input.RunID
	metadata["step"] = fmt.Sprintf("%d", input.Step)
	metadata["agent"] = input.Agent.Name
	if input.CorrelationID != "" {
		metadata["correlation_id"] = input.CorrelationID
	}
	if input.CausationID != "" {
		metadata["causation_id"] = input.CausationID
	}

	return llmClient.Request{
		Model:       input.Agent.Model,
		Messages:    cloneMessages(input.Messages),
		Tools:       cloneToolDefinitions(input.Agent.Tools),
		Temperature: input.Agent.Temperature,
		Metadata:    metadata,
	}, nil
}

func (r *Manager) CallModel(ctx context.Context, input ModelCallInput) (ModelCallResult, error) {
	if r == nil {
		return ModelCallResult{}, fmt.Errorf("context manager: manager is nil")
	}

	request, err := r.BuildRequest(ctx, input)
	if err != nil {
		return ModelCallResult{}, err
	}
	return r.CompleteRequest(ctx, input, request)
}

func (r *Manager) CompleteRequest(ctx context.Context, input ModelCallInput, request llmClient.Request) (ModelCallResult, error) {
	if r == nil {
		return ModelCallResult{}, fmt.Errorf("context manager: manager is nil")
	}
	if r.llm == nil {
		return ModelCallResult{}, fmt.Errorf("context manager: llm client is required")
	}
	if r.logger != nil {
		r.logger.Debugf("context manager calling model agent=%s model=%s run_id=%s step=%d", input.Agent.Name, request.Model, input.RunID, input.Step)
	}

	startedAt := time.Now().UTC()
	response, err := r.llm.Complete(ctx, request)
	completedAt := time.Now().UTC()
	result := ModelCallResult{
		Request:     cloneRequest(request),
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}
	if err != nil {
		return result, fmt.Errorf("context manager llm complete: %w", err)
	}

	toolCalls := NormalizeToolCalls(response.ToolCalls, input.Step)
	response.ToolCalls = cloneLLMToolCalls(toolCalls)
	if assistant, ok := NewAssistantMessage(response, toolCalls); ok {
		result.AssistantMessage = &assistant
	}
	result.Response = cloneResponse(response)
	result.ToolCalls = cloneLLMToolCalls(toolCalls)
	result.Usage = cloneUsage(response.Usage)
	return result, nil
}

func NormalizeToolCalls(calls []llmClient.ToolCall, step int) []llmClient.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	normalized := make([]llmClient.ToolCall, 0, len(calls))
	for i, call := range calls {
		if strings.TrimSpace(call.ID) == "" {
			call.ID = fmt.Sprintf("call_%d_%d", step, i+1)
		}
		if strings.TrimSpace(string(call.Input)) == "" {
			call.Input = json.RawMessage(`{}`)
		} else {
			call.Input = append(json.RawMessage(nil), call.Input...)
		}
		normalized = append(normalized, call)
	}
	return normalized
}

func NewAssistantMessage(response llmClient.Response, toolCalls []llmClient.ToolCall) (llmClient.Message, bool) {
	if strings.TrimSpace(response.Content) == "" && len(toolCalls) == 0 {
		return llmClient.Message{}, false
	}
	return llmClient.Message{
		Role:      llmClient.RoleAssistant,
		Content:   response.Content,
		ToolCalls: cloneLLMToolCalls(toolCalls),
		Usage:     cloneUsage(response.Usage),
	}, true
}

func NewToolResultMessage(call llmClient.ToolCall, result ToolResult) llmClient.Message {
	content := result.Content
	if result.Error != "" {
		content = "error: " + result.Error
	}
	return llmClient.Message{
		Role:       llmClient.RoleTool,
		Name:       call.Name,
		Content:    content,
		ToolCallID: call.ID,
	}
}

func cloneRequest(request llmClient.Request) llmClient.Request {
	cloned := request
	cloned.Messages = cloneMessages(request.Messages)
	cloned.Tools = cloneToolDefinitions(request.Tools)
	cloned.Metadata = cloneStringMap(request.Metadata)
	return cloned
}

func cloneResponse(response llmClient.Response) llmClient.Response {
	cloned := response
	cloned.ToolCalls = cloneLLMToolCalls(response.ToolCalls)
	cloned.Usage = cloneUsage(response.Usage)
	cloned.Raw = append(json.RawMessage(nil), response.Raw...)
	return cloned
}

func cloneMessages(messages []llmClient.Message) []llmClient.Message {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]llmClient.Message, 0, len(messages))
	for _, message := range messages {
		cloned = append(cloned, cloneMessage(message))
	}
	return cloned
}

func cloneMessage(message llmClient.Message) llmClient.Message {
	cloned := message
	cloned.ToolCalls = cloneLLMToolCalls(message.ToolCalls)
	cloned.Usage = cloneUsage(message.Usage)
	return cloned
}

func cloneLLMToolCalls(calls []llmClient.ToolCall) []llmClient.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	cloned := make([]llmClient.ToolCall, 0, len(calls))
	for _, call := range calls {
		cloned = append(cloned, llmClient.ToolCall{
			ID:    call.ID,
			Name:  call.Name,
			Input: append(json.RawMessage(nil), call.Input...),
		})
	}
	return cloned
}

func cloneUsage(usage *llmClient.Usage) *llmClient.Usage {
	if usage == nil {
		return nil
	}
	cloned := *usage
	return &cloned
}

func cloneToolDefinitions(defs []llmClient.ToolDefinition) []llmClient.ToolDefinition {
	if len(defs) == 0 {
		return nil
	}
	cloned := make([]llmClient.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		cloned = append(cloned, llmClient.ToolDefinition{
			Name:        def.Name,
			Description: def.Description,
			InputSchema: append(json.RawMessage(nil), def.InputSchema...),
		})
	}
	return cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values)+5)
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

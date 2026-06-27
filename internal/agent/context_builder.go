package agent

import (
	"agent/internal/foundation/llmClient"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"agent/internal/prompt"
	"agent/internal/session"
)

type ContextBuilder interface {
	Build(ctx context.Context, input ContextInput) (LLMContext, error)
}

type ContextInput struct {
	Prompt         prompt.Output
	SessionRecords []session.Record
	History        []llmClient.Message
	Tools          []llmClient.ToolDefinition
}

type RunState struct {
	RunID           string                     `json:"run_id"`
	TaskID          string                     `json:"task_id,omitempty"`
	TurnID          string                     `json:"turn_id"`
	Task            Task                       `json:"task"`
	Status          RunStatus                  `json:"status"`
	CurrentStep     int                        `json:"current_step"`
	StepIndex       int                        `json:"-"`
	Messages        []llmClient.Message        `json:"messages,omitempty"`
	ToolDefinitions []llmClient.ToolDefinition `json:"tool_definitions,omitempty"`
	PendingTools    []PendingToolCall          `json:"pending_tools,omitempty"`
	Steps           []Step                     `json:"steps,omitempty"`
	FinalAnswer     string                     `json:"final_answer,omitempty"`
	Summary         string                     `json:"summary,omitempty"`
	Model           string                     `json:"model,omitempty"`
	Temperature     float64                    `json:"temperature,omitempty"`
	PromptDebugText string                     `json:"prompt_debug_text,omitempty"`
	Usage           llmClient.Usage            `json:"usage,omitempty"`
	LLMCalls        int                        `json:"llm_calls,omitempty"`
	LastEventID     string                     `json:"last_event_id,omitempty"`
	CreatedAt       time.Time                  `json:"created_at"`
	UpdatedAt       time.Time                  `json:"updated_at"`
}

type LLMContext interface {
	InitialMessages() []llmClient.Message
	BuildRequest(state RunState) llmClient.Request
	AddAssistantResponse(response llmClient.Response, toolCalls []llmClient.ToolCall) (llmClient.Message, bool)
	AddToolResult(call llmClient.ToolCall, result ToolResult) llmClient.Message
	History() []llmClient.Message
}

type NativeContextBuilder struct{}

func NewNativeContextBuilder() *NativeContextBuilder {
	return &NativeContextBuilder{}
}

func (b *NativeContextBuilder) Build(_ context.Context, input ContextInput) (LLMContext, error) {
	history := cloneMessages(input.History)
	if len(history) == 0 {
		history = messagesFromSession(input.SessionRecords)
	}

	messages, initial := initialMessages(history, input.Prompt.Messages)
	return &nativeLLMContext{
		prompt:          input.Prompt,
		tools:           cloneToolDefinitions(input.Tools),
		messages:        messages,
		initialMessages: initial,
	}, nil
}

type nativeLLMContext struct {
	prompt          prompt.Output
	tools           []llmClient.ToolDefinition
	messages        []llmClient.Message
	initialMessages []llmClient.Message
}

func (c *nativeLLMContext) InitialMessages() []llmClient.Message {
	return cloneMessages(c.initialMessages)
}

func (c *nativeLLMContext) BuildRequest(state RunState) llmClient.Request {
	step := state.StepIndex
	if step == 0 {
		step = state.CurrentStep
	}
	return llmClient.Request{
		Model:       c.prompt.Model,
		Messages:    cloneMessages(c.messages),
		Tools:       cloneToolDefinitions(c.tools),
		Temperature: c.prompt.Temperature,
		Metadata: map[string]string{
			"loop": "native",
			"step": fmt.Sprintf("%d", step),
		},
	}
}

func (c *nativeLLMContext) AddAssistantResponse(response llmClient.Response, toolCalls []llmClient.ToolCall) (llmClient.Message, bool) {
	msg, ok := assistantMessage(response, toolCalls)
	if !ok {
		return llmClient.Message{}, false
	}
	c.messages = append(c.messages, msg)
	return cloneMessage(msg), true
}

func (c *nativeLLMContext) AddToolResult(call llmClient.ToolCall, result ToolResult) llmClient.Message {
	msg := toolResultMessage(call, result)
	c.messages = append(c.messages, msg)
	return cloneMessage(msg)
}

func (c *nativeLLMContext) History() []llmClient.Message {
	return cloneMessages(c.messages)
}

func initialMessages(history []llmClient.Message, promptMessages []llmClient.Message) ([]llmClient.Message, []llmClient.Message) {
	messages := cloneMessages(history)
	generated := []llmClient.Message{}
	if len(messages) == 0 {
		for _, msg := range promptMessages {
			if msg.Role == llmClient.RoleSystem {
				cloned := cloneMessage(msg)
				messages = append(messages, cloned)
				generated = append(generated, cloned)
			}
		}
	}

	for _, msg := range promptMessages {
		if msg.Role != llmClient.RoleSystem {
			cloned := cloneMessage(msg)
			messages = append(messages, cloned)
			generated = append(generated, cloned)
		}
	}
	return messages, generated
}

func messagesFromSession(records []session.Record) []llmClient.Message {
	if len(records) == 0 {
		return nil
	}
	messages := make([]llmClient.Message, 0, len(records))
	for _, record := range records {
		if record.Kind != "" && record.Kind != session.RecordKindMessage {
			continue
		}
		if record.Message == nil {
			continue
		}
		messages = append(messages, cloneMessage(*record.Message))
	}
	return messages
}

func assistantMessage(response llmClient.Response, toolCalls []llmClient.ToolCall) (llmClient.Message, bool) {
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

func toolResultMessage(call llmClient.ToolCall, result ToolResult) llmClient.Message {
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

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"agent/internal/llm"
	"agent/internal/prompt"
	"agent/internal/session"
)

type ContextBuilder interface {
	Build(ctx context.Context, input ContextInput) (LLMContext, error)
}

type ContextInput struct {
	Prompt         prompt.Output
	SessionRecords []session.Record
	History        []llm.Message
	Tools          []llm.ToolDefinition
}

type RunState struct {
	Task      Task
	TurnID    string
	StepIndex int
}

type LLMContext interface {
	InitialMessages() []llm.Message
	BuildRequest(state RunState) llm.Request
	AddAssistantResponse(response llm.Response, toolCalls []llm.ToolCall) (llm.Message, bool)
	AddToolResult(call llm.ToolCall, result ToolResult) llm.Message
	History() []llm.Message
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
	tools           []llm.ToolDefinition
	messages        []llm.Message
	initialMessages []llm.Message
}

func (c *nativeLLMContext) InitialMessages() []llm.Message {
	return cloneMessages(c.initialMessages)
}

func (c *nativeLLMContext) BuildRequest(state RunState) llm.Request {
	return llm.Request{
		Model:       c.prompt.Model,
		Messages:    cloneMessages(c.messages),
		Tools:       cloneToolDefinitions(c.tools),
		Temperature: c.prompt.Temperature,
		Metadata: map[string]string{
			"loop": "native",
			"step": fmt.Sprintf("%d", state.StepIndex),
		},
	}
}

func (c *nativeLLMContext) AddAssistantResponse(response llm.Response, toolCalls []llm.ToolCall) (llm.Message, bool) {
	msg, ok := assistantMessage(response, toolCalls)
	if !ok {
		return llm.Message{}, false
	}
	c.messages = append(c.messages, msg)
	return cloneMessage(msg), true
}

func (c *nativeLLMContext) AddToolResult(call llm.ToolCall, result ToolResult) llm.Message {
	msg := toolResultMessage(call, result)
	c.messages = append(c.messages, msg)
	return cloneMessage(msg)
}

func (c *nativeLLMContext) History() []llm.Message {
	return cloneMessages(c.messages)
}

func initialMessages(history []llm.Message, promptMessages []llm.Message) ([]llm.Message, []llm.Message) {
	messages := cloneMessages(history)
	generated := []llm.Message{}
	if len(messages) == 0 {
		for _, msg := range promptMessages {
			if msg.Role == llm.RoleSystem {
				cloned := cloneMessage(msg)
				messages = append(messages, cloned)
				generated = append(generated, cloned)
			}
		}
	}

	for _, msg := range promptMessages {
		if msg.Role != llm.RoleSystem {
			cloned := cloneMessage(msg)
			messages = append(messages, cloned)
			generated = append(generated, cloned)
		}
	}
	return messages, generated
}

func messagesFromSession(records []session.Record) []llm.Message {
	if len(records) == 0 {
		return nil
	}
	messages := make([]llm.Message, 0, len(records))
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

func assistantMessage(response llm.Response, toolCalls []llm.ToolCall) (llm.Message, bool) {
	if strings.TrimSpace(response.Content) == "" && len(toolCalls) == 0 {
		return llm.Message{}, false
	}
	return llm.Message{
		Role:      llm.RoleAssistant,
		Content:   response.Content,
		ToolCalls: cloneLLMToolCalls(toolCalls),
		Usage:     cloneUsage(response.Usage),
	}, true
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

func cloneMessages(messages []llm.Message) []llm.Message {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]llm.Message, 0, len(messages))
	for _, message := range messages {
		cloned = append(cloned, cloneMessage(message))
	}
	return cloned
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

func cloneToolDefinitions(defs []llm.ToolDefinition) []llm.ToolDefinition {
	if len(defs) == 0 {
		return nil
	}
	cloned := make([]llm.ToolDefinition, 0, len(defs))
	for _, def := range defs {
		cloned = append(cloned, llm.ToolDefinition{
			Name:        def.Name,
			Description: def.Description,
			InputSchema: append(json.RawMessage(nil), def.InputSchema...),
		})
	}
	return cloned
}

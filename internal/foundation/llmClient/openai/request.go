package openai

import (
	"agent/internal/foundation/llmClient"
	"encoding/json"
	"strings"
)

type Builder struct{}

type ChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Tools       []ChatTool    `json:"tool,omitempty"`
	Temperature float64       `json:"temperature,omitempty"`
}

type ChatMessage struct {
	Role       string         `json:"role"`
	Content    *string        `json:"content,omitempty"`
	ToolCalls  []ChatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type ChatTool struct {
	Type     string       `json:"type"`
	Function ChatFunction `json:"function"`
}

type ChatFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type ChatToolCall struct {
	ID       string               `json:"id"`
	Type     string               `json:"type"`
	Function ChatToolCallFunction `json:"function"`
}

type ChatToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func (Builder) Build(req llmClient.Request) ([]byte, error) {
	messages := make([]ChatMessage, 0, len(req.Messages))
	for _, msg := range req.Messages {
		messages = append(messages, ChatMessage{
			Role:       string(msg.Role),
			Content:    chatContent(msg),
			ToolCalls:  chatToolCalls(msg.ToolCalls),
			ToolCallID: msg.ToolCallID,
		})
	}

	return json.MarshalIndent(ChatRequest{
		Model:       req.Model,
		Messages:    messages,
		Tools:       chatTools(req.Tools),
		Temperature: req.Temperature,
	}, "", "  ")
}

func chatContent(msg llmClient.Message) *string {
	if msg.Role == llmClient.RoleAssistant && msg.Content == "" && len(msg.ToolCalls) > 0 {
		return nil
	}
	content := msg.Content
	return &content
}

func chatToolCalls(calls []llmClient.ToolCall) []ChatToolCall {
	if len(calls) == 0 {
		return nil
	}
	result := make([]ChatToolCall, 0, len(calls))
	for _, call := range calls {
		result = append(result, ChatToolCall{
			ID:   call.ID,
			Type: "function",
			Function: ChatToolCallFunction{
				Name:      call.Name,
				Arguments: rawArguments(call.Input),
			},
		})
	}
	return result
}

func rawArguments(input json.RawMessage) string {
	trimmed := strings.TrimSpace(string(input))
	if trimmed == "" {
		return "{}"
	}
	return trimmed
}

func chatTools(defs []llmClient.ToolDefinition) []ChatTool {
	if len(defs) == 0 {
		return nil
	}

	tools := make([]ChatTool, 0, len(defs))
	for _, def := range defs {
		tools = append(tools, ChatTool{
			Type: "function",
			Function: ChatFunction{
				Name:        def.Name,
				Description: def.Description,
				Parameters:  def.InputSchema,
			},
		})
	}
	return tools
}

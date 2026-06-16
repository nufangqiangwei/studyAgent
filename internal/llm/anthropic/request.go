package anthropic

import (
	"encoding/json"
	"strings"

	"agent/internal/llm"
)

type Builder struct{}

type MessagesRequest struct {
	Model       string    `json:"model"`
	MaxTokens   int       `json:"max_tokens"`
	System      string    `json:"system,omitempty"`
	Messages    []Message `json:"messages"`
	Tools       []Tool    `json:"tools,omitempty"`
	Temperature float64   `json:"temperature,omitempty"`
}

type Message struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type ContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
}

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

func (Builder) Build(req llm.Request) ([]byte, error) {
	system := ""
	messages := make([]Message, 0, len(req.Messages))
	for _, msg := range req.Messages {
		if msg.Role == llm.RoleSystem {
			if system != "" {
				system += "\n\n"
			}
			system += msg.Content
			continue
		}

		if msg.Role == llm.RoleTool {
			messages = appendToolResult(messages, msg)
			continue
		}

		role := anthropicRole(msg.Role)
		messages = append(messages, Message{
			Role:    role,
			Content: anthropicContent(msg),
		})
	}

	return json.MarshalIndent(MessagesRequest{
		Model:       req.Model,
		MaxTokens:   4096,
		System:      system,
		Messages:    messages,
		Tools:       messageTools(req.Tools),
		Temperature: req.Temperature,
	}, "", "  ")
}

func appendToolResult(messages []Message, msg llm.Message) []Message {
	block := ContentBlock{
		Type:      "tool_result",
		ToolUseID: msg.ToolCallID,
		Content:   msg.Content,
	}
	if len(messages) > 0 && messages[len(messages)-1].Role == "user" {
		if blocks, ok := messages[len(messages)-1].Content.([]ContentBlock); ok && allToolResults(blocks) {
			messages[len(messages)-1].Content = append(blocks, block)
			return messages
		}
	}
	return append(messages, Message{
		Role:    "user",
		Content: []ContentBlock{block},
	})
}

func allToolResults(blocks []ContentBlock) bool {
	if len(blocks) == 0 {
		return false
	}
	for _, block := range blocks {
		if block.Type != "tool_result" {
			return false
		}
	}
	return true
}

func anthropicRole(role llm.Role) string {
	if role == llm.RoleAssistant {
		return "assistant"
	}
	return "user"
}

func anthropicContent(msg llm.Message) any {
	if len(msg.ToolCalls) == 0 {
		return msg.Content
	}

	blocks := make([]ContentBlock, 0, len(msg.ToolCalls)+1)
	if strings.TrimSpace(msg.Content) != "" {
		blocks = append(blocks, ContentBlock{
			Type: "text",
			Text: msg.Content,
		})
	}
	for _, call := range msg.ToolCalls {
		blocks = append(blocks, ContentBlock{
			Type:  "tool_use",
			ID:    call.ID,
			Name:  call.Name,
			Input: rawInput(call.Input),
		})
	}
	return blocks
}

func rawInput(input json.RawMessage) json.RawMessage {
	if len(strings.TrimSpace(string(input))) == 0 {
		return json.RawMessage(`{}`)
	}
	return input
}

func messageTools(defs []llm.ToolDefinition) []Tool {
	if len(defs) == 0 {
		return nil
	}

	tools := make([]Tool, 0, len(defs))
	for _, def := range defs {
		tools = append(tools, Tool{
			Name:        def.Name,
			Description: def.Description,
			InputSchema: def.InputSchema,
		})
	}
	return tools
}

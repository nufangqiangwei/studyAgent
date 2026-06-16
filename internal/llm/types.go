package llm

import (
	"context"
	"encoding/json"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type Message struct {
	Role       Role       `json:"role"`
	Content    string     `json:"content,omitempty"`
	Name       string     `json:"name,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	Usage      *Usage     `json:"usage,omitempty"`
}

type Request struct {
	Provider    string            `json:"provider,omitempty"`
	Model       string            `json:"model"`
	Messages    []Message         `json:"messages"`
	Tools       []ToolDefinition  `json:"tools,omitempty"`
	Temperature float64           `json:"temperature,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

type Response struct {
	Provider   string          `json:"provider"`
	Model      string          `json:"model"`
	Content    string          `json:"content"`
	StopReason string          `json:"stop_reason,omitempty"`
	ToolCalls  []ToolCall      `json:"tool_calls,omitempty"`
	Usage      *Usage          `json:"usage,omitempty"`
	Raw        json.RawMessage `json:"raw,omitempty"`
}

type ToolCall struct {
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type Usage struct {
	InputTokens              int `json:"input_tokens,omitempty"`
	OutputTokens             int `json:"output_tokens,omitempty"`
	TotalTokens              int `json:"total_tokens,omitempty"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`
}

func (u Usage) Add(other Usage) Usage {
	return Usage{
		InputTokens:              u.InputTokens + other.InputTokens,
		OutputTokens:             u.OutputTokens + other.OutputTokens,
		TotalTokens:              u.TotalTokens + other.TotalTokens,
		CacheCreationInputTokens: u.CacheCreationInputTokens + other.CacheCreationInputTokens,
		CacheReadInputTokens:     u.CacheReadInputTokens + other.CacheReadInputTokens,
	}
}

func (u Usage) IsZero() bool {
	return u.InputTokens == 0 &&
		u.OutputTokens == 0 &&
		u.TotalTokens == 0 &&
		u.CacheCreationInputTokens == 0 &&
		u.CacheReadInputTokens == 0
}

type Client interface {
	ModelName() string
	Complete(ctx context.Context, req Request) (Response, error)
}

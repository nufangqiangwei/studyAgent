package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"agent/internal/llm"
)

const (
	defaultContextWindowTokens          = 32_000
	defaultContextCompressionRatio      = 0.5
	contextCompressionPromptTemperature = 0.2
)

type SessionCompressor interface {
	Compress(ctx context.Context, input CompressionInput) (CompressionResult, error)
}

type ContextTokenCounter interface {
	CountRequest(req llm.Request) int
}

type CompressionInput struct {
	Task                Task
	TurnID              string
	StepIndex           int
	Model               string
	Messages            []llm.Message
	TriggerTokens       int
	ContextWindowTokens int
}

type CompressionResult struct {
	Messages []llm.Message
	Summary  string
	Usage    *llm.Usage
}

type EstimatedContextTokenCounter struct{}

type compressionDecision struct {
	ShouldCompress      bool
	EstimatedTokens     int
	UsageInputTokens    int
	TriggerTokens       int
	ContextWindowTokens int
	ThresholdTokens     int
}

type contextCompressionEventPayload struct {
	Status               string     `json:"status"`
	Reason               string     `json:"reason,omitempty"`
	Error                string     `json:"error,omitempty"`
	EstimatedTokens      int        `json:"estimated_tokens,omitempty"`
	UsageInputTokens     int        `json:"usage_input_tokens,omitempty"`
	TriggerTokens        int        `json:"trigger_tokens,omitempty"`
	ThresholdTokens      int        `json:"threshold_tokens,omitempty"`
	ContextWindowTokens  int        `json:"context_window_tokens,omitempty"`
	OriginalMessageCount int        `json:"original_message_count,omitempty"`
	CompressedMessages   int        `json:"compressed_messages,omitempty"`
	Summary              string     `json:"summary,omitempty"`
	Usage                *llm.Usage `json:"usage,omitempty"`
}

type LLMSessionCompressor struct {
	llm LLMClient
}

func NewLLMSessionCompressor(client LLMClient) *LLMSessionCompressor {
	return &LLMSessionCompressor{llm: client}
}

func (c *LLMSessionCompressor) Compress(ctx context.Context, input CompressionInput) (CompressionResult, error) {
	if c == nil || c.llm == nil {
		return CompressionResult{}, fmt.Errorf("context compression: llm client is required")
	}
	if len(input.Messages) == 0 {
		return CompressionResult{}, fmt.Errorf("context compression: messages are required")
	}

	response, err := c.llm.Complete(ctx, llm.Request{
		Model: input.Model,
		Messages: []llm.Message{
			{
				Role:    llm.RoleSystem,
				Content: contextCompressionSystemPrompt(),
			},
			{
				Role:    llm.RoleUser,
				Content: buildContextCompressionPrompt(input),
			},
		},
		Temperature: contextCompressionPromptTemperature,
		Metadata: map[string]string{
			"loop":    "native",
			"purpose": "context_compression",
			"step":    fmt.Sprintf("%d", input.StepIndex),
			"turn_id": input.TurnID,
		},
	})
	if err != nil {
		return CompressionResult{}, fmt.Errorf("context compression llm complete: %w", err)
	}

	summary := strings.TrimSpace(response.Content)
	if summary == "" {
		return CompressionResult{}, fmt.Errorf("context compression: empty summary")
	}

	return CompressionResult{
		Messages: compressedHistoryMessages(input.Messages, summary),
		Summary:  summary,
		Usage:    cloneUsage(response.Usage),
	}, nil
}

func (EstimatedContextTokenCounter) CountRequest(req llm.Request) int {
	tokens := 8
	for _, msg := range req.Messages {
		tokens += 4
		tokens += estimateTextTokens(string(msg.Role))
		tokens += estimateTextTokens(msg.Name)
		tokens += estimateTextTokens(msg.ToolCallID)
		tokens += estimateTextTokens(msg.Content)
		for _, call := range msg.ToolCalls {
			tokens += 8
			tokens += estimateTextTokens(call.ID)
			tokens += estimateTextTokens(call.Name)
			tokens += estimateTextTokens(string(call.Input))
		}
	}
	for _, tool := range req.Tools {
		tokens += 12
		tokens += estimateTextTokens(tool.Name)
		tokens += estimateTextTokens(tool.Description)
		tokens += estimateTextTokens(string(tool.InputSchema))
	}
	for key, value := range req.Metadata {
		tokens += estimateTextTokens(key) + estimateTextTokens(value)
	}
	if tokens < 1 {
		return 1
	}
	return tokens
}

func contextCompressionDecision(req llm.Request, usage *llm.Usage, counter ContextTokenCounter) compressionDecision {
	if counter == nil {
		counter = EstimatedContextTokenCounter{}
	}
	window := contextWindowTokens(req.Model)
	threshold := int(float64(window) * defaultContextCompressionRatio)
	if threshold <= 0 {
		threshold = window / 2
	}
	estimated := counter.CountRequest(req)
	usageInput := 0
	if usage != nil {
		usageInput = usage.InputTokens
	}
	trigger := maxInt(estimated, usageInput)
	return compressionDecision{
		ShouldCompress:      trigger >= threshold,
		EstimatedTokens:     estimated,
		UsageInputTokens:    usageInput,
		TriggerTokens:       trigger,
		ContextWindowTokens: window,
		ThresholdTokens:     threshold,
	}
}

func contextWindowTokens(model string) int {
	name := strings.ToLower(strings.TrimSpace(model))
	switch {
	case name == "":
		return defaultContextWindowTokens
	case strings.HasPrefix(name, "gpt-"),
		strings.HasPrefix(name, "o1"),
		strings.HasPrefix(name, "o3"),
		strings.HasPrefix(name, "o4"),
		strings.HasPrefix(name, "chatgpt-"):
		return 128_000
	case strings.HasPrefix(name, "claude"):
		return 200_000
	case strings.HasPrefix(name, "gemini"):
		return 1_000_000
	case strings.HasPrefix(name, "deepseek"):
		return 64_000
	case strings.HasPrefix(name, "mock"):
		return defaultContextWindowTokens
	default:
		return defaultContextWindowTokens
	}
}

func estimateTextTokens(text string) int {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	asciiRunes := 0
	nonASCIIRunes := 0
	for _, r := range text {
		if r <= 127 {
			asciiRunes++
		} else {
			nonASCIIRunes++
		}
	}
	return (asciiRunes+3)/4 + nonASCIIRunes
}

func compressedHistoryMessages(messages []llm.Message, summary string) []llm.Message {
	compressed := make([]llm.Message, 0, 2)
	for _, msg := range messages {
		if msg.Role == llm.RoleSystem {
			compressed = append(compressed, cloneMessage(msg))
		}
	}
	compressed = append(compressed, llm.Message{
		Role:    llm.RoleUser,
		Content: "Conversation summary:\n" + strings.TrimSpace(summary),
	})
	return compressed
}

func contextCompressionSystemPrompt() string {
	return `You compress an agent conversation into a durable handoff summary.
Preserve facts needed to continue the work. Do not invent details.
Include the current task, key decisions, tool results, errors, user constraints, and concrete next steps.`
}

func buildContextCompressionPrompt(input CompressionInput) string {
	transcript := formatMessagesForCompression(input.Messages)
	return fmt.Sprintf(`Current task:
%s

Workspace:
%s

The current context has reached %d tokens out of a %d token window. Compress the transcript below into a concise but complete continuation summary.

Transcript:
%s`, input.Task.Input, input.Task.WorkDir, input.TriggerTokens, input.ContextWindowTokens, transcript)
}

func formatMessagesForCompression(messages []llm.Message) string {
	var builder strings.Builder
	for i, msg := range messages {
		fmt.Fprintf(&builder, "Message %d\nrole: %s\n", i+1, msg.Role)
		if msg.Name != "" {
			fmt.Fprintf(&builder, "name: %s\n", msg.Name)
		}
		if msg.ToolCallID != "" {
			fmt.Fprintf(&builder, "tool_call_id: %s\n", msg.ToolCallID)
		}
		if len(msg.ToolCalls) > 0 {
			raw, err := json.Marshal(msg.ToolCalls)
			if err == nil {
				fmt.Fprintf(&builder, "tool_calls: %s\n", raw)
			}
		}
		if strings.TrimSpace(msg.Content) != "" {
			fmt.Fprintf(&builder, "content:\n%s\n", msg.Content)
		}
		builder.WriteString("\n")
	}
	return strings.TrimSpace(builder.String())
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

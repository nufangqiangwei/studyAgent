package anthropic

import (
	"agent/internal/foundation/llmClient"
	"encoding/json"
	"testing"
)

func TestBuilderIncludesTools(t *testing.T) {
	body, err := (Builder{}).Build(llmClient.Request{
		Model:    "claude-test",
		Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: "hello"}},
		Tools: []llmClient.ToolDefinition{{
			Name:        "ask_user",
			Description: "Ask the user.",
			InputSchema: json.RawMessage(`{"type":"object"}`),
		}},
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	tools := got["tool"].([]any)
	tool := tools[0].(map[string]any)
	if tool["name"] != "ask_user" {
		t.Fatalf("tool name = %v, want ask_user", tool["name"])
	}
	if tool["input_schema"] == nil {
		t.Fatal("tool missing input_schema")
	}
}

func TestBuilderIncludesToolUseAndResultBlocks(t *testing.T) {
	body, err := (Builder{}).Build(llmClient.Request{
		Model: "claude-test",
		Messages: []llmClient.Message{
			{Role: llmClient.RoleSystem, Content: "system prompt"},
			{
				Role: llmClient.RoleAssistant,
				ToolCalls: []llmClient.ToolCall{{
					ID:    "toolu_1",
					Name:  "ask_user",
					Input: json.RawMessage(`{"question":"Which target?"}`),
				}},
			},
			{Role: llmClient.RoleTool, ToolCallID: "toolu_1", Content: "web app"},
		},
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	var got struct {
		System   string `json:"system"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if got.System != "system prompt" {
		t.Fatalf("system = %q", got.System)
	}

	var toolUse []struct {
		Type  string          `json:"type"`
		ID    string          `json:"id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(got.Messages[0].Content, &toolUse); err != nil {
		t.Fatalf("unmarshal tool_use blocks: %v", err)
	}
	if got.Messages[0].Role != "assistant" || toolUse[0].Type != "tool_use" || toolUse[0].ID != "toolu_1" || toolUse[0].Name != "ask_user" {
		t.Fatalf("assistant tool_use = role %q blocks %#v", got.Messages[0].Role, toolUse)
	}

	var toolResult []struct {
		Type      string `json:"type"`
		ToolUseID string `json:"tool_use_id"`
		Content   string `json:"content"`
	}
	if err := json.Unmarshal(got.Messages[1].Content, &toolResult); err != nil {
		t.Fatalf("unmarshal tool_result blocks: %v", err)
	}
	if got.Messages[1].Role != "user" || toolResult[0].Type != "tool_result" || toolResult[0].ToolUseID != "toolu_1" || toolResult[0].Content != "web app" {
		t.Fatalf("tool_result = role %q blocks %#v", got.Messages[1].Role, toolResult)
	}
}

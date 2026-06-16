package deepseek

import (
	"encoding/json"
	"testing"

	"agent/internal/llm"
)

func TestBuilderIncludesTools(t *testing.T) {
	body, err := (Builder{}).Build(llm.Request{
		Model:    "deepseek-chat",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
		Tools: []llm.ToolDefinition{{
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
	tools := got["tools"].([]any)
	function := tools[0].(map[string]any)["function"].(map[string]any)
	if function["name"] != "ask_user" {
		t.Fatalf("function name = %v, want ask_user", function["name"])
	}
	if function["parameters"] == nil {
		t.Fatal("function missing parameters")
	}
}

func TestBuilderIncludesToolCallMessages(t *testing.T) {
	body, err := (Builder{}).Build(llm.Request{
		Model: "deepseek-chat",
		Messages: []llm.Message{
			{
				Role: llm.RoleAssistant,
				ToolCalls: []llm.ToolCall{{
					ID:    "call_1",
					Name:  "ask_user",
					Input: json.RawMessage(`{"question":"Which target?"}`),
				}},
			},
			{Role: llm.RoleTool, ToolCallID: "call_1", Content: "web app"},
		},
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	var got struct {
		Messages []struct {
			Role       string `json:"role"`
			Content    string `json:"content"`
			ToolCallID string `json:"tool_call_id"`
			ToolCalls  []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if got.Messages[0].ToolCalls[0].ID != "call_1" || got.Messages[0].ToolCalls[0].Function.Arguments == "" {
		t.Fatalf("assistant tool call = %#v", got.Messages[0])
	}
	if got.Messages[1].Role != "tool" || got.Messages[1].ToolCallID != "call_1" || got.Messages[1].Content != "web app" {
		t.Fatalf("tool result message = %#v", got.Messages[1])
	}
}

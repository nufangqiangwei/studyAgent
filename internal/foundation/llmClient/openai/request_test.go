package openai

import (
	"agent/internal/foundation/llmClient"
	"encoding/json"
	"testing"
)

func TestBuilderIncludesTools(t *testing.T) {
	body, err := (Builder{}).Build(llmClient.Request{
		Model:    "gpt-test",
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
	function := tools[0].(map[string]any)["function"].(map[string]any)
	if function["name"] != "ask_user" {
		t.Fatalf("function name = %v, want ask_user", function["name"])
	}
	if function["parameters"] == nil {
		t.Fatal("function missing parameters")
	}
}

func TestBuilderIncludesToolCallMessages(t *testing.T) {
	body, err := (Builder{}).Build(llmClient.Request{
		Model: "gpt-test",
		Messages: []llmClient.Message{
			{Role: llmClient.RoleUser, Content: "hello"},
			{
				Role: llmClient.RoleAssistant,
				ToolCalls: []llmClient.ToolCall{{
					ID:    "call_1",
					Name:  "ask_user",
					Input: json.RawMessage(`{"question":"Which target?"}`),
				}},
			},
			{Role: llmClient.RoleTool, ToolCallID: "call_1", Content: "web app"},
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
				Type     string `json:"type"`
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
	if got.Messages[1].ToolCalls[0].ID != "call_1" || got.Messages[1].ToolCalls[0].Function.Name != "ask_user" {
		t.Fatalf("assistant tool call = %#v", got.Messages[1])
	}
	if got.Messages[1].ToolCalls[0].Function.Arguments != `{"question":"Which target?"}` {
		t.Fatalf("tool arguments = %q", got.Messages[1].ToolCalls[0].Function.Arguments)
	}
	if got.Messages[2].Role != "tool" || got.Messages[2].ToolCallID != "call_1" || got.Messages[2].Content != "web app" {
		t.Fatalf("tool result message = %#v", got.Messages[2])
	}
}

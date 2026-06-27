package gemini

import (
	"agent/internal/foundation/llmClient"
	"encoding/json"
	"testing"
)

func TestBuilderIncludesTools(t *testing.T) {
	body, err := (Builder{}).Build(llmClient.Request{
		Model:    "gemini-test",
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
	declarations := tools[0].(map[string]any)["functionDeclarations"].([]any)
	function := declarations[0].(map[string]any)
	if function["name"] != "ask_user" {
		t.Fatalf("function name = %v, want ask_user", function["name"])
	}
	if function["parameters"] == nil {
		t.Fatal("function missing parameters")
	}
}

func TestBuilderIncludesFunctionCallAndResponse(t *testing.T) {
	body, err := (Builder{}).Build(llmClient.Request{
		Model: "gemini-test",
		Messages: []llmClient.Message{
			{Role: llmClient.RoleSystem, Content: "system prompt"},
			{
				Role: llmClient.RoleAssistant,
				ToolCalls: []llmClient.ToolCall{{
					ID:    "call_1",
					Name:  "ask_user",
					Input: json.RawMessage(`{"question":"Which target?"}`),
				}},
			},
			{Role: llmClient.RoleTool, Name: "ask_user", ToolCallID: "call_1", Content: "web app"},
		},
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	var got struct {
		SystemInstruction struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"systemInstruction"`
		Contents []struct {
			Role  string `json:"role"`
			Parts []struct {
				FunctionCall *struct {
					ID   string          `json:"id"`
					Name string          `json:"name"`
					Args json.RawMessage `json:"args"`
				} `json:"functionCall"`
				FunctionResponse *struct {
					ID       string         `json:"id"`
					Name     string         `json:"name"`
					Response map[string]any `json:"response"`
				} `json:"functionResponse"`
			} `json:"parts"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if got.SystemInstruction.Parts[0].Text != "system prompt" {
		t.Fatalf("system instruction = %#v", got.SystemInstruction)
	}
	if got.Contents[0].Role != "model" || got.Contents[0].Parts[0].FunctionCall.ID != "call_1" {
		t.Fatalf("function call content = %#v", got.Contents[0])
	}
	if got.Contents[1].Role != "user" || got.Contents[1].Parts[0].FunctionResponse.ID != "call_1" || got.Contents[1].Parts[0].FunctionResponse.Response["content"] != "web app" {
		t.Fatalf("function response content = %#v", got.Contents[1])
	}
}

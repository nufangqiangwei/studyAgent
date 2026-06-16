package provider

import (
	"encoding/json"
	"testing"

	"agent/internal/llm"
)

func TestOpenAIParserPreservesToolCallID(t *testing.T) {
	resp, err := (openAIParser{provider: "openai"}).Parse(llm.Request{Model: "gpt-test"}, []byte(`{
  "model": "gpt-test",
  "choices": [{
    "finish_reason": "tool_calls",
    "message": {
      "content": "",
      "tool_calls": [{
        "id": "call_1",
        "type": "function",
        "function": {"name": "ask_user", "arguments": "{\"question\":\"Which target?\"}"}
      }]
    }
  }]
}`))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].ID != "call_1" || resp.ToolCalls[0].Name != "ask_user" {
		t.Fatalf("tool calls = %#v", resp.ToolCalls)
	}
}

func TestAnthropicParserPreservesToolUseID(t *testing.T) {
	resp, err := (anthropicParser{provider: "anthropic"}).Parse(llm.Request{Model: "claude-test"}, []byte(`{
  "model": "claude-test",
  "stop_reason": "tool_use",
  "content": [{
    "type": "tool_use",
    "id": "toolu_1",
    "name": "ask_user",
    "input": {"question": "Which target?"}
  }]
}`))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].ID != "toolu_1" || resp.ToolCalls[0].Name != "ask_user" {
		t.Fatalf("tool calls = %#v", resp.ToolCalls)
	}
}

func TestGeminiParserPreservesFunctionCallID(t *testing.T) {
	resp, err := (geminiParser{}).Parse(llm.Request{Model: "gemini-test"}, []byte(`{
  "candidates": [{
    "finishReason": "STOP",
    "content": {
      "parts": [{
        "functionCall": {
          "id": "call_1",
          "name": "ask_user",
          "args": {"question": "Which target?"}
        }
      }]
    }
  }]
}`))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].ID != "call_1" || resp.ToolCalls[0].Name != "ask_user" {
		t.Fatalf("tool calls = %#v", resp.ToolCalls)
	}

	var args map[string]string
	if err := json.Unmarshal(resp.ToolCalls[0].Input, &args); err != nil {
		t.Fatalf("unmarshal args: %v", err)
	}
	if args["question"] != "Which target?" {
		t.Fatalf("args = %#v", args)
	}
}

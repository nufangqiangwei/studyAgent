package agent

import (
	"agent/internal/foundation/llmClient"
	"context"
	"strings"
	"testing"
)

func TestToolsTesterAgentUsesToolsPromptAndDefaultTools(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "tool ready",
			},
		},
	}

	var out strings.Builder
	toolsAgent, err := NewToolsTesterAgent(context.Background(), CreatAgentOptions{
		LLM:      model,
		Model:    "mock-native",
		In:       strings.NewReader(""),
		Out:      &out,
		MaxSteps: 1,
	})
	if err != nil {
		t.Fatalf("NewToolsTesterAgent returned error: %v", err)
	}
	if toolsAgent.Name() != ToolsTesterAgentName {
		t.Fatalf("Name = %q, want %q", toolsAgent.Name(), ToolsTesterAgentName)
	}
	assertAgentDefaultTools(t, toolsAgent.Tools())

	if err := toolsAgent.Run(context.Background(), "test read_file tool"); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "tool ready") {
		t.Fatalf("output = %q, want tool ready", got)
	}
	if len(model.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(model.requests))
	}

	req := model.requests[0]
	assertLLMDefaultTools(t, req.Tools)
	if len(req.Messages) == 0 || !strings.Contains(req.Messages[0].Content, "Tool Testing Agent") {
		t.Fatalf("system prompt missing tool tester instructions: %#v", req.Messages)
	}
}

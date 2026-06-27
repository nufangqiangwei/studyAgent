package agent

import (
	"agent/internal/capability/tool"
	"agent/internal/foundation/llmClient"
	"context"
	"strings"
	"testing"
)

func TestAnalyzeAgentUsesAnalyzePromptAndDefaultTools(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "analysis ready",
			},
		},
	}

	var out strings.Builder
	analyzeAgent, err := NewAnalyzeAgent(context.Background(), CreatAgentOptions{
		LLM:      model,
		Model:    "mock-native",
		In:       strings.NewReader(""),
		Out:      &out,
		MaxSteps: 1,
	})
	if err != nil {
		t.Fatalf("NewAnalyzeAgent returned error: %v", err)
	}
	if analyzeAgent.Name() != AnalyzeAgentName {
		t.Fatalf("Name = %q, want %q", analyzeAgent.Name(), AnalyzeAgentName)
	}
	assertAgentDefaultTools(t, analyzeAgent.Tools())
	assertAgentDefaultTools(t, tool.RegisteredTools())

	if err := analyzeAgent.Run(context.Background(), "研究 AI 代码助手"); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "analysis ready") {
		t.Fatalf("output = %q, want analysis ready", got)
	}
	if len(model.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(model.requests))
	}

	req := model.requests[0]
	assertLLMDefaultTools(t, req.Tools)
	if len(req.Messages) == 0 || !strings.Contains(req.Messages[0].Content, "研究需求发掘 Agent") {
		t.Fatalf("system prompt missing analyze agent instructions: %#v", req.Messages)
	}
}

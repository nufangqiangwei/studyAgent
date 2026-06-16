package agent

import (
	"context"
	"strings"
	"testing"

	"agent/internal/llm"
	"agent/internal/tools"
)

func TestDefaultAgentUsesNativePromptAndAskUserTool(t *testing.T) {
	model := &scriptedLLM{
		responses: []llm.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "default ready",
			},
		},
	}

	var out strings.Builder
	defaultAgent, err := NewDefaultAgent(context.Background(), CreatAgentOptions{
		LLM:      model,
		Model:    "mock-native",
		In:       strings.NewReader(""),
		Out:      &out,
		MaxSteps: 1,
	})
	if err != nil {
		t.Fatalf("NewDefaultAgent returned error: %v", err)
	}
	if defaultAgent.Name() != DefaultAgentName {
		t.Fatalf("Name = %q, want %q", defaultAgent.Name(), DefaultAgentName)
	}
	if got := defaultAgent.Tools(); len(got) != 1 || got[0].Name() != tools.AskUserToolName {
		t.Fatalf("agent tools = %#v, want only ask_user", got)
	}

	if err := defaultAgent.Run(context.Background(), "summarize project"); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "default ready") {
		t.Fatalf("output = %q, want default ready", got)
	}
	if len(model.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(model.requests))
	}

	req := model.requests[0]
	if len(req.Tools) != 1 || req.Tools[0].Name != tools.AskUserToolName {
		t.Fatalf("tools = %#v, want only ask_user", req.Tools)
	}
	if len(req.Messages) == 0 || !strings.Contains(req.Messages[0].Content, "agent development assistant") {
		t.Fatalf("system prompt missing native agent instructions: %#v", req.Messages)
	}
}

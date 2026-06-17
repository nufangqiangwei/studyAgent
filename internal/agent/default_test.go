package agent

import (
	"context"
	"strings"
	"testing"

	"agent/internal/llm"
	"agent/internal/tools"
)

func TestDefaultAgentUsesNativePromptAndDefaultTools(t *testing.T) {
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
	assertAgentDefaultTools(t, defaultAgent.Tools())

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
	assertLLMDefaultTools(t, req.Tools)
	if len(req.Messages) == 0 || !strings.Contains(req.Messages[0].Content, "agent development assistant") {
		t.Fatalf("system prompt missing native agent instructions: %#v", req.Messages)
	}
}

func assertAgentDefaultTools(t *testing.T, got []tools.Tool) {
	t.Helper()

	want := []string{
		tools.AskUserToolName,
		tools.GetWorkspaceSummaryToolName,
		tools.ListFilesToolName,
		tools.ReadFileToolName,
		tools.SearchTextToolName,
	}
	if len(got) != len(want) {
		t.Fatalf("agent tools = %d, want %d: %#v", len(got), len(want), got)
	}
	for i, tool := range got {
		if tool.Name() != want[i] {
			t.Fatalf("agent tool[%d] = %q, want %q", i, tool.Name(), want[i])
		}
	}
}

func assertLLMDefaultTools(t *testing.T, got []llm.ToolDefinition) {
	t.Helper()

	want := []string{
		tools.AskUserToolName,
		tools.GetWorkspaceSummaryToolName,
		tools.ListFilesToolName,
		tools.ReadFileToolName,
		tools.SearchTextToolName,
	}
	if len(got) != len(want) {
		t.Fatalf("llm tools = %d, want %d: %#v", len(got), len(want), got)
	}
	for i, tool := range got {
		if tool.Name != want[i] {
			t.Fatalf("llm tool[%d] = %q, want %q", i, tool.Name, want[i])
		}
	}
}

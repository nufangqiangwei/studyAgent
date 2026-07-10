package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/agents/builtinagents/prompt"
	"context"
	"strings"
	"testing"
	"time"
)

func TestDefaultAgentStartsWithPersonalAssistantPromptAndName(t *testing.T) {
	store := agents2.NewMemorySnapshotStore()
	agent, err := NewDefaultAgent(
		WithSnapshotStore(store),
		WithModelName("mock-native"),
		WithTools([]agents2.ToolSpec{{Name: "ask_user"}}),
		WithAgentClock(func() time.Time {
			return time.Date(2026, 7, 7, 8, 0, 0, 0, time.UTC)
		}),
	)
	if err != nil {
		t.Fatalf("NewDefaultAgent returned error: %v", err)
	}
	if agent.Name() != DefaultAgentName {
		t.Fatalf("Name = %q, want %q", agent.Name(), DefaultAgentName)
	}

	startResult, err := agent.Start(context.Background(), agents2.AgentStartInput{
		TaskID: "task_default",
		Input:  "help me plan weekend errands",
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	payload, request := mustModelRequestEvent(t, startResult.Events[0])
	if request.Agent != DefaultAgentName || payload.Agent != DefaultAgentName {
		t.Fatalf("request agent=%q payload agent=%q, want %q", request.Agent, payload.Agent, DefaultAgentName)
	}
	if request.Model != "mock-native" {
		t.Fatalf("request model = %q, want mock-native", request.Model)
	}
	if len(request.Tools) != 1 || request.Tools[0].Name != "ask_user" {
		t.Fatalf("request tools = %#v, want ask_user", request.Tools)
	}
	if len(request.Messages) == 0 || !strings.Contains(request.Messages[0].Content, "个人生活助手") {
		t.Fatalf("system prompt missing personal assistant instructions: %#v", request.Messages)
	}
	if !strings.Contains(request.Messages[0].Content, "不用于无目的地窥探用户信息") {
		t.Fatalf("system prompt missing private data boundary: %#v", request.Messages[0])
	}
	if !strings.Contains(request.Messages[0].Content, prompt.DefaultUserHabitContext) {
		t.Fatalf("system prompt missing user habit context: %#v", request.Messages[0])
	}
	if !strings.Contains(request.Messages[0].Content, "<task_context>") ||
		!strings.Contains(request.Messages[0].Content, "当前时间：2026-07-07T08:00:00Z") ||
		!strings.Contains(request.Messages[0].Content, "用户原始输入：help me plan weekend errands") ||
		!strings.Contains(request.Messages[0].Content, "- ask_user") {
		t.Fatalf("system prompt missing filled task context: %#v", request.Messages[0])
	}
	if len(request.Messages) < 2 || request.Messages[1].Role != "user" || request.Messages[1].Content != "help me plan weekend errands" {
		t.Fatalf("user message = %#v, want original input", request.Messages)
	}
	if startResult.Events[0].Metadata["agent"] != DefaultAgentName {
		t.Fatalf("event metadata = %#v, want default agent", startResult.Events[0].Metadata)
	}
	snapshot, ok, err := store.Load(context.Background(), DefaultAgentName, "task_default")
	if err != nil || !ok {
		t.Fatalf("store.Load ok=%v err=%v, want snapshot", ok, err)
	}
	if snapshot.Agent != DefaultAgentName || snapshot.Phase != agents2.BusinessPhaseCallingModel {
		t.Fatalf("snapshot = %#v, want default calling model", snapshot)
	}
}

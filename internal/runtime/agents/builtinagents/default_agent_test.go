package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
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
	if len(request.Messages) == 0 || !strings.Contains(request.Messages[0].Content, "personal life assistant") {
		t.Fatalf("system prompt missing personal assistant instructions: %#v", request.Messages)
	}
	if !strings.Contains(request.Messages[0].Content, "repository or workspace tools") {
		t.Fatalf("system prompt missing workspace boundary: %#v", request.Messages[0])
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

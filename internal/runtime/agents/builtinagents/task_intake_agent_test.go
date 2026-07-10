package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"context"
	"strings"
	"testing"
	"time"
)

func TestTaskIntakeAgentStartsWithTaskIntakePromptAndName(t *testing.T) {
	store := agents2.NewMemorySnapshotStore()
	agent, err := NewTaskIntakeAgent(
		WithSnapshotStore(store),
		WithModelName("mock-native"),
		WithTools([]agents2.ToolSpec{{Name: "order.lookup"}}),
		WithAgentClock(func() time.Time {
			return time.Date(2026, 7, 8, 8, 0, 0, 0, time.UTC)
		}),
	)
	if err != nil {
		t.Fatalf("NewTaskIntakeAgent returned error: %v", err)
	}
	if agent.Name() != TaskIntakeAgentName {
		t.Fatalf("Name = %q, want %q", agent.Name(), TaskIntakeAgentName)
	}

	startResult, err := agent.Start(context.Background(), agents2.AgentStartInput{
		TaskID: "task_intake",
		Input:  "my headphones are broken, help me deal with it",
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	payload, request := mustModelRequestEvent(t, startResult.Events[0])
	if request.Agent != TaskIntakeAgentName || payload.Agent != TaskIntakeAgentName {
		t.Fatalf("request agent=%q payload agent=%q, want %q", request.Agent, payload.Agent, TaskIntakeAgentName)
	}
	if request.Model != "mock-native" {
		t.Fatalf("request model = %q, want mock-native", request.Model)
	}
	if len(request.Tools) != 1 || request.Tools[0].Name != "order.lookup" {
		t.Fatalf("request tools = %#v, want order.lookup", request.Tools)
	}
	if len(request.Messages) == 0 || !strings.Contains(request.Messages[0].Content, "TaskIntakeAgent") {
		t.Fatalf("system prompt missing task intake instructions: %#v", request.Messages)
	}
	if !strings.Contains(request.Messages[0].Content, `"status": "ready"`) {
		t.Fatalf("system prompt missing ready JSON schema: %#v", request.Messages[0])
	}
	if startResult.Events[0].Metadata["agent"] != TaskIntakeAgentName {
		t.Fatalf("event metadata = %#v, want task-intake agent", startResult.Events[0].Metadata)
	}
	snapshot, ok, err := store.Load(context.Background(), TaskIntakeAgentName, "task_intake")
	if err != nil || !ok {
		t.Fatalf("store.Load ok=%v err=%v, want snapshot", ok, err)
	}
	if snapshot.Agent != TaskIntakeAgentName || snapshot.Phase != agents2.BusinessPhaseCallingModel {
		t.Fatalf("snapshot = %#v, want task intake calling model", snapshot)
	}
}

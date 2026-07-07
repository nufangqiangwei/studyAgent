package app

import (
	agents2 "agent/internal/runtime/agents"
	"testing"
)

func TestNewRuntimeAgentUsesDefaultAgentWhenSelected(t *testing.T) {
	runner := &AppAgentRunner{
		current: defaultAgentName,
		opts: AppAgentRunnerOptions{
			MaxSteps: 5,
			Model:    "mock-native",
		},
	}

	agent, err := runner.newRuntimeAgent(t.TempDir(), agents2.NewMemorySnapshotStore(), nil)
	if err != nil {
		t.Fatalf("newRuntimeAgent returned error: %v", err)
	}
	if agent.Name() != defaultAgentName {
		t.Fatalf("agent name = %q, want %q", agent.Name(), defaultAgentName)
	}
}

func TestRuntimeAgentNameKeepsDefaultDistinct(t *testing.T) {
	if got := runtimeAgentName(defaultAgentName); got != defaultAgentName {
		t.Fatalf("runtimeAgentName(default) = %q, want %q", got, defaultAgentName)
	}
}

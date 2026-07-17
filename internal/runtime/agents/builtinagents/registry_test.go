package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"reflect"
	"testing"
)

func TestFactoryRegistryContainsBuiltins(t *testing.T) {
	registry, err := NewFactoryRegistry()
	if err != nil {
		t.Fatalf("NewFactoryRegistry returned error: %v", err)
	}
	want := []string{AnalyzeAgentName, DefaultAgentName, ToolsTesterAgentName}
	if got := registry.ListNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ListNames = %#v, want %#v", got, want)
	}
	agent, err := registry.Create(DefaultAgentName, agents2.FactoryConfig{
		ModelName:     "mock-native",
		SnapshotStore: agents2.NewMemorySnapshotStore(),
		MaxTurns:      5,
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if agent.Name() != DefaultAgentName {
		t.Fatalf("agent name = %q", agent.Name())
	}
}

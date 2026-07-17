package app

import (
	"agent/internal/runtime/agents/builtinagents"
	"context"
	"testing"
)

func TestRuntimeSetupBuilderCreatesSelectedAgentThroughRegistry(t *testing.T) {
	factories, err := builtinagents.NewFactoryRegistry()
	if err != nil {
		t.Fatalf("NewFactoryRegistry returned error: %v", err)
	}
	builder, err := newRuntimeSetupBuilder(runtimeSetupOptions{
		Model: "mock-native", MaxSteps: 5, WorkDir: t.TempDir(),
		RuntimeStoreRoot: t.TempDir(), Agents: factories,
	})
	if err != nil {
		t.Fatalf("newRuntimeSetupBuilder returned error: %v", err)
	}
	setup, err := builder.Build(context.Background(), "task_default", builtinagents.DefaultAgentName)
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	defer setup.Close()
	if got := setup.TaskRuntime.AgentName(); got != builtinagents.DefaultAgentName {
		t.Fatalf("agent name = %q, want %q", got, builtinagents.DefaultAgentName)
	}
}

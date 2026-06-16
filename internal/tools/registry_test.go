package tools

import "testing"

func TestNewDefaultRegistryRegistersAndPublishesTools(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry returned error: %v", err)
	}

	assertOnlyAskUserTool(t, registry.List())
	assertOnlyAskUserTool(t, RegisteredTools())
}

func assertOnlyAskUserTool(t *testing.T, got []Tool) {
	t.Helper()

	if len(got) != 1 {
		t.Fatalf("tools = %d, want 1: %#v", len(got), got)
	}
	if got[0].Name() != AskUserToolName {
		t.Fatalf("tool name = %q, want %q", got[0].Name(), AskUserToolName)
	}
}

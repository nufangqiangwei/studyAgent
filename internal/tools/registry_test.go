package tools

import "testing"

func TestNewDefaultRegistryRegistersAndPublishesTools(t *testing.T) {
	registry, err := NewDefaultRegistry()
	if err != nil {
		t.Fatalf("NewDefaultRegistry returned error: %v", err)
	}

	assertDefaultTools(t, registry.List())
	assertDefaultTools(t, RegisteredTools())
}

func assertDefaultTools(t *testing.T, got []Tool) {
	t.Helper()

	want := []string{
		AskUserToolName,
		GetWorkspaceSummaryToolName,
		ListFilesToolName,
		ReadFileToolName,
		SearchTextToolName,
	}
	if len(got) != len(want) {
		t.Fatalf("tools = %d, want %d: %#v", len(got), len(want), got)
	}
	for i, tool := range got {
		if tool.Name() != want[i] {
			t.Fatalf("tool[%d] name = %q, want %q", i, tool.Name(), want[i])
		}
	}
}

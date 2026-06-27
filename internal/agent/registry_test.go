package agent

import (
	"agent/internal/foundation/llmClient"
	"context"
	"reflect"
	"testing"
)

func TestCatalogListsRegisteredAgentFactories(t *testing.T) {
	want := []string{AnalyzeAgentName, DefaultAgentName, ToolsTesterAgentName}
	if got := Catalog.ListAgentNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("agent names = %#v, want %#v", got, want)
	}
	if got := RegisteredAgentNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("registered agent names = %#v, want %#v", got, want)
	}
}

func TestCatalogSelectAgentCreatesAgent(t *testing.T) {
	factory, err := Catalog.SelectAgent(DefaultAgentName)
	if err != nil {
		t.Fatalf("SelectAgent returned error: %v", err)
	}

	created, err := factory(context.Background(), CreatAgentOptions{
		LLM: &scriptedLLM{responses: []llmClient.Response{
			{Provider: "mock", Model: "mock-native", Content: "ready"},
		}},
		Model:    "mock-native",
		MaxSteps: 1,
	})
	if err != nil {
		t.Fatalf("factory returned error: %v", err)
	}
	if created.Name() != DefaultAgentName {
		t.Fatalf("agent name = %q, want %q", created.Name(), DefaultAgentName)
	}
}

func TestCatalogSelectAgentRejectsUnknownName(t *testing.T) {
	_, err := Catalog.SelectAgent("missing")
	if err == nil {
		t.Fatal("SelectAgent returned nil error")
	}
}

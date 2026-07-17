package runservice

import (
	"agent/internal/runtime/agents"
	"agent/internal/runtime/persistence"
	"context"
	"reflect"
	"testing"
)

func TestServiceListsAndSelectsRegisteredAgents(t *testing.T) {
	registry, err := agents.NewFactoryRegistry(
		agents.FactorySpec{Name: "alpha", Factory: unusedFactory},
		agents.FactorySpec{Name: "custom", Factory: unusedFactory},
	)
	if err != nil {
		t.Fatalf("NewFactoryRegistry returned error: %v", err)
	}
	service, err := New(Options{Builder: unusedBuilder{}, Agents: registry, InitialAgent: "alpha"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if got := service.ListAgentNames(); !reflect.DeepEqual(got, []string{"alpha", "custom"}) {
		t.Fatalf("ListAgentNames = %#v", got)
	}
	if err := service.SelectAgent("CUSTOM"); err != nil {
		t.Fatalf("SelectAgent returned error: %v", err)
	}
	if got := service.ActiveAgentName(); got != "custom" {
		t.Fatalf("ActiveAgentName = %q", got)
	}
}

func unusedFactory(agents.FactoryConfig) (agents.Agent, error) { return nil, nil }

type unusedBuilder struct{}

func (unusedBuilder) Build(context.Context, string, string) (*Setup, error) { return nil, nil }
func (unusedBuilder) OpenStorage(context.Context) (persistence.RuntimeStorage, *persistence.WorkQueue, func(), error) {
	return nil, nil, nil, nil
}
func (unusedBuilder) DefaultWorkDir() string { return "" }

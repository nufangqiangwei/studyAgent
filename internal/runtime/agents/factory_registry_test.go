package agents

import (
	"context"
	"testing"
)

func TestFactoryRegistryCreatesCanonicalAgent(t *testing.T) {
	registry, err := NewFactoryRegistry(FactorySpec{
		Name: "worker",
		Factory: func(config FactoryConfig) (Agent, error) {
			return factoryTestAgent{name: "worker", config: config}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewFactoryRegistry returned error: %v", err)
	}

	agent, err := registry.Create("WORKER", FactoryConfig{ModelName: "mock", MaxTurns: 4})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	created := agent.(factoryTestAgent)
	if created.config.ModelName != "mock" || created.config.MaxTurns != 4 {
		t.Fatalf("factory config = %#v", created.config)
	}
	if canonical, ok := registry.CanonicalName("Worker"); !ok || canonical != "worker" {
		t.Fatalf("CanonicalName = %q, %v", canonical, ok)
	}
}

func TestFactoryRegistryRejectsCaseInsensitiveDuplicate(t *testing.T) {
	registry, err := NewFactoryRegistry(FactorySpec{Name: "worker", Factory: func(FactoryConfig) (Agent, error) {
		return factoryTestAgent{name: "worker"}, nil
	}})
	if err != nil {
		t.Fatalf("NewFactoryRegistry returned error: %v", err)
	}
	if err := registry.Register(FactorySpec{Name: "WORKER", Factory: func(FactoryConfig) (Agent, error) {
		return factoryTestAgent{name: "WORKER"}, nil
	}}); err == nil {
		t.Fatal("Register returned nil error for duplicate name")
	}
}

type factoryTestAgent struct {
	name   string
	config FactoryConfig
}

func (a factoryTestAgent) Name() string { return a.name }
func (factoryTestAgent) Start(context.Context, AgentStartInput) (AgentResult, error) {
	return AgentResult{}, nil
}
func (factoryTestAgent) Resume(context.Context, AgentResumeInput) (AgentResult, error) {
	return AgentResult{}, nil
}
func (factoryTestAgent) Snapshot(context.Context, string) (AgentSnapshot, bool, error) {
	return AgentSnapshot{}, false, nil
}

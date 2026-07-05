package agents

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Registry struct {
	mu     sync.RWMutex
	agents map[string]Agent
}

func NewRegistry(agents ...Agent) (*Registry, error) {
	registry := &Registry{agents: make(map[string]Agent, len(agents))}
	for _, agent := range agents {
		if err := registry.Register(agent); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func (r *Registry) Register(agent Agent) error {
	if r == nil {
		return fmt.Errorf("agent registry is nil")
	}
	if agent == nil {
		return fmt.Errorf("agent is required")
	}
	name := strings.TrimSpace(agent.Name())
	if name == "" {
		return fmt.Errorf("agent name is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.agents[name]; exists {
		return fmt.Errorf("agent %q already exists", name)
	}
	r.agents[name] = agent
	return nil
}

func (r *Registry) Lookup(name string) (Agent, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	agent, ok := r.agents[name]
	return agent, ok
}

func (r *Registry) ListNames() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.agents))
	for name := range r.agents {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

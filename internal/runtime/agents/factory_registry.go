package agents

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// FactoryConfig contains the runtime-scoped dependencies needed to construct
// an agent. Concrete agent packages translate this generic configuration into
// their own options.
type FactoryConfig struct {
	ModelName     string
	SnapshotStore SnapshotStore
	Tools         []ToolSpec
	Source        string
	MaxTurns      int
}

func (c FactoryConfig) Clone() FactoryConfig {
	cloned := c
	cloned.Tools = cloneToolSpecs(c.Tools)
	return cloned
}

type Factory func(FactoryConfig) (Agent, error)

type FactorySpec struct {
	Name    string
	Factory Factory
}

// FactoryRegistry is the construction-time counterpart of Registry. Registry
// owns task-scoped agent instances; FactoryRegistry owns replaceable factories
// that can create those instances from runtime dependencies.
type FactoryRegistry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

func NewFactoryRegistry(specs ...FactorySpec) (*FactoryRegistry, error) {
	registry := &FactoryRegistry{factories: make(map[string]Factory, len(specs))}
	for _, spec := range specs {
		if err := registry.Register(spec); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func (r *FactoryRegistry) Register(spec FactorySpec) error {
	if r == nil {
		return fmt.Errorf("agent factory registry is nil")
	}
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return fmt.Errorf("agent factory name is required")
	}
	if spec.Factory == nil {
		return fmt.Errorf("agent factory %q is required", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for registered := range r.factories {
		if strings.EqualFold(registered, name) {
			return fmt.Errorf("agent factory %q already exists", registered)
		}
	}
	r.factories[name] = spec.Factory
	return nil
}

func (r *FactoryRegistry) Lookup(name string) (Factory, bool) {
	canonical, ok := r.CanonicalName(name)
	if !ok {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.factories[canonical]
	return factory, ok
}

func (r *FactoryRegistry) Create(name string, config FactoryConfig) (Agent, error) {
	canonical, ok := r.CanonicalName(name)
	if !ok {
		return nil, fmt.Errorf("agent %q not found", strings.TrimSpace(name))
	}
	factory, ok := r.Lookup(canonical)
	if !ok {
		return nil, fmt.Errorf("agent factory %q not found", canonical)
	}
	agent, err := factory(config.Clone())
	if err != nil {
		return nil, fmt.Errorf("create agent %q: %w", canonical, err)
	}
	if agent == nil {
		return nil, fmt.Errorf("create agent %q: factory returned nil", canonical)
	}
	if actual := strings.TrimSpace(agent.Name()); actual != canonical {
		return nil, fmt.Errorf("create agent %q: factory returned agent %q", canonical, actual)
	}
	return agent, nil
}

func (r *FactoryRegistry) CanonicalName(name string) (string, bool) {
	if r == nil {
		return "", false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for candidate := range r.factories {
		if strings.EqualFold(candidate, name) {
			return candidate, true
		}
	}
	return "", false
}

func (r *FactoryRegistry) ListNames() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.factories))
	for name := range r.factories {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

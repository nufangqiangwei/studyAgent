package event

import (
	"fmt"
	"sort"
	"sync"
)

type Registry struct {
	mu          sync.RWMutex
	definitions map[Type]Definition
}

func NewRegistry(definitions ...Definition) (*Registry, error) {
	registry := &Registry{
		definitions: make(map[Type]Definition, len(definitions)),
	}
	for _, definition := range definitions {
		if err := registry.Define(definition); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func DefaultRegistry() *Registry {
	registry, err := NewRegistry(BuiltinDefinitions()...)
	if err != nil {
		panic(fmt.Errorf("create default event registry: %w", err))
	}
	return registry
}

func (r *Registry) Define(definition Definition) error {
	if r == nil {
		return fmt.Errorf("event registry is nil")
	}
	definition = definition.normalized()
	if err := definition.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.definitions[definition.Type]; exists {
		return fmt.Errorf("event definition %q: already exists", definition.Type)
	}
	r.definitions[definition.Type] = definition
	return nil
}

func (r *Registry) Lookup(eventType Type) (Definition, bool) {
	if r == nil {
		return Definition{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	definition, ok := r.definitions[eventType]
	return definition, ok
}

func (r *Registry) List() []Definition {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	definitions := make([]Definition, 0, len(r.definitions))
	for _, definition := range r.definitions {
		definitions = append(definitions, definition)
	}
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].Type < definitions[j].Type
	})
	return definitions
}

func (r *Registry) NewEvent(eventType Type, payload any, options ...EventOption) (Event, error) {
	if r == nil {
		return Event{}, fmt.Errorf("event registry is nil")
	}
	if _, ok := r.Lookup(eventType); !ok {
		return Event{}, fmt.Errorf("event definition %q: not registered", eventType)
	}
	return newEvent(eventType, payload, options...)
}

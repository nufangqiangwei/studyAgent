package tool

import (
	"agent/internal/capability/builtin"
	"agent/internal/foundation/policy"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

type Tool interface {
	Name() string
	Description() string
	InputSchema() json.RawMessage
	Execute(ctx context.Context, input json.RawMessage) (Result, error)
}

type Result = builtin.Result

type PolicyAnalyzer interface {
	AnalyzePolicy(input json.RawMessage) (PolicyFacts, error)
}

type PolicyAnalyzerFunc func(input json.RawMessage) (PolicyFacts, error)

func (f PolicyAnalyzerFunc) AnalyzePolicy(input json.RawMessage) (PolicyFacts, error) {
	if f == nil {
		return PolicyFacts{}, nil
	}
	return f(input)
}

type PolicyFacts struct {
	Paths     []string
	Command   []string
	DryRun    bool
	Read      bool
	Write     bool
	Delete    bool
	Exec      bool
	Network   bool
	HighRisk  bool
	Operation string
}

type Registry struct {
	tools  map[string]Tool
	policy policy.Checker
}

var (
	currentRegistryMu sync.RWMutex
	currentRegistry   *Registry
)

type RegistryOption func(*Registry)

func WithPolicy(checker policy.Checker) RegistryOption {
	return func(registry *Registry) {
		if checker != nil {
			registry.policy = checker
		}
	}
}

func NewRegistry(options ...RegistryOption) *Registry {
	registry := &Registry{
		tools:  make(map[string]Tool),
		policy: policy.Default(),
	}
	for _, option := range options {
		if option != nil {
			option(registry)
		}
	}
	return registry
}

func CurrentRegistry() *Registry {
	currentRegistryMu.RLock()
	defer currentRegistryMu.RUnlock()
	return currentRegistry
}

func SetCurrentRegistry(registry *Registry) {
	currentRegistryMu.Lock()
	defer currentRegistryMu.Unlock()
	currentRegistry = registry
}

func RegisteredTools() []Tool {
	currentRegistryMu.RLock()
	registry := currentRegistry
	currentRegistryMu.RUnlock()
	if registry == nil {
		return nil
	}
	return registry.List()
}

func (r *Registry) Register(tool Tool) error {
	return r.RegisterWithPolicyAnalyzer(tool, nil)
}

func (r *Registry) RegisterWithPolicyAnalyzer(tool Tool, analyzer PolicyAnalyzer) error {
	if r == nil {
		return fmt.Errorf("register tool: nil registry")
	}
	if tool == nil {
		return fmt.Errorf("register tool: nil tool")
	}
	name := tool.Name()
	if name == "" {
		return fmt.Errorf("register tool: empty name")
	}
	if _, exists := r.tools[name]; exists {
		return fmt.Errorf("register tool %q: already exists", name)
	}
	if analyzer == nil {
		if candidate, ok := tool.(PolicyAnalyzer); ok {
			analyzer = candidate
		}
	}
	r.tools[name] = newPolicyTool(tool, analyzer, r.policy)
	return nil
}

func (r *Registry) Lookup(name string) (Tool, bool) {
	if r == nil {
		return nil, false
	}
	tool, ok := r.tools[name]
	return tool, ok
}

func (r *Registry) List() []Tool {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)

	result := make([]Tool, 0, len(names))
	for _, name := range names {
		result = append(result, r.tools[name])
	}
	return result
}

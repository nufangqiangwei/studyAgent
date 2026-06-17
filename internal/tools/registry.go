package tools

import (
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

type Result struct {
	Content  string          `json:"content"`
	Metadata map[string]any  `json:"metadata,omitempty"`
	Raw      json.RawMessage `json:"raw,omitempty"`
}

type Registry struct {
	tools map[string]Tool
}

var (
	currentRegistryMu sync.RWMutex
	currentRegistry   = mustNewDefaultRegistry()
)

func NewRegistry() *Registry {
	return &Registry{tools: make(map[string]Tool)}
}

func NewDefaultRegistry() (*Registry, error) {
	registry := NewRegistry()
	if err := RegisterDefaults(registry); err != nil {
		return nil, err
	}
	SetCurrentRegistry(registry)
	return registry, nil
}

func RegisterDefaults(registry *Registry) error {
	if registry == nil {
		return fmt.Errorf("register default tools: nil registry")
	}
	defaults := []Tool{
		NewAskUserTool(),
		NewListFilesTool(),
		NewReadFileTool(),
		NewSearchTextTool(),
		NewGetWorkspaceSummaryTool(),
	}
	for _, tool := range defaults {
		if err := registry.Register(tool); err != nil {
			return fmt.Errorf("register default tool %q: %w", tool.Name(), err)
		}
	}
	return nil
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
	r.tools[name] = tool
	return nil
}

func (r *Registry) Execute(ctx context.Context, name string, input json.RawMessage) (Result, error) {
	if r == nil {
		return Result{}, fmt.Errorf("tool registry is nil")
	}
	tool, ok := r.tools[name]
	if !ok {
		return Result{}, fmt.Errorf("unknown tool %q", name)
	}
	return tool.Execute(ctx, input)
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

func mustNewDefaultRegistry() *Registry {
	registry := NewRegistry()
	if err := RegisterDefaults(registry); err != nil {
		panic(err)
	}
	return registry
}

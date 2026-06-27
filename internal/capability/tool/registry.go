package tool

import (
	"agent/internal/capability/builtin/askUser"
	"agent/internal/capability/builtin/workspace"
	"agent/internal/content"
	"agent/internal/foundation/policy"
	"agent/internal/session"
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

type policyDecisionEventPayload struct {
	Request           policy.Request `json:"request"`
	Result            policy.Result  `json:"result"`
	Confirmed         *bool          `json:"confirmed,omitempty"`
	ConfirmationError string         `json:"confirmation_error,omitempty"`
}

type Registry struct {
	tools  map[string]Tool
	policy policy.Checker
}

var (
	currentRegistryMu sync.RWMutex
	currentRegistry   = mustNewDefaultRegistry()
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

func NewDefaultRegistry(options ...RegistryOption) (*Registry, error) {
	registry := NewRegistry(options...)
	if err := RegisterDefaults(registry); err != nil {
		return nil, err
	}
	SetCurrentRegistry(registry)
	return registry, nil
}

func RegisterDefaults(registry *Registry) error {
	if registry == nil {
		return fmt.Errorf("register default tool: nil registry")
	}
	defaults := []Tool{
		NewApplyPatchTool(),
		askUser.NewAskUserTool(),
		workspace.NewListFilesTool(),
		workspace.NewReadFileTool(),
		workspace.NewSearchTextTool(),
		workspace.NewGetWorkspaceSummaryTool(),
		NewWriteFileTool(),
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
	request := policyRequestForToolCall(name, input)
	checker := r.policy
	if checker == nil {
		checker = policy.Default()
	}
	decision := checker.Check(request)
	switch decision.Decision {
	case policy.Allow:
		if err := recordPolicyDecisionEvent(ctx, request, decision, nil, ""); err != nil {
			return Result{}, err
		}
	case policy.Ask:
		confirmed, err := confirmPolicyDecision(ctx, request, decision)
		if err != nil {
			if recordErr := recordPolicyDecisionEvent(ctx, request, decision, nil, err.Error()); recordErr != nil {
				return Result{}, recordErr
			}
			return Result{}, fmt.Errorf("policy confirmation for tool %q: %w", name, err)
		}
		if err := recordPolicyDecisionEvent(ctx, request, decision, &confirmed, ""); err != nil {
			return Result{}, err
		}
		if !confirmed {
			return Result{}, fmt.Errorf("policy denied tool %q: user declined confirmation: %s", name, decision.Reason)
		}
	case policy.Deny:
		if err := recordPolicyDecisionEvent(ctx, request, decision, nil, ""); err != nil {
			return Result{}, err
		}
		return Result{}, fmt.Errorf("policy denied tool %q: %s", name, decision.Reason)
	default:
		if err := recordPolicyDecisionEvent(ctx, request, decision, nil, ""); err != nil {
			return Result{}, err
		}
		return Result{}, fmt.Errorf("policy returned unknown decision %q for tool %q", decision.Decision, name)
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

func recordPolicyDecisionEvent(ctx context.Context, request policy.Request, result policy.Result, confirmed *bool, confirmationError string) error {
	env, ok := content.EnvFromContext(ctx)
	if !ok || env.Session == nil {
		return nil
	}
	scope := env.EventScope
	if scope.AgentName == "" {
		scope.AgentName = env.Config.AgentName
	}
	err := session.SaveEvent(ctx, env.Session, scope, session.EventTypePolicyDecision, policyDecisionEventPayload{
		Request:           request,
		Result:            result,
		Confirmed:         confirmed,
		ConfirmationError: confirmationError,
	})
	if err != nil {
		return fmt.Errorf("record policy decision event: %w", err)
	}
	return nil
}

func mustNewDefaultRegistry() *Registry {
	registry := NewRegistry()
	if err := RegisterDefaults(registry); err != nil {
		panic(err)
	}
	return registry
}

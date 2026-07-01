package tool

import (
	"agent/internal/capability/builtin"
	"agent/internal/capability/builtin/askUser"
	"agent/internal/capability/builtin/workspace"
	"agent/internal/content"
	"agent/internal/foundation/policy"
	"agent/internal/session"
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

type Tool interface {
	Name() string
	Description() string
	InputSchema() json.RawMessage
	Execute(ctx context.Context, input json.RawMessage) (Result, error)
}

type PolicyRequester interface {
	PolicyRequest(input json.RawMessage) policy.Request
}

type Result = builtin.Result

type ApprovalRequiredError struct {
	Request policy.Request
	Result  policy.Result
}

func (e *ApprovalRequiredError) Error() string {
	if e == nil {
		return "policy approval required"
	}
	return fmt.Sprintf("policy approval required for tool %q: %s", e.Request.ToolName, e.Result.Reason)
}

type policyDecisionEventPayload struct {
	Request           policy.Request `json:"request"`
	Result            policy.Result  `json:"result"`
	Confirmed         *bool          `json:"confirmed,omitempty"`
	ConfirmationError string         `json:"confirmation_error,omitempty"`
}

type Manage struct {
	tools               map[string]Tool
	policy              policy.Checker
	asyncPolicyApproval bool
}

var (
	currentRegistry *Manage
	toolsNames      []string
)

type ManageOption func(*Manage)

func init() {
	currentRegistry = NewManage()
	defaults := []Tool{
		workspace.NewApplyPatchTool(),
		askUser.NewAskUserTool(),
		workspace.NewListFilesTool(),
		workspace.NewReadFileTool(),
		workspace.NewSearchTextTool(),
		workspace.NewGetWorkspaceSummaryTool(),
		workspace.NewWriteFileTool(),
	}
	for _, tool := range defaults {
		if err := currentRegistry.register(tool); err != nil {
			panic(fmt.Errorf("register default tool %q: %w", tool.Name(), err))
		}
	}
	toolsNames = registeredToolNames(currentRegistry)
}

func WithPolicy(checker policy.Checker) ManageOption {
	return func(manager *Manage) {
		if checker != nil {
			manager.policy = checker
		}
	}
}

func WithAsyncPolicyApproval() ManageOption {
	return func(manager *Manage) {
		manager.asyncPolicyApproval = true
	}
}

func NewManage(options ...ManageOption) *Manage {
	manager := &Manage{
		tools:  make(map[string]Tool),
		policy: policy.Default(),
	}
	for _, option := range options {
		if option != nil {
			option(manager)
		}
	}
	return manager
}

func NewDefaultManage(options ...ManageOption) (*Manage, error) {
	return currentRegistry.Subset(AllToolNames(), options...)
}

func AllToolNames() []string {
	return append([]string(nil), toolsNames...)
}

func registeredToolNames(registry *Manage) []string {
	tools := registry.List()
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name())
	}
	return names
}

func AddTool(name string, manager *Manage) error {
	if manager == nil {
		return fmt.Errorf("add tool %q: nil Manage", name)
	}
	return addToolFrom(currentRegistry, name, manager)
}

func addToolFrom(source *Manage, name string, target *Manage) error {
	if source == nil {
		return fmt.Errorf("add tool %q: nil source Manage", name)
	}
	if target == nil {
		return fmt.Errorf("add tool %q: nil target Manage", name)
	}
	if name == "" {
		return fmt.Errorf("add tool: empty name")
	}
	tool, ok := source.tools[name]
	if !ok {
		return fmt.Errorf("add tool %q: not registered", name)
	}
	return target.register(tool)
}

func (r *Manage) register(tool Tool) error {
	if r == nil {
		return fmt.Errorf("register tool: nil Manage")
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

func (r *Manage) Subset(names []string, options ...ManageOption) (*Manage, error) {
	if r == nil {
		return nil, fmt.Errorf("select tools: nil Manage")
	}
	manager := &Manage{
		tools:               make(map[string]Tool, len(names)),
		policy:              r.policy,
		asyncPolicyApproval: r.asyncPolicyApproval,
	}
	if manager.policy == nil {
		manager.policy = policy.Default()
	}
	for _, option := range options {
		if option != nil {
			option(manager)
		}
	}
	for _, name := range names {
		if err := addToolFrom(r, name, manager); err != nil {
			return nil, fmt.Errorf("select tool: %w", err)
		}
	}
	return manager, nil
}

func (r *Manage) Execute(ctx context.Context, name string, input json.RawMessage) (Result, error) {
	if r == nil {
		return Result{}, fmt.Errorf("tool Manage is nil")
	}
	tool, ok := r.tools[name]
	if !ok {
		return Result{}, fmt.Errorf("unknown tool %q", name)
	}
	request := policyRequestForToolCall(name, input)
	if requester, ok := tool.(PolicyRequester); ok {
		request = requester.PolicyRequest(input)
	}
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
		if r.asyncPolicyApproval {
			if err := recordPolicyDecisionEvent(ctx, request, decision, nil, "approval required"); err != nil {
				return Result{}, err
			}
			return Result{}, &ApprovalRequiredError{Request: request, Result: decision}
		}
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

func (r *Manage) ExecuteApproved(ctx context.Context, name string, input json.RawMessage) (Result, error) {
	if r == nil {
		return Result{}, fmt.Errorf("tool Manage is nil")
	}
	tool, ok := r.tools[name]
	if !ok {
		return Result{}, fmt.Errorf("unknown tool %q", name)
	}
	return tool.Execute(ctx, input)
}

func (r *Manage) List() []Tool {
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

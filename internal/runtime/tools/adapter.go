package tools

import (
	"agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	reactor2 "agent/internal/runtime/reactor"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/foundation/policy"
)

type Option func(*config)

type config struct {
	manager             *tool.Manage
	toolNames           []string
	managerOptions      []tool.ManageOption
	workDir             string
	env                 *content.Env
	source              string
	returnExecutorError bool
}

func WithManager(manager *tool.Manage) Option {
	return func(config *config) {
		config.manager = manager
	}
}

func WithToolNames(names ...string) Option {
	return func(config *config) {
		config.toolNames = append([]string(nil), names...)
	}
}

func WithPolicy(checker policy.Checker) Option {
	return func(config *config) {
		if checker != nil {
			config.managerOptions = append(config.managerOptions, tool.WithPolicy(checker))
		}
	}
}

func WithAsyncPolicyApproval() Option {
	return func(config *config) {
		config.managerOptions = append(config.managerOptions, tool.WithAsyncPolicyApproval())
	}
}

func WithWorkDir(workDir string) Option {
	return func(config *config) {
		config.workDir = strings.TrimSpace(workDir)
	}
}

func WithEnv(env content.Env) Option {
	return func(config *config) {
		cloned := env.Clone()
		config.env = &cloned
	}
}

func WithSource(source string) Option {
	return func(config *config) {
		config.source = strings.TrimSpace(source)
	}
}

func WithExecutorErrors() Option {
	return func(config *config) {
		config.returnExecutorError = true
	}
}

type Adapter struct {
	manager             *tool.Manage
	workDir             string
	env                 *content.Env
	source              string
	returnExecutorError bool
}

func NewDefault(options ...Option) (*Adapter, error) {
	config := config{source: "new_runtime.tools"}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}

	manager, err := buildManager(config)
	if err != nil {
		return nil, err
	}
	source := config.source
	if source == "" {
		source = "new_runtime.tools"
	}
	return &Adapter{
		manager:             manager,
		workDir:             config.workDir,
		env:                 cloneEnv(config.env),
		source:              source,
		returnExecutorError: config.returnExecutorError,
	}, nil
}

func (a *Adapter) Specs() []agents.ToolSpec {
	if a == nil || a.manager == nil {
		return nil
	}
	managed := a.manager.List()
	specs := make([]agents.ToolSpec, 0, len(managed))
	for _, item := range managed {
		specs = append(specs, agents.ToolSpec{
			Name:        item.Name(),
			Description: item.Description(),
			InputSchema: append(json.RawMessage(nil), item.InputSchema()...),
		})
	}
	return specs
}

func (a *Adapter) ToolNames() []string {
	if a == nil || a.manager == nil {
		return nil
	}
	managed := a.manager.List()
	names := make([]string, 0, len(managed))
	for _, item := range managed {
		names = append(names, item.Name())
	}
	return names
}

func (a *Adapter) ExecuteEffect(ctx context.Context, runtime reactor2.TaskRuntime, effect reactor2.Effect) (reactor2.EffectResult, error) {
	if a == nil {
		return reactor2.EffectResult{}, fmt.Errorf("tools adapter is nil")
	}
	if a.manager == nil {
		return reactor2.EffectResult{}, fmt.Errorf("tools adapter manager is required")
	}
	if effect.Type != reactor2.EffectToolDispatch {
		return reactor2.EffectResult{}, fmt.Errorf("tools adapter: unsupported effect type %q", effect.Type)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var payload statemachine.ToolCallPayload
	if err := json.Unmarshal(effect.Payload, &payload); err != nil {
		return reactor2.EffectResult{}, fmt.Errorf("tools adapter: decode tool dispatch payload: %w", err)
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return reactor2.EffectResult{}, fmt.Errorf("tools adapter: tool_call_id is required")
	}
	if strings.TrimSpace(payload.ToolName) == "" {
		return reactor2.EffectResult{}, fmt.Errorf("tools adapter: tool_name is required")
	}

	result, err := a.manager.Execute(a.contextForTool(ctx), payload.ToolName, append(json.RawMessage(nil), payload.Arguments...))
	if err != nil {
		if a.returnExecutorError {
			return reactor2.EffectResult{}, err
		}
		event, eventErr := a.toolFailedEvent(effect.TaskID, payload, err)
		if eventErr != nil {
			return reactor2.EffectResult{}, eventErr
		}
		return reactor2.EffectResult{Events: []eventbus.Event{event}}, nil
	}

	event, err := a.toolCompletedEvent(effect.TaskID, payload, result)
	if err != nil {
		return reactor2.EffectResult{}, err
	}
	return reactor2.EffectResult{Events: []eventbus.Event{event}}, nil
}

func (a *Adapter) contextForTool(ctx context.Context) context.Context {
	if a == nil {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if a.env != nil {
		env := a.env.Clone()
		if a.workDir != "" {
			env.Config.WorkDir = a.workDir
		}
		return content.WithEnv(ctx, &env)
	}
	if a.workDir == "" {
		return ctx
	}
	if existing, ok := content.EnvFromContext(ctx); ok {
		env := existing.Clone()
		env.Config.WorkDir = a.workDir
		return content.WithEnv(ctx, &env)
	}
	return content.WithEnv(ctx, &content.Env{Config: content.Config{WorkDir: a.workDir}})
}

func (a *Adapter) toolCompletedEvent(taskID string, call statemachine.ToolCallPayload, result tool.Result) (eventbus.Event, error) {
	rawResult, err := encodeResult(result)
	if err != nil {
		return eventbus.Event{}, err
	}
	return eventbus.NewEvent(statemachine.TopicTask, statemachine.EventToolCompleted, statemachine.ToolCallPayload{
		ToolCallID: call.ToolCallID,
		ToolName:   call.ToolName,
		Arguments:  append(json.RawMessage(nil), call.Arguments...),
		Result:     rawResult,
	}, eventbus.WithTaskID(taskID), eventbus.WithSource(a.source))
}

func (a *Adapter) toolFailedEvent(taskID string, call statemachine.ToolCallPayload, callErr error) (eventbus.Event, error) {
	return eventbus.NewEvent(statemachine.TopicTask, statemachine.EventToolFailed, statemachine.ToolCallPayload{
		ToolCallID: call.ToolCallID,
		ToolName:   call.ToolName,
		Arguments:  append(json.RawMessage(nil), call.Arguments...),
		Error:      callErr.Error(),
	}, eventbus.WithTaskID(taskID), eventbus.WithSource(a.source))
}

func buildManager(config config) (*tool.Manage, error) {
	if config.manager != nil {
		if len(config.toolNames) == 0 {
			return config.manager, nil
		}
		return config.manager.Subset(config.toolNames)
	}
	if len(config.toolNames) == 0 {
		return tool.NewDefaultManage(config.managerOptions...)
	}
	manager := tool.NewManage(config.managerOptions...)
	for _, name := range config.toolNames {
		if err := tool.AddTool(name, manager); err != nil {
			return nil, err
		}
	}
	return manager, nil
}

type ResultPayload struct {
	Content  string          `json:"content"`
	Metadata map[string]any  `json:"metadata,omitempty"`
	Raw      json.RawMessage `json:"raw,omitempty"`
}

func encodeResult(result tool.Result) (json.RawMessage, error) {
	payload := ResultPayload{
		Content:  result.Content,
		Metadata: cloneAnyMap(result.Metadata),
		Raw:      append(json.RawMessage(nil), result.Raw...),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("tools adapter: encode tool result: %w", err)
	}
	return json.RawMessage(raw), nil
}

func cloneAnyMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneEnv(env *content.Env) *content.Env {
	if env == nil {
		return nil
	}
	cloned := env.Clone()
	return &cloned
}

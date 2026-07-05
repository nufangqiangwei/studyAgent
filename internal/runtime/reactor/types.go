package reactor

import (
	"agent/internal/runtime/eventbus"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type StateMachine interface {
	HandleEvent(ctx context.Context, event eventbus.Event) (StateResult, error)
}

type StateMachineFunc func(ctx context.Context, event eventbus.Event) (StateResult, error)

func (f StateMachineFunc) HandleEvent(ctx context.Context, event eventbus.Event) (StateResult, error) {
	if f == nil {
		return StateResult{}, fmt.Errorf("state machine is nil")
	}
	return f(ctx, event)
}

type StateResult struct {
	TaskID  string           `json:"task_id,omitempty"`
	Effects []Effect         `json:"effects,omitempty"`
	Events  []eventbus.Event `json:"events,omitempty"`
}

func (r StateResult) Clone() StateResult {
	cloned := r
	if len(r.Effects) > 0 {
		cloned.Effects = make([]Effect, 0, len(r.Effects))
		for _, effect := range r.Effects {
			cloned.Effects = append(cloned.Effects, effect.Clone())
		}
	}
	if len(r.Events) > 0 {
		cloned.Events = make([]eventbus.Event, 0, len(r.Events))
		for _, event := range r.Events {
			cloned.Events = append(cloned.Events, event.Clone())
		}
	}
	return cloned
}

type TaskRuntime struct {
	TaskID               string            `json:"task_id"`
	Agent                string            `json:"agent,omitempty"`
	StateMachine         StateMachine      `json:"-"`
	Executors            *ExecutorRegistry `json:"-"`
	EffectTimeout        time.Duration     `json:"-"`
	MaxConcurrentEffects int               `json:"max_concurrent_effects,omitempty"`
	Metadata             map[string]string `json:"metadata,omitempty"`
}

func (r TaskRuntime) Clone() TaskRuntime {
	cloned := r
	if len(r.Metadata) > 0 {
		cloned.Metadata = make(map[string]string, len(r.Metadata))
		for key, value := range r.Metadata {
			cloned.Metadata[key] = value
		}
	}
	return cloned
}

type RuntimeResolver interface {
	ResolveRuntime(ctx context.Context, event eventbus.Event) (TaskRuntime, error)
}

type RuntimeResolverFunc func(ctx context.Context, event eventbus.Event) (TaskRuntime, error)

func (f RuntimeResolverFunc) ResolveRuntime(ctx context.Context, event eventbus.Event) (TaskRuntime, error) {
	if f == nil {
		return TaskRuntime{}, fmt.Errorf("runtime resolver is nil")
	}
	return f(ctx, event)
}

type RuntimeRegistry struct {
	mu       sync.RWMutex
	runtimes map[string]TaskRuntime
}

func NewRuntimeRegistry(runtimes ...TaskRuntime) (*RuntimeRegistry, error) {
	registry := &RuntimeRegistry{
		runtimes: make(map[string]TaskRuntime, len(runtimes)),
	}
	for _, runtime := range runtimes {
		if err := registry.Register(runtime); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func (r *RuntimeRegistry) Register(runtime TaskRuntime) error {
	if r == nil {
		return fmt.Errorf("runtime registry is nil")
	}
	runtime.TaskID = strings.TrimSpace(runtime.TaskID)
	if runtime.TaskID == "" {
		return fmt.Errorf("task runtime id is required")
	}
	if runtime.StateMachine == nil {
		return fmt.Errorf("task runtime %q: state machine is required", runtime.TaskID)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.runtimes[runtime.TaskID]; exists {
		return fmt.Errorf("task runtime %q: already exists", runtime.TaskID)
	}
	r.runtimes[runtime.TaskID] = runtime.Clone()
	return nil
}

func (r *RuntimeRegistry) Unregister(taskID string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.runtimes[taskID]; !exists {
		return false
	}
	delete(r.runtimes, taskID)
	return true
}

func (r *RuntimeRegistry) ResolveRuntime(_ context.Context, event eventbus.Event) (TaskRuntime, error) {
	if r == nil {
		return TaskRuntime{}, fmt.Errorf("runtime registry is nil")
	}
	taskID := strings.TrimSpace(event.TaskID)
	if taskID == "" {
		return TaskRuntime{}, fmt.Errorf("event task_id is required")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	runtime, ok := r.runtimes[taskID]
	if !ok {
		return TaskRuntime{}, fmt.Errorf("task runtime %q: not found", taskID)
	}
	return runtime.Clone(), nil
}

func (r *RuntimeRegistry) List() []TaskRuntime {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	runtimes := make([]TaskRuntime, 0, len(r.runtimes))
	for _, runtime := range r.runtimes {
		runtimes = append(runtimes, runtime.Clone())
	}
	sort.Slice(runtimes, func(i, j int) bool {
		return runtimes[i].TaskID < runtimes[j].TaskID
	})
	return runtimes
}

type EffectExecutor interface {
	ExecuteEffect(ctx context.Context, runtime TaskRuntime, effect Effect) (EffectResult, error)
}

type EffectExecutorFunc func(ctx context.Context, runtime TaskRuntime, effect Effect) (EffectResult, error)

func (f EffectExecutorFunc) ExecuteEffect(ctx context.Context, runtime TaskRuntime, effect Effect) (EffectResult, error) {
	if f == nil {
		return EffectResult{}, fmt.Errorf("effect executor is nil")
	}
	return f(ctx, runtime, effect)
}

type EffectDispatcher interface {
	DispatchEffect(ctx context.Context, request EffectDispatchRequest) error
}

type EffectDispatchRequest struct {
	Context   context.Context
	Runtime   TaskRuntime
	Effect    Effect
	Executor  EffectExecutor
	Timeout   time.Duration
	Semaphore chan struct{}
	Reporter  EffectReporter
	OnDone    func()
}

type EffectReporter interface {
	EffectStarted(ctx context.Context, runtime TaskRuntime, effect Effect) error
	EffectSucceeded(ctx context.Context, runtime TaskRuntime, effect Effect) error
	EffectFailed(ctx context.Context, runtime TaskRuntime, effect Effect, stage ErrorStage, err error) error
	EffectResultEvents(ctx context.Context, runtime TaskRuntime, effect Effect, events []eventbus.Event) ([]eventbus.Event, error)
}

type EffectResult struct {
	Events   []eventbus.Event  `json:"events,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Data     json.RawMessage   `json:"data,omitempty"`
}

func (r EffectResult) Clone() EffectResult {
	cloned := r
	cloned.Data = append(json.RawMessage(nil), r.Data...)
	if len(r.Metadata) > 0 {
		cloned.Metadata = make(map[string]string, len(r.Metadata))
		for key, value := range r.Metadata {
			cloned.Metadata[key] = value
		}
	}
	if len(r.Events) > 0 {
		cloned.Events = make([]eventbus.Event, 0, len(r.Events))
		for _, event := range r.Events {
			cloned.Events = append(cloned.Events, event.Clone())
		}
	}
	return cloned
}

type ExecutorRegistry struct {
	mu        sync.RWMutex
	executors map[EffectType]EffectExecutor
}

func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{executors: make(map[EffectType]EffectExecutor)}
}

func (r *ExecutorRegistry) Register(effectType EffectType, executor EffectExecutor) error {
	if r == nil {
		return fmt.Errorf("executor registry is nil")
	}
	if strings.TrimSpace(string(effectType)) == "" {
		return fmt.Errorf("effect type is required")
	}
	if executor == nil {
		return fmt.Errorf("executor for effect %q is required", effectType)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.executors[effectType]; exists {
		return fmt.Errorf("executor for effect %q: already exists", effectType)
	}
	r.executors[effectType] = executor
	return nil
}

func (r *ExecutorRegistry) RegisterFunc(effectType EffectType, executor func(context.Context, TaskRuntime, Effect) (EffectResult, error)) error {
	return r.Register(effectType, EffectExecutorFunc(executor))
}

func (r *ExecutorRegistry) Lookup(effectType EffectType) (EffectExecutor, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	executor, ok := r.executors[effectType]
	return executor, ok
}

func (r *ExecutorRegistry) ListTypes() []EffectType {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]EffectType, 0, len(r.executors))
	for effectType := range r.executors {
		types = append(types, effectType)
	}
	sort.Slice(types, func(i, j int) bool {
		return types[i] < types[j]
	})
	return types
}

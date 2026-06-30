package event

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type StateMachine interface {
	HandleEvent(ctx context.Context, event Event) error
}

type StateMachineFunc func(ctx context.Context, event Event) error

func (f StateMachineFunc) HandleEvent(ctx context.Context, event Event) error {
	return f(ctx, event)
}

type Dispatcher struct {
	mu           sync.RWMutex
	registry     *Registry
	stateMachine StateMachine
	hooks        map[Type][]registeredHook
	nextHookID   int
}

type registeredHook struct {
	id   int
	spec HookSpec
}

type DispatchResult struct {
	Event          Event           `json:"event"`
	Delivered      bool            `json:"delivered"`
	Stopped        bool            `json:"stopped"`
	StoppedBy      string          `json:"stopped_by,omitempty"`
	StopReason     string          `json:"stop_reason,omitempty"`
	HookExecutions []HookExecution `json:"hook_executions,omitempty"`
}

func NewDispatcher(registry *Registry, stateMachine StateMachine) (*Dispatcher, error) {
	if registry == nil {
		return nil, fmt.Errorf("event dispatcher: registry is required")
	}
	return &Dispatcher{
		registry:     registry,
		stateMachine: stateMachine,
		hooks:        make(map[Type][]registeredHook),
	}, nil
}

func (d *Dispatcher) Registry() *Registry {
	if d == nil {
		return nil
	}
	return d.registry
}

func (d *Dispatcher) SetStateMachine(stateMachine StateMachine) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stateMachine = stateMachine
}

func (d *Dispatcher) RegisterHook(spec HookSpec) error {
	if d == nil {
		return fmt.Errorf("event dispatcher is nil")
	}
	if err := spec.validate(d.registry); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextHookID++
	hook := registeredHook{id: d.nextHookID, spec: spec}
	d.hooks[spec.EventType] = append(d.hooks[spec.EventType], hook)
	return nil
}

func (d *Dispatcher) NewEvent(eventType Type, payload any, options ...EventOption) (Event, error) {
	if d == nil {
		return Event{}, fmt.Errorf("event dispatcher is nil")
	}
	return d.registry.NewEvent(eventType, payload, options...)
}

func (d *Dispatcher) Emit(ctx context.Context, event Event) (DispatchResult, error) {
	if d == nil {
		return DispatchResult{}, fmt.Errorf("event dispatcher is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	definition, ok := d.registry.Lookup(event.Type)
	if !ok {
		return DispatchResult{}, fmt.Errorf("event definition %q: not registered", event.Type)
	}
	event = event.Clone()
	if event.ID == "" || event.OccurredAt.IsZero() {
		created, err := d.registry.NewEvent(event.Type, event.Payload, eventCompletionOptions(event)...)
		if err != nil {
			return DispatchResult{}, err
		}
		event = created
	}

	result := DispatchResult{Event: event.Clone()}
	var hookErr error
	for _, hook := range d.matchingHooks(event.Type) {
		if hook.spec.When != nil && !hook.spec.When(ctx, event.Clone()) {
			continue
		}
		hookResult, err := hook.spec.Handle(ctx, event.Clone())
		action := normalizeHookAction(hookResult.Action)
		execution := HookExecution{
			Name:      hook.spec.Name,
			EventType: hook.spec.EventType,
			Level:     hook.spec.Level,
			Action:    action,
			Reason:    hookResult.Reason,
		}
		if err != nil {
			hookErr = fmt.Errorf("event hook %q: %w", hook.spec.Name, err)
			result.HookExecutions = append(result.HookExecutions, execution)
			if definition.Interceptable() {
				return result, hookErr
			}
			break
		}
		if action == HookStop {
			if definition.Interceptable() {
				result.Stopped = true
				result.StoppedBy = hook.spec.Name
				result.StopReason = hookResult.Reason
				result.HookExecutions = append(result.HookExecutions, execution)
				return result, nil
			}
			execution.StopIgnored = true
		}
		result.HookExecutions = append(result.HookExecutions, execution)
	}

	stateMachine := d.currentStateMachine()
	if stateMachine == nil {
		if hookErr != nil {
			return result, hookErr
		}
		return result, fmt.Errorf("event dispatcher: state machine is required for event %q", event.Type)
	}
	if err := stateMachine.HandleEvent(ctx, event.Clone()); err != nil {
		if hookErr != nil {
			return result, fmt.Errorf("%v; state machine: %w", hookErr, err)
		}
		return result, fmt.Errorf("state machine handle event %q: %w", event.Type, err)
	}
	result.Delivered = true
	if hookErr != nil {
		return result, hookErr
	}
	return result, nil
}

func (d *Dispatcher) matchingHooks(eventType Type) []registeredHook {
	d.mu.RLock()
	defer d.mu.RUnlock()

	hooks := make([]registeredHook, 0, len(d.hooks[eventType])+len(d.hooks[AnyType]))
	hooks = append(hooks, d.hooks[AnyType]...)
	hooks = append(hooks, d.hooks[eventType]...)
	sort.SliceStable(hooks, func(i, j int) bool {
		if hooks[i].spec.Level == hooks[j].spec.Level {
			return hooks[i].id < hooks[j].id
		}
		return hooks[i].spec.Level < hooks[j].spec.Level
	})
	return hooks
}

func (d *Dispatcher) currentStateMachine() StateMachine {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.stateMachine
}

func normalizeHookAction(action HookAction) HookAction {
	switch action {
	case HookStop:
		return HookStop
	default:
		return HookContinue
	}
}

func eventCompletionOptions(event Event) []EventOption {
	options := make([]EventOption, 0, 7)
	if event.ID != "" {
		options = append(options, WithID(event.ID))
	}
	if !event.OccurredAt.IsZero() {
		options = append(options, WithTime(event.OccurredAt))
	}
	if event.Source != "" {
		options = append(options, WithSource(event.Source))
	}
	if event.RunID != "" {
		options = append(options, WithRunID(event.RunID))
	}
	if event.CorrelationID != "" {
		options = append(options, WithCorrelationID(event.CorrelationID))
	}
	if event.CausationID != "" {
		options = append(options, WithCausationID(event.CausationID))
	}
	if len(event.Metadata) > 0 {
		options = append(options, WithMetadata(event.Metadata))
	}
	return options
}

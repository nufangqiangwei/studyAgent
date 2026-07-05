package runtime

import (
	agents2 "agent/internal/runtime/agents"
	eventbus2 "agent/internal/runtime/eventbus"
	persistence2 "agent/internal/runtime/persistence"
	reactor2 "agent/internal/runtime/reactor"
	statemachine2 "agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	defaultSource = "new_runtime"

	TopicTaskResult = "task.result"
)

const (
	EventTaskCompleted eventbus2.EventType = "task.completed"
	EventTaskFailed    eventbus2.EventType = "task.failed"
	EventTaskCancelled eventbus2.EventType = "task.cancelled"
)

type Task struct {
	TaskID      string            `json:"task_id"`
	Agent       string            `json:"agent,omitempty"`
	Input       string            `json:"input,omitempty"`
	MaxFailures int               `json:"max_failures,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type Runtime struct {
	bus              *eventbus2.Bus
	ownsBus          bool
	reactor          *reactor2.Reactor
	runtimes         *reactor2.RuntimeRegistry
	executors        *reactor2.ExecutorRegistry
	stateMachine     *statemachine2.TaskStateMachine
	stateStore       statemachine2.StateStore
	snapshotStore    agents2.SnapshotStore
	agentFlows       *statemachine2.AgentFlowRegistry
	taskAgents       *taskAgentRegistry
	storage          persistence2.RuntimeStorage
	ownsStorage      bool
	source           string
	effectTimeout    time.Duration
	maxEffects       int
	subscriptionByID map[string]eventbus2.Subscription
	mu               sync.Mutex
	closed           bool
}

func New(options ...Option) (*Runtime, error) {
	cfg := runtimeConfig{
		source:         defaultSource,
		resultDelivery: eventbus2.DeliveryAsync,
		maxFailures:    3,
		maxEffects:     1,
	}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}

	source := strings.TrimSpace(cfg.source)
	if source == "" {
		source = defaultSource
	}

	bus := cfg.bus
	ownsBus := false
	if bus == nil {
		created, err := eventbus2.New(cfg.busOptions...)
		if err != nil {
			return nil, err
		}
		bus = created
		ownsBus = true
	}

	runtimeRegistry := cfg.runtimeRegistry
	if runtimeRegistry == nil {
		var err error
		runtimeRegistry, err = reactor2.NewRuntimeRegistry()
		if err != nil {
			if ownsBus {
				_ = bus.Close()
			}
			return nil, err
		}
	}

	taskMemory := statemachine2.NewMemoryStateStore()
	snapshotMemory := agents2.NewMemorySnapshotStore()
	stateStore := cfg.stateStore
	if stateStore == nil {
		stateStore = taskMemory
		if cfg.storage != nil && cfg.storage.TaskStates() != nil {
			stateStore = persistence2.NewMirroredTaskStateStore(taskMemory, cfg.storage.TaskStates())
		}
	}
	snapshotStore := cfg.snapshotStore
	if snapshotStore == nil {
		snapshotStore = snapshotMemory
		if cfg.storage != nil && cfg.storage.AgentSnapshots() != nil {
			snapshotStore = persistence2.NewMirroredSnapshotStore(snapshotMemory, cfg.storage.AgentSnapshots())
		}
	}

	flows := cfg.agentFlows
	if flows == nil {
		flows = statemachine2.NewAgentFlowRegistry()
	}
	for _, registration := range cfg.flowRegistrations {
		if err := flows.Register(registration.agent, registration.flow); err != nil {
			if ownsBus {
				_ = bus.Close()
			}
			return nil, err
		}
	}

	machineOptions := []statemachine2.TaskStateMachineOption{
		statemachine2.WithStateStore(stateStore),
		statemachine2.WithAgentFlows(flows),
		statemachine2.WithMaxFailures(cfg.maxFailures),
	}
	if cfg.clock != nil {
		machineOptions = append(machineOptions, statemachine2.WithClock(cfg.clock))
	}
	stateMachine, err := statemachine2.NewTaskStateMachine(machineOptions...)
	if err != nil {
		if ownsBus {
			_ = bus.Close()
		}
		return nil, err
	}

	executors := cfg.executors
	if executors == nil {
		executors = reactor2.NewExecutorRegistry()
	}
	for _, registration := range cfg.executorRegistrations {
		if err := executors.Register(registration.effectType, registration.executor); err != nil {
			if ownsBus {
				_ = bus.Close()
			}
			return nil, err
		}
	}
	taskAgents := newTaskAgentRegistry()
	agentExecutor := &taskAgentExecutor{registry: taskAgents}
	if err := registerDefaultExecutor(executors, reactor2.EffectAgentStart, agentExecutor); err != nil {
		if ownsBus {
			_ = bus.Close()
		}
		return nil, err
	}
	if err := registerDefaultExecutor(executors, reactor2.EffectAgentResume, agentExecutor); err != nil {
		if ownsBus {
			_ = bus.Close()
		}
		return nil, err
	}
	terminalExecutor := terminalEffectExecutor{source: source}
	for _, effectType := range []reactor2.EffectType{
		statemachine2.EffectEmitTaskCompleted,
		statemachine2.EffectEmitTaskFailed,
		statemachine2.EffectEmitTaskCancelled,
	} {
		if err := registerDefaultExecutor(executors, effectType, terminalExecutor); err != nil {
			if ownsBus {
				_ = bus.Close()
			}
			return nil, err
		}
	}

	dispatcher := cfg.dispatcher
	if dispatcher == nil {
		dispatcher = reactor2.NewAsyncEffectDispatcher()
	}
	resultDelivery := cfg.resultDelivery
	if resultDelivery != eventbus2.DeliverySync {
		resultDelivery = eventbus2.DeliveryAsync
	}

	reactorRuntime, err := reactor2.New(
		reactor2.WithRuntimeResolver(runtimeRegistry),
		reactor2.WithExecutorRegistry(executors),
		reactor2.WithEffectDispatcher(dispatcher),
		reactor2.WithEventBus(bus),
		reactor2.WithResultDelivery(resultDelivery),
		reactor2.WithEventTimeout(cfg.eventTimeout),
		reactor2.WithEffectTimeout(cfg.effectTimeout),
		reactor2.WithMaxConcurrentEvents(cfg.maxEvents),
		reactor2.WithMaxConcurrentEffects(cfg.maxEffects),
		reactor2.WithSource(source),
	)
	if err != nil {
		if ownsBus {
			_ = bus.Close()
		}
		return nil, err
	}

	runtime := &Runtime{
		bus:              bus,
		ownsBus:          ownsBus,
		reactor:          reactorRuntime,
		runtimes:         runtimeRegistry,
		executors:        executors,
		stateMachine:     stateMachine,
		stateStore:       stateStore,
		snapshotStore:    snapshotStore,
		agentFlows:       flows,
		taskAgents:       taskAgents,
		storage:          cfg.storage,
		ownsStorage:      cfg.ownsStorage,
		source:           source,
		effectTimeout:    cfg.effectTimeout,
		maxEffects:       cfg.maxEffects,
		subscriptionByID: make(map[string]eventbus2.Subscription),
	}
	if cfg.storage != nil && cfg.storage.Events() != nil {
		recorder, err := persistence2.NewEventRecorder(cfg.storage.Events())
		if err != nil {
			_ = runtime.Close()
			return nil, err
		}
		subscription, err := bus.SubscribeReadOnly(eventbus2.Filter{}, recorder, eventbus2.WithSubscriptionID(source+".events"))
		if err != nil {
			_ = runtime.Close()
			return nil, err
		}
		runtime.subscriptionByID[subscription.ID] = subscription
	}

	return runtime, nil
}

func (r *Runtime) RegisterAgent(ctx context.Context, taskID string, agent agents2.Agent, options ...RegisterAgentOption) error {
	if r == nil {
		return fmt.Errorf("runtime is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	cfg := registerAgentConfig{
		effectTimeout: r.effectTimeout,
		maxEffects:    r.maxEffects,
	}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("register agent: task_id is required")
	}
	if agent == nil {
		return fmt.Errorf("register agent %q: agent is required", taskID)
	}
	agentName := strings.TrimSpace(agent.Name())
	if agentName == "" {
		return fmt.Errorf("register agent %q: agent name is required", taskID)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return fmt.Errorf("runtime is closed")
	}
	if _, exists := r.subscriptionByID[taskSubscriptionID(taskID)]; exists {
		r.mu.Unlock()
		return fmt.Errorf("task runtime %q: already registered", taskID)
	}
	r.mu.Unlock()

	if err := r.taskAgents.Register(taskID, agent); err != nil {
		return err
	}
	runtime := reactor2.TaskRuntime{
		TaskID:               taskID,
		Agent:                agentName,
		StateMachine:         r.stateMachine,
		EffectTimeout:        cfg.effectTimeout,
		MaxConcurrentEffects: cfg.maxEffects,
		Metadata:             cloneStringMap(cfg.metadata),
	}
	if cfg.executors != nil {
		runtime.Executors = cfg.executors
	}
	if err := r.runtimes.Register(runtime); err != nil {
		r.taskAgents.Unregister(taskID, agentName)
		return err
	}
	if r.storage != nil && r.storage.Runtimes() != nil {
		if err := r.storage.Runtimes().Save(ctx, persistence2.NewRuntimeSnapshot(runtime, time.Now().UTC())); err != nil {
			r.runtimes.Unregister(taskID)
			r.taskAgents.Unregister(taskID, agentName)
			return err
		}
	}

	subscription, err := r.reactor.Attach(r.bus,
		eventbus2.Filter{Topic: statemachine2.TopicTask, TaskID: taskID},
		eventbus2.WithSubscriptionID(taskSubscriptionID(taskID)),
	)
	if err != nil {
		if r.storage != nil && r.storage.Runtimes() != nil {
			_ = r.storage.Runtimes().Delete(ctx, taskID)
		}
		r.runtimes.Unregister(taskID)
		r.taskAgents.Unregister(taskID, agentName)
		return err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.bus.Unsubscribe(subscription.ID)
		if r.storage != nil && r.storage.Runtimes() != nil {
			_ = r.storage.Runtimes().Delete(ctx, taskID)
		}
		r.runtimes.Unregister(taskID)
		r.taskAgents.Unregister(taskID, agentName)
		return fmt.Errorf("runtime is closed")
	}
	r.subscriptionByID[subscription.ID] = subscription
	r.mu.Unlock()
	return nil
}

func (r *Runtime) StartTask(ctx context.Context, task Task) (eventbus2.PublishResult, error) {
	if r == nil {
		return eventbus2.PublishResult{}, fmt.Errorf("runtime is nil")
	}
	taskID := strings.TrimSpace(task.TaskID)
	if taskID == "" {
		return eventbus2.PublishResult{}, fmt.Errorf("start task: task_id is required")
	}
	runtime, err := r.runtimes.ResolveRuntime(ctx, eventbus2.Event{TaskID: taskID})
	if err != nil {
		return eventbus2.PublishResult{}, err
	}
	if strings.TrimSpace(task.Agent) != "" && strings.TrimSpace(task.Agent) != runtime.Agent {
		return eventbus2.PublishResult{}, fmt.Errorf("start task %q: agent %q does not match registered agent %q", taskID, task.Agent, runtime.Agent)
	}
	event, err := eventbus2.NewEvent(statemachine2.TopicTask, statemachine2.EventTaskStartRequested, statemachine2.TaskStartPayload{
		Agent:       runtime.Agent,
		Input:       task.Input,
		MaxFailures: task.MaxFailures,
		Metadata:    cloneStringMap(task.Metadata),
	}, eventbus2.WithTaskID(taskID), eventbus2.WithSource(r.source))
	if err != nil {
		return eventbus2.PublishResult{}, err
	}
	return r.bus.Publish(ctx, event)
}

func (r *Runtime) Publish(ctx context.Context, event eventbus2.Event) (eventbus2.PublishResult, error) {
	if r == nil {
		return eventbus2.PublishResult{}, fmt.Errorf("runtime is nil")
	}
	return r.bus.Publish(ctx, event)
}

func (r *Runtime) PublishAsync(ctx context.Context, event eventbus2.Event) (eventbus2.PublishResult, error) {
	if r == nil {
		return eventbus2.PublishResult{}, fmt.Errorf("runtime is nil")
	}
	return r.bus.PublishAsync(ctx, event)
}

func (r *Runtime) State(ctx context.Context, taskID string) (statemachine2.TaskState, bool, error) {
	if r == nil {
		return statemachine2.TaskState{}, false, fmt.Errorf("runtime is nil")
	}
	return r.stateMachine.State(ctx, taskID)
}

func (r *Runtime) AgentSnapshot(ctx context.Context, agentName string, taskID string) (agents2.AgentSnapshot, bool, error) {
	if r == nil {
		return agents2.AgentSnapshot{}, false, fmt.Errorf("runtime is nil")
	}
	if r.snapshotStore != nil {
		snapshot, ok, err := r.snapshotStore.Load(ctx, agentName, taskID)
		if err != nil || ok {
			return snapshot, ok, err
		}
	}
	if agent, ok := r.taskAgents.Lookup(taskID, agentName); ok {
		return agent.Snapshot(ctx, taskID)
	}
	return agents2.AgentSnapshot{}, false, nil
}

func (r *Runtime) EventBus() *eventbus2.Bus {
	if r == nil {
		return nil
	}
	return r.bus
}

func (r *Runtime) Reactor() *reactor2.Reactor {
	if r == nil {
		return nil
	}
	return r.reactor
}

func (r *Runtime) RuntimeRegistry() *reactor2.RuntimeRegistry {
	if r == nil {
		return nil
	}
	return r.runtimes
}

func (r *Runtime) ExecutorRegistry() *reactor2.ExecutorRegistry {
	if r == nil {
		return nil
	}
	return r.executors
}

func (r *Runtime) StateStore() statemachine2.StateStore {
	if r == nil {
		return nil
	}
	return r.stateStore
}

func (r *Runtime) SnapshotStore() agents2.SnapshotStore {
	if r == nil {
		return nil
	}
	return r.snapshotStore
}

func (r *Runtime) AgentFlows() *statemachine2.AgentFlowRegistry {
	if r == nil {
		return nil
	}
	return r.agentFlows
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	subscriptions := make([]eventbus2.Subscription, 0, len(r.subscriptionByID))
	for _, subscription := range r.subscriptionByID {
		subscriptions = append(subscriptions, subscription)
	}
	r.subscriptionByID = nil
	r.mu.Unlock()

	for _, subscription := range subscriptions {
		r.bus.Unsubscribe(subscription.ID)
	}

	var closeErr error
	if r.ownsBus && r.bus != nil {
		closeErr = r.bus.Close()
	}
	if r.ownsStorage && r.storage != nil {
		if err := r.storage.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func registerDefaultExecutor(registry *reactor2.ExecutorRegistry, effectType reactor2.EffectType, executor reactor2.EffectExecutor) error {
	if _, ok := registry.Lookup(effectType); ok {
		return nil
	}
	return registry.Register(effectType, executor)
}

func taskSubscriptionID(taskID string) string {
	return defaultSource + ".task." + strings.TrimSpace(taskID)
}

type taskAgentExecutor struct {
	registry *taskAgentRegistry
}

func (e *taskAgentExecutor) ExecuteEffect(ctx context.Context, runtime reactor2.TaskRuntime, effect reactor2.Effect) (reactor2.EffectResult, error) {
	if e == nil || e.registry == nil {
		return reactor2.EffectResult{}, fmt.Errorf("task agent executor is nil")
	}
	taskID := strings.TrimSpace(runtime.TaskID)
	if taskID == "" {
		taskID = strings.TrimSpace(effect.TaskID)
	}
	if taskID == "" {
		return reactor2.EffectResult{}, fmt.Errorf("task agent executor: task_id is required")
	}
	agentName := strings.TrimSpace(runtime.Agent)
	if agentName == "" {
		agentName = strings.TrimSpace(effect.Metadata["agent"])
	}
	if agentName == "" {
		agentName = agentNameFromStartPayload(effect.Payload)
	}
	if agentName == "" {
		return reactor2.EffectResult{}, fmt.Errorf("task agent executor %q: agent name is required", taskID)
	}
	agent, ok := e.registry.Lookup(taskID, agentName)
	if !ok {
		return reactor2.EffectResult{}, fmt.Errorf("task agent executor: agent %q for task %q not found", agentName, taskID)
	}

	switch effect.Type {
	case reactor2.EffectAgentStart:
		result, err := agent.Start(ctx, startInputFromEffect(effect))
		if err != nil {
			return reactor2.EffectResult{}, err
		}
		return reactor2.EffectResult{Events: result.Events}, nil
	case reactor2.EffectAgentResume:
		result, err := agent.Resume(ctx, agents2.AgentResumeInput{
			TaskID:   taskID,
			Payload:  append(json.RawMessage(nil), effect.Payload...),
			Metadata: cloneStringMap(effect.Metadata),
		})
		if err != nil {
			return reactor2.EffectResult{}, err
		}
		return reactor2.EffectResult{Events: result.Events}, nil
	default:
		return reactor2.EffectResult{}, fmt.Errorf("task agent executor: unsupported effect type %q", effect.Type)
	}
}

type terminalEffectExecutor struct {
	source string
}

func (e terminalEffectExecutor) ExecuteEffect(_ context.Context, runtime reactor2.TaskRuntime, effect reactor2.Effect) (reactor2.EffectResult, error) {
	var eventType eventbus2.EventType
	switch effect.Type {
	case statemachine2.EffectEmitTaskCompleted:
		eventType = EventTaskCompleted
	case statemachine2.EffectEmitTaskFailed:
		eventType = EventTaskFailed
	case statemachine2.EffectEmitTaskCancelled:
		eventType = EventTaskCancelled
	default:
		return reactor2.EffectResult{}, fmt.Errorf("terminal effect executor: unsupported effect type %q", effect.Type)
	}
	taskID := strings.TrimSpace(effect.TaskID)
	if taskID == "" {
		taskID = runtime.TaskID
	}
	source := strings.TrimSpace(e.source)
	if source == "" {
		source = defaultSource
	}
	event, err := eventbus2.NewEvent(TopicTaskResult, eventType, append(json.RawMessage(nil), effect.Payload...),
		eventbus2.WithTaskID(taskID),
		eventbus2.WithSource(source),
	)
	if err != nil {
		return reactor2.EffectResult{}, err
	}
	return reactor2.EffectResult{Events: []eventbus2.Event{event}}, nil
}

func startInputFromEffect(effect reactor2.Effect) agents2.AgentStartInput {
	var payload statemachine2.TaskStartPayload
	_ = json.Unmarshal(effect.Payload, &payload)
	metadata := cloneStringMap(payload.Metadata)
	if metadata == nil {
		metadata = cloneStringMap(effect.Metadata)
	}
	return agents2.AgentStartInput{
		TaskID:   strings.TrimSpace(effect.TaskID),
		Input:    payload.Input,
		Metadata: metadata,
	}
}

func agentNameFromStartPayload(raw json.RawMessage) string {
	var payload statemachine2.TaskStartPayload
	if len(raw) == 0 || json.Unmarshal(raw, &payload) != nil {
		return ""
	}
	return strings.TrimSpace(payload.Agent)
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

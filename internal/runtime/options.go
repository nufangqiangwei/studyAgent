package runtime

import (
	"agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/persistence"
	reactor2 "agent/internal/runtime/reactor"
	statemachine2 "agent/internal/runtime/statemachine"
	"context"
	"time"
)

type Option func(*runtimeConfig)

type runtimeConfig struct {
	bus                   *eventbus.Bus
	busOptions            []eventbus.BusOption
	runtimeRegistry       *reactor2.RuntimeRegistry
	executors             *reactor2.ExecutorRegistry
	executorRegistrations []executorRegistration
	dispatcher            reactor2.EffectDispatcher
	resultDelivery        eventbus.DeliveryMode
	eventTimeout          time.Duration
	effectTimeout         time.Duration
	maxEvents             int
	maxEffects            int
	stateStore            statemachine2.StateStore
	snapshotStore         agents.SnapshotStore
	agentFlows            *statemachine2.AgentFlowRegistry
	flowRegistrations     []flowRegistration
	storage               persistence.RuntimeStorage
	ownsStorage           bool
	source                string
	clock                 statemachine2.Clock
	maxFailures           int
}

type executorRegistration struct {
	effectType reactor2.EffectType
	executor   reactor2.EffectExecutor
}

type flowRegistration struct {
	agent string
	flow  statemachine2.AgentFlowMachine
}

func WithEventBus(bus *eventbus.Bus) Option {
	return func(config *runtimeConfig) {
		config.bus = bus
	}
}

func WithEventBusOptions(options ...eventbus.BusOption) Option {
	return func(config *runtimeConfig) {
		config.busOptions = append(config.busOptions, options...)
	}
}

func WithRuntimeRegistry(registry *reactor2.RuntimeRegistry) Option {
	return func(config *runtimeConfig) {
		config.runtimeRegistry = registry
	}
}

func WithExecutorRegistry(registry *reactor2.ExecutorRegistry) Option {
	return func(config *runtimeConfig) {
		config.executors = registry
	}
}

func WithEffectExecutor(effectType reactor2.EffectType, executor reactor2.EffectExecutor) Option {
	return func(config *runtimeConfig) {
		config.executorRegistrations = append(config.executorRegistrations, executorRegistration{
			effectType: effectType,
			executor:   executor,
		})
	}
}

func WithEffectExecutorFunc(effectType reactor2.EffectType, executor func(context.Context, reactor2.TaskRuntime, reactor2.Effect) (reactor2.EffectResult, error)) Option {
	return WithEffectExecutor(effectType, reactor2.EffectExecutorFunc(executor))
}

func WithEffectDispatcher(dispatcher reactor2.EffectDispatcher) Option {
	return func(config *runtimeConfig) {
		config.dispatcher = dispatcher
	}
}

func WithSyncEffects() Option {
	return WithEffectDispatcher(NewSyncEffectDispatcher())
}

func WithResultDelivery(mode eventbus.DeliveryMode) Option {
	return func(config *runtimeConfig) {
		config.resultDelivery = mode
	}
}

func WithEventTimeout(timeout time.Duration) Option {
	return func(config *runtimeConfig) {
		config.eventTimeout = timeout
	}
}

func WithEffectTimeout(timeout time.Duration) Option {
	return func(config *runtimeConfig) {
		config.effectTimeout = timeout
	}
}

func WithMaxConcurrentEvents(limit int) Option {
	return func(config *runtimeConfig) {
		config.maxEvents = limit
	}
}

func WithMaxConcurrentEffects(limit int) Option {
	return func(config *runtimeConfig) {
		config.maxEffects = limit
	}
}

func WithStateStore(store statemachine2.StateStore) Option {
	return func(config *runtimeConfig) {
		config.stateStore = store
	}
}

func WithSnapshotStore(store agents.SnapshotStore) Option {
	return func(config *runtimeConfig) {
		config.snapshotStore = store
	}
}

func WithAgentFlows(flows *statemachine2.AgentFlowRegistry) Option {
	return func(config *runtimeConfig) {
		config.agentFlows = flows
	}
}

func WithAgentFlow(agent string, flow statemachine2.AgentFlowMachine) Option {
	return func(config *runtimeConfig) {
		config.flowRegistrations = append(config.flowRegistrations, flowRegistration{
			agent: agent,
			flow:  flow,
		})
	}
}

func WithStorage(storage persistence.RuntimeStorage) Option {
	return func(config *runtimeConfig) {
		config.storage = storage
	}
}

func WithOwnedStorage(storage persistence.RuntimeStorage) Option {
	return func(config *runtimeConfig) {
		config.storage = storage
		config.ownsStorage = storage != nil
	}
}

func WithSource(source string) Option {
	return func(config *runtimeConfig) {
		config.source = source
	}
}

func WithClock(clock statemachine2.Clock) Option {
	return func(config *runtimeConfig) {
		config.clock = clock
	}
}

func WithMaxFailures(maxFailures int) Option {
	return func(config *runtimeConfig) {
		config.maxFailures = maxFailures
	}
}

type RegisterAgentOption func(*registerAgentConfig)

type registerAgentConfig struct {
	executors     *reactor2.ExecutorRegistry
	effectTimeout time.Duration
	maxEffects    int
	metadata      map[string]string
}

func WithTaskExecutors(executors *reactor2.ExecutorRegistry) RegisterAgentOption {
	return func(config *registerAgentConfig) {
		config.executors = executors
	}
}

func WithTaskEffectTimeout(timeout time.Duration) RegisterAgentOption {
	return func(config *registerAgentConfig) {
		config.effectTimeout = timeout
	}
}

func WithTaskMaxConcurrentEffects(limit int) RegisterAgentOption {
	return func(config *registerAgentConfig) {
		config.maxEffects = limit
	}
}

func WithTaskMetadata(metadata map[string]string) RegisterAgentOption {
	return func(config *registerAgentConfig) {
		config.metadata = cloneStringMap(metadata)
	}
}

package runner

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimeevent "agent/internal/event"
	"agent/internal/llm"
	"agent/internal/state"
)

const defaultLeaseDuration = 5 * time.Minute

type Options struct {
	Dispatcher       *runtimeevent.Dispatcher
	Machine          *state.Machine
	EventInbox       state.EventInboxStore
	EffectStore      state.EffectStore
	EffectDispatcher EffectDispatcher
	EffectWorker     EffectWorker
	LLM              *llm.Runtime
	ToolRegistry     ToolRegistry
	StateStore       state.StateStore
	EventStore       state.EventStore
	Reducers         *state.ReducerRegistry
	MaxSteps         int
	Source           string
	WorkerOwner      string
	LeaseDuration    time.Duration
}

type AgentRunner struct {
	dispatcher       *runtimeevent.Dispatcher
	machine          *state.Machine
	eventInbox       state.EventInboxStore
	effectStore      state.EffectStore
	effectDispatcher EffectDispatcher
	effectWorker     EffectWorker
	llm              *llm.Runtime
	toolRegistry     ToolRegistry
	states           state.StateStore
	events           state.EventStore
	maxSteps         int
	source           string
	workerOwner      string
	leaseDuration    time.Duration
}

func NewAgentRunner(opts Options) (*AgentRunner, error) {
	states := opts.StateStore
	if states == nil {
		states = state.NewMemoryStateStore()
	}
	events := opts.EventStore
	if events == nil {
		events = state.NewMemoryEventStore()
	}
	eventInbox := opts.EventInbox
	if eventInbox == nil {
		eventInbox = state.NewMemoryEventInbox()
	}
	effects := opts.EffectStore
	if effects == nil {
		effects = state.NewMemoryEffectStore()
	}
	maxSteps := opts.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 20
	}

	reducers := opts.Reducers
	if reducers == nil {
		reducers = state.NewReducerRegistry()
		reducers.Register(state.CoreRunReducer{})
		reducers.Register(ReActReducer{})
	}

	machine := opts.Machine
	if machine == nil {
		machine = state.NewMachine(states, events, effects, reducers)
	}

	effectDispatcher := opts.EffectDispatcher
	if effectDispatcher == nil {
		created, err := NewStoreEffectDispatcher(effects)
		if err != nil {
			return nil, err
		}
		effectDispatcher = created
	}

	effectWorker := opts.EffectWorker
	if effectWorker == nil && opts.LLM != nil {
		created, err := NewRuntimeEffectWorker(EffectWorkerOptions{
			StateStore: states,
			LLM:        opts.LLM,
			Tools:      opts.ToolRegistry,
		})
		if err != nil {
			return nil, err
		}
		effectWorker = created
	}

	source := opts.Source
	if source == "" {
		source = "agent.runner"
	}
	workerOwner := strings.TrimSpace(opts.WorkerOwner)
	if workerOwner == "" {
		workerOwner = state.NewID("worker")
	}
	leaseDuration := opts.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = defaultLeaseDuration
	}

	runner := &AgentRunner{
		machine:          machine,
		eventInbox:       eventInbox,
		effectStore:      effects,
		effectDispatcher: effectDispatcher,
		effectWorker:     effectWorker,
		llm:              opts.LLM,
		toolRegistry:     opts.ToolRegistry,
		states:           states,
		events:           events,
		maxSteps:         maxSteps,
		source:           source,
		workerOwner:      workerOwner,
		leaseDuration:    leaseDuration,
	}

	adapter := runnerStateMachine{runner: runner}
	dispatcher := opts.Dispatcher
	if dispatcher == nil {
		created, err := runtimeevent.NewDispatcher(runtimeevent.DefaultRegistry(), adapter)
		if err != nil {
			return nil, err
		}
		dispatcher = created
	} else {
		dispatcher.SetStateMachine(adapter)
	}
	runner.dispatcher = dispatcher

	return runner, nil
}

func (r *AgentRunner) Run(ctx context.Context, task Task) (RunResult, error) {
	if r == nil {
		return RunResult{}, fmt.Errorf("agent runner is nil")
	}
	if r.effectStore == nil {
		return RunResult{}, fmt.Errorf("agent runner effect store is required")
	}
	if r.effectWorker == nil {
		return RunResult{}, fmt.Errorf("agent runner effect worker is required")
	}

	runID, err := r.Start(ctx, task)
	if err != nil {
		return RunResult{}, err
	}

	for {
		processed, err := r.ProcessNextEvent(ctx, runID)
		if err != nil {
			result, resultErr := r.Result(ctx, runID)
			if resultErr != nil {
				return RunResult{}, err
			}
			return result, err
		}
		if processed {
			continue
		}

		result, err := r.Result(ctx, runID)
		if err != nil {
			return result, err
		}
		if result.State.IsTerminal() {
			if result.Status == state.PhaseFailed {
				return result, resultError(result.Error)
			}
			return result, nil
		}

		stored, ok, err := r.effectStore.Claim(ctx, string(runID), r.workerOwner, r.leaseDuration)
		if err != nil {
			return result, err
		}
		if !ok {
			return result, fmt.Errorf("runner: run %s is suspended with no claimable effect", runID)
		}

		nextEvents, err := r.effectWorker.Execute(ctx, stored.Effect)
		if err != nil {
			_ = r.effectStore.MarkFailed(ctx, stored.Effect.ID, r.workerOwner, err)
			return result, err
		}
		if _, err := r.effectStore.RenewLease(ctx, stored.Effect.ID, r.workerOwner, r.leaseDuration); err != nil {
			return result, err
		}
		for _, nextEvent := range nextEvents {
			if nextEvent.RunID == "" {
				nextEvent.RunID = string(runID)
			}
			if err := r.HandleEvent(ctx, nextEvent); err != nil {
				_ = r.effectStore.MarkFailed(ctx, stored.Effect.ID, r.workerOwner, err)
				return result, err
			}
		}
		if err := r.effectStore.MarkCompleted(ctx, stored.Effect.ID, r.workerOwner); err != nil {
			return result, err
		}
	}
}

func (r *AgentRunner) Start(ctx context.Context, task Task) (RunID, error) {
	if r == nil {
		return "", fmt.Errorf("agent runner is nil")
	}
	if r.states == nil {
		return "", fmt.Errorf("agent runner state store is required")
	}
	if r.dispatcher == nil {
		return "", fmt.Errorf("agent runner dispatcher is required")
	}

	runState, err := newInitialState(task, r.maxSteps)
	if err != nil {
		return "", err
	}
	if err := r.states.Save(ctx, runState); err != nil {
		return "", err
	}

	started, err := r.dispatcher.NewEvent(runtimeevent.EventRunStarted, map[string]string{"task": task.Input},
		runtimeevent.WithRunID(runState.RunID),
		runtimeevent.WithSource(r.source),
	)
	if err != nil {
		return "", err
	}
	if err := r.HandleEvent(ctx, started); err != nil {
		return "", err
	}
	return RunID(runState.RunID), nil
}

func (r *AgentRunner) HandleEvent(ctx context.Context, event runtimeevent.Event) error {
	if r == nil {
		return fmt.Errorf("agent runner is nil")
	}
	if r.eventInbox == nil {
		return fmt.Errorf("agent runner event inbox is required")
	}
	completed, err := completeEvent(event)
	if err != nil {
		return err
	}
	_, _, err = r.eventInbox.Append(ctx, completed)
	return err
}

func (r *AgentRunner) ProcessNextEvent(ctx context.Context, runID RunID) (bool, error) {
	if r == nil {
		return false, fmt.Errorf("agent runner is nil")
	}
	if r.eventInbox == nil {
		return false, fmt.Errorf("agent runner event inbox is required")
	}
	if r.dispatcher == nil {
		return false, fmt.Errorf("agent runner dispatcher is required")
	}
	stored, ok, err := r.eventInbox.Claim(ctx, string(runID), r.workerOwner, r.leaseDuration)
	if err != nil || !ok {
		return ok, err
	}
	if _, err := r.eventInbox.RenewLease(ctx, stored.Event.ID, r.workerOwner, r.leaseDuration); err != nil {
		return true, err
	}
	if _, err := r.dispatcher.Emit(ctx, stored.Event); err != nil {
		_ = r.eventInbox.MarkFailed(ctx, stored.Event.ID, r.workerOwner, err)
		return true, err
	}
	if err := r.eventInbox.MarkProcessed(ctx, stored.Event.ID, r.workerOwner); err != nil {
		return true, err
	}
	return true, nil
}

func (r *AgentRunner) ProcessPendingEvents(ctx context.Context, runID RunID) (int, error) {
	processed := 0
	for {
		ok, err := r.ProcessNextEvent(ctx, runID)
		if err != nil || !ok {
			return processed, err
		}
		processed++
	}
}

func (r *AgentRunner) Result(ctx context.Context, runID RunID) (RunResult, error) {
	if r == nil {
		return RunResult{}, fmt.Errorf("agent runner is nil")
	}
	if r.states == nil {
		return RunResult{}, fmt.Errorf("agent runner state store is required")
	}
	if r.events == nil {
		return RunResult{}, fmt.Errorf("agent runner event store is required")
	}
	stateID := string(runID)
	runState, err := r.states.Load(ctx, stateID)
	if err != nil {
		return RunResult{}, err
	}
	storedEvents, err := r.events.List(ctx, stateID)
	if err != nil {
		return RunResult{}, err
	}
	data, err := loadRunData(runState)
	if err != nil {
		return RunResult{}, err
	}
	return RunResult{
		RunID:       stateID,
		Status:      runState.Phase,
		FinalAnswer: data.FinalAnswer,
		StepsUsed:   runState.Step,
		State:       runState,
		Events:      storedEvents,
		Error:       runState.Error,
	}, nil
}

func (r *AgentRunner) dispatchEffects(ctx context.Context, effects []state.Effect) error {
	if r == nil || r.effectDispatcher == nil {
		return nil
	}
	for _, effect := range effects {
		if err := r.effectDispatcher.Dispatch(ctx, effect); err != nil {
			return err
		}
	}
	return nil
}

type runnerStateMachine struct {
	runner *AgentRunner
}

func (m runnerStateMachine) HandleEvent(ctx context.Context, event runtimeevent.Event) error {
	if m.runner == nil {
		return fmt.Errorf("agent runner state machine is nil")
	}
	if m.runner.machine == nil {
		return fmt.Errorf("agent runner machine is required")
	}
	result, err := m.runner.machine.Advance(ctx, event)
	if err != nil {
		return err
	}
	return m.runner.dispatchEffects(ctx, result.Effects)
}

func completeEvent(event runtimeevent.Event) (runtimeevent.Event, error) {
	if event.ID != "" && !event.OccurredAt.IsZero() {
		return event.Clone(), nil
	}
	options := make([]runtimeevent.EventOption, 0, 7)
	if event.ID != "" {
		options = append(options, runtimeevent.WithID(event.ID))
	}
	if !event.OccurredAt.IsZero() {
		options = append(options, runtimeevent.WithTime(event.OccurredAt))
	}
	if event.Source != "" {
		options = append(options, runtimeevent.WithSource(event.Source))
	}
	if event.RunID != "" {
		options = append(options, runtimeevent.WithRunID(event.RunID))
	}
	if event.CorrelationID != "" {
		options = append(options, runtimeevent.WithCorrelationID(event.CorrelationID))
	}
	if event.CausationID != "" {
		options = append(options, runtimeevent.WithCausationID(event.CausationID))
	}
	if len(event.Metadata) > 0 {
		options = append(options, runtimeevent.WithMetadata(event.Metadata))
	}
	return runtimeevent.New(event.Type, event.Payload, options...)
}

func resultError(errorState *state.ErrorState) error {
	if errorState == nil {
		return fmt.Errorf("run failed")
	}
	if errorState.Code == "" {
		return fmt.Errorf("%s", errorState.Message)
	}
	if errorState.Message == "" {
		return fmt.Errorf("%s", errorState.Code)
	}
	return fmt.Errorf("%s: %s", errorState.Code, errorState.Message)
}

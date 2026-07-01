package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeevent "agent/internal/event"
	"agent/internal/llm"
	"agent/internal/state"
)

const defaultLeaseDuration = 5 * time.Minute

type Options struct {
	Dispatcher             *runtimeevent.Dispatcher
	Machine                *state.Machine
	EventInbox             state.EventInboxStore
	EffectStore            state.EffectStore
	EffectDispatcher       EffectDispatcher
	EffectWorker           EffectWorker
	LLM                    *llm.Runtime
	ToolRegistry           ToolRegistry
	StateStore             state.StateStore
	EventStore             state.EventStore
	Reducers               *state.ReducerRegistry
	MaxSteps               int
	Source                 string
	WorkerOwner            string
	LeaseDuration          time.Duration
	SuspendUserInteraction bool
	UserInteraction        UserInteraction
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
	userInteraction  UserInteraction
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
			StateStore:             states,
			LLM:                    opts.LLM,
			Tools:                  opts.ToolRegistry,
			SuspendUserInteraction: opts.SuspendUserInteraction,
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
		userInteraction:  opts.UserInteraction,
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

	runID, err := r.Start(ctx, task)
	if err != nil {
		return RunResult{}, err
	}

	for {
		advanced, err := r.Advance(ctx, runID)
		if err != nil {
			result, resultErr := r.Result(ctx, runID)
			if resultErr != nil {
				return RunResult{}, err
			}
			return result, err
		}
		switch advanced.Status {
		case AdvanceStatusEventProcessed:
			continue
		case AdvanceStatusTerminal:
			result, err := r.Result(ctx, runID)
			if err != nil {
				return result, err
			}
			if result.Status == state.PhaseFailed {
				return result, resultError(result.Error)
			}
			return result, nil
		}

		dispatched, err := r.DispatchNextEffect(ctx, runID)
		if err != nil {
			result, resultErr := r.Result(ctx, runID)
			if resultErr != nil {
				return RunResult{}, err
			}
			return result, err
		}
		switch dispatched.Status {
		case AdvanceStatusEffectDispatched:
			continue
		case AdvanceStatusTerminal:
			result, err := r.Result(ctx, runID)
			if err != nil {
				return result, err
			}
			if result.Status == state.PhaseFailed {
				return result, resultError(result.Error)
			}
			return result, nil
		default:
			handled, handleErr := r.resolveUserInteraction(ctx, runID)
			if handleErr != nil {
				result, resultErr := r.Result(ctx, runID)
				if resultErr != nil {
					return RunResult{}, handleErr
				}
				return result, handleErr
			}
			if handled {
				continue
			}
			result, err := r.Result(ctx, runID)
			if err != nil {
				return result, err
			}
			return result, fmt.Errorf("runner: run %s is suspended with no claimable effect", runID)
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

func (r *AgentRunner) Submit(ctx context.Context, task Task) (RunID, error) {
	return r.Start(ctx, task)
}

func (r *AgentRunner) Recover(ctx context.Context) (RecoverResult, error) {
	if r == nil {
		return RecoverResult{}, fmt.Errorf("agent runner is nil")
	}
	if r.states == nil {
		return RecoverResult{}, fmt.Errorf("agent runner state store is required")
	}

	runStates, err := r.states.List(ctx)
	if err != nil {
		return RecoverResult{}, err
	}

	recovered := make([]RecoverableRun, 0, len(runStates))
	for _, runState := range runStates {
		if runState.RunID == "" || runState.IsTerminal() {
			continue
		}
		if err := r.enqueueRunResumed(ctx, runState); err != nil {
			return RecoverResult{}, err
		}

		pendingEvents, err := r.pendingEventCount(ctx, runState.RunID)
		if err != nil {
			return RecoverResult{}, err
		}
		pendingEffects, err := r.pendingEffectCount(ctx, runState.RunID)
		if err != nil {
			return RecoverResult{}, err
		}
		recovered = append(recovered, RecoverableRun{
			RunID:          runState.RunID,
			State:          runState,
			PendingEvents:  pendingEvents,
			PendingEffects: pendingEffects,
		})
	}

	return RecoverResult{Runs: recovered}, nil
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

func (r *AgentRunner) Advance(ctx context.Context, runID RunID) (LoopAdvanceResult, error) {
	stored, processed, err := r.processNextEvent(ctx, runID)
	if err != nil {
		return LoopAdvanceResult{}, err
	}

	result, err := r.Result(ctx, runID)
	if err != nil {
		return LoopAdvanceResult{}, err
	}
	if processed {
		event := stored.Event.Clone()
		return LoopAdvanceResult{
			RunID:  string(runID),
			Status: AdvanceStatusEventProcessed,
			State:  result.State,
			Event:  &event,
		}, nil
	}
	if result.State.IsTerminal() {
		return LoopAdvanceResult{
			RunID:  string(runID),
			Status: AdvanceStatusTerminal,
			State:  result.State,
		}, nil
	}

	return LoopAdvanceResult{
		RunID:  string(runID),
		Status: r.waitingStatus(ctx, runID),
		State:  result.State,
	}, nil
}

func (r *AgentRunner) ProcessNextEvent(ctx context.Context, runID RunID) (bool, error) {
	_, processed, err := r.processNextEvent(ctx, runID)
	return processed, err
}

func (r *AgentRunner) processNextEvent(ctx context.Context, runID RunID) (state.StoredEvent, bool, error) {
	if r == nil {
		return state.StoredEvent{}, false, fmt.Errorf("agent runner is nil")
	}
	if r.eventInbox == nil {
		return state.StoredEvent{}, false, fmt.Errorf("agent runner event inbox is required")
	}
	if r.dispatcher == nil {
		return state.StoredEvent{}, false, fmt.Errorf("agent runner dispatcher is required")
	}
	stored, ok, err := r.eventInbox.Claim(ctx, string(runID), r.workerOwner, r.leaseDuration)
	if err != nil || !ok {
		return stored, ok, err
	}
	if _, err := r.eventInbox.RenewLease(ctx, stored.Event.ID, r.workerOwner, r.leaseDuration); err != nil {
		return stored, true, err
	}
	if _, err := r.dispatcher.Emit(ctx, stored.Event); err != nil {
		_ = r.eventInbox.MarkFailed(ctx, stored.Event.ID, r.workerOwner, err)
		return stored, true, err
	}
	if err := r.eventInbox.MarkProcessed(ctx, stored.Event.ID, r.workerOwner); err != nil {
		return stored, true, err
	}
	return stored, true, nil
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

func (r *AgentRunner) DispatchNextEffect(ctx context.Context, runID RunID) (LoopAdvanceResult, error) {
	if r == nil {
		return LoopAdvanceResult{}, fmt.Errorf("agent runner is nil")
	}
	if r.effectStore == nil {
		return LoopAdvanceResult{}, fmt.Errorf("agent runner effect store is required")
	}
	if r.effectWorker == nil {
		return LoopAdvanceResult{}, fmt.Errorf("agent runner effect worker is required")
	}

	result, err := r.Result(ctx, runID)
	if err != nil {
		return LoopAdvanceResult{}, err
	}
	if result.State.IsTerminal() {
		return LoopAdvanceResult{
			RunID:  string(runID),
			Status: AdvanceStatusTerminal,
			State:  result.State,
		}, nil
	}

	stored, ok, err := r.effectStore.Claim(ctx, string(runID), r.workerOwner, r.leaseDuration)
	if err != nil {
		return LoopAdvanceResult{}, err
	}
	if !ok {
		return LoopAdvanceResult{
			RunID:  string(runID),
			Status: r.waitingStatus(ctx, runID),
			State:  result.State,
		}, nil
	}

	nextEvents, err := r.effectWorker.Execute(ctx, stored.Effect)
	if err != nil {
		_ = r.effectStore.MarkFailed(ctx, stored.Effect.ID, r.workerOwner, err)
		return LoopAdvanceResult{}, err
	}
	if _, err := r.effectStore.RenewLease(ctx, stored.Effect.ID, r.workerOwner, r.leaseDuration); err != nil {
		return LoopAdvanceResult{}, err
	}
	for _, nextEvent := range nextEvents {
		if nextEvent.RunID == "" {
			nextEvent.RunID = string(runID)
		}
		if err := r.HandleEvent(ctx, nextEvent); err != nil {
			_ = r.effectStore.MarkFailed(ctx, stored.Effect.ID, r.workerOwner, err)
			return LoopAdvanceResult{}, err
		}
	}
	if err := r.effectStore.MarkCompleted(ctx, stored.Effect.ID, r.workerOwner); err != nil {
		return LoopAdvanceResult{}, err
	}

	latest, err := r.Result(ctx, runID)
	if err != nil {
		return LoopAdvanceResult{}, err
	}
	effect := stored.Effect.Clone()
	return LoopAdvanceResult{
		RunID:  string(runID),
		Status: AdvanceStatusEffectDispatched,
		State:  latest.State,
		Effect: &effect,
		Events: cloneEvents(nextEvents),
	}, nil
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

func (r *AgentRunner) waitingStatus(ctx context.Context, runID RunID) AdvanceStatus {
	if r == nil || r.effectStore == nil {
		return AdvanceStatusSuspended
	}
	effects, err := r.effectStore.ListPending(ctx, string(runID))
	if err == nil && len(effects) > 0 {
		return AdvanceStatusWaitingForEffect
	}
	return AdvanceStatusSuspended
}

func cloneEvents(events []runtimeevent.Event) []runtimeevent.Event {
	if len(events) == 0 {
		return nil
	}
	cloned := make([]runtimeevent.Event, 0, len(events))
	for _, event := range events {
		cloned = append(cloned, event.Clone())
	}
	return cloned
}

func (r *AgentRunner) resolveUserInteraction(ctx context.Context, runID RunID) (bool, error) {
	if r == nil || r.userInteraction == nil {
		return false, nil
	}
	result, err := r.Result(ctx, runID)
	if err != nil {
		return false, err
	}
	if result.State.Waiting == nil {
		return false, nil
	}
	switch result.State.Waiting.Reason {
	case "user_input":
		return r.resolveUserInput(ctx, runID, result.State.Waiting.Target)
	case "user_approval":
		return r.resolveUserApproval(ctx, runID, result.State.Waiting.Target)
	default:
		return false, nil
	}
}

func (r *AgentRunner) resolveUserInput(ctx context.Context, runID RunID, target string) (bool, error) {
	requestEvent, ok, err := r.latestRunEvent(ctx, runID, runtimeevent.EventUserInputRequested, target)
	if err != nil || !ok {
		return false, err
	}
	var request UserInputRequestedPayload
	if err := json.Unmarshal(requestEvent.Payload, &request); err != nil {
		return false, fmt.Errorf("runner: decode user input request: %w", err)
	}
	received, err := r.userInteraction.ReceiveInput(ctx, request)
	if err != nil {
		return false, err
	}
	if received.ToolCallID == "" {
		received.ToolCallID = request.ToolCallID
	}
	if received.ToolName == "" {
		received.ToolName = request.ToolName
	}
	event, err := r.dispatcher.NewEvent(runtimeevent.EventUserInputReceived, received,
		runtimeevent.WithRunID(string(runID)),
		runtimeevent.WithSource(r.source),
		runtimeevent.WithCausationID(requestEvent.ID),
	)
	if err != nil {
		return false, err
	}
	return true, r.HandleEvent(ctx, event)
}

func (r *AgentRunner) resolveUserApproval(ctx context.Context, runID RunID, target string) (bool, error) {
	requestEvent, ok, err := r.latestRunEvent(ctx, runID, runtimeevent.EventUserApprovalRequired, target)
	if err != nil || !ok {
		return false, err
	}
	var request UserApprovalRequiredPayload
	if err := json.Unmarshal(requestEvent.Payload, &request); err != nil {
		return false, fmt.Errorf("runner: decode user approval request: %w", err)
	}
	received, err := r.userInteraction.ReceiveApproval(ctx, request)
	if err != nil {
		return false, err
	}
	if received.ToolCallID == "" {
		received.ToolCallID = request.ToolCallID
	}
	if received.ToolName == "" {
		received.ToolName = request.ToolName
	}
	event, err := r.dispatcher.NewEvent(runtimeevent.EventUserApprovalReceived, received,
		runtimeevent.WithRunID(string(runID)),
		runtimeevent.WithSource(r.source),
		runtimeevent.WithCausationID(requestEvent.ID),
	)
	if err != nil {
		return false, err
	}
	return true, r.HandleEvent(ctx, event)
}

func (r *AgentRunner) latestRunEvent(ctx context.Context, runID RunID, eventType runtimeevent.Type, target string) (runtimeevent.Event, bool, error) {
	if r == nil || r.events == nil {
		return runtimeevent.Event{}, false, nil
	}
	events, err := r.events.List(ctx, string(runID))
	if err != nil {
		return runtimeevent.Event{}, false, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.Type != eventType {
			continue
		}
		if target == "" || eventToolCallID(event) == target {
			return event.Clone(), true, nil
		}
	}
	return runtimeevent.Event{}, false, nil
}

func eventToolCallID(event runtimeevent.Event) string {
	var payload struct {
		ToolCallID string `json:"tool_call_id"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	return payload.ToolCallID
}

func (r *AgentRunner) enqueueRunResumed(ctx context.Context, runState state.RunState) error {
	if r == nil {
		return fmt.Errorf("agent runner is nil")
	}
	if r.dispatcher == nil {
		return fmt.Errorf("agent runner dispatcher is required")
	}
	event, err := r.dispatcher.NewEvent(runtimeevent.EventRunResumed, map[string]string{
		"last_event_id": runState.LastEventID,
		"phase":         string(runState.Phase),
	},
		runtimeevent.WithID(runResumedEventID(runState.RunID)),
		runtimeevent.WithRunID(runState.RunID),
		runtimeevent.WithSource(r.source),
		runtimeevent.WithCausationID(runState.LastEventID),
	)
	if err != nil {
		return err
	}
	return r.HandleEvent(ctx, event)
}

func (r *AgentRunner) pendingEventCount(ctx context.Context, runID string) (int, error) {
	if r == nil || r.eventInbox == nil {
		return 0, nil
	}
	events, err := r.eventInbox.ListPending(ctx, runID)
	if err != nil {
		return 0, err
	}
	return len(events), nil
}

func (r *AgentRunner) pendingEffectCount(ctx context.Context, runID string) (int, error) {
	if r == nil || r.effectStore == nil {
		return 0, nil
	}
	effects, err := r.effectStore.ListPending(ctx, runID)
	if err != nil {
		return 0, err
	}
	return len(effects), nil
}

func runResumedEventID(runID string) string {
	return "resume_" + strings.ReplaceAll(runID, "-", "_")
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

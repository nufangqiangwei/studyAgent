package reactor

import (
	"agent/internal/runtime/eventbus"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type EventSink interface {
	Publish(ctx context.Context, event eventbus.Event) (eventbus.PublishResult, error)
	PublishAsync(ctx context.Context, event eventbus.Event) (eventbus.PublishResult, error)
}

type ReactorOption func(*config)

type config struct {
	resolver             RuntimeResolver
	executors            *ExecutorRegistry
	dispatcher           EffectDispatcher
	sink                 EventSink
	resultDelivery       eventbus.DeliveryMode
	eventTimeout         time.Duration
	effectTimeout        time.Duration
	maxConcurrentEvents  int
	maxConcurrentEffects int
	source               string
	internalTopic        string
}

func WithRuntimeResolver(resolver RuntimeResolver) ReactorOption {
	return func(config *config) {
		config.resolver = resolver
	}
}

func WithExecutorRegistry(registry *ExecutorRegistry) ReactorOption {
	return func(config *config) {
		config.executors = registry
	}
}

func WithEffectDispatcher(dispatcher EffectDispatcher) ReactorOption {
	return func(config *config) {
		config.dispatcher = dispatcher
	}
}

func WithEventSink(sink EventSink) ReactorOption {
	return func(config *config) {
		config.sink = sink
	}
}

func WithEventBus(bus *eventbus.Bus) ReactorOption {
	return func(config *config) {
		config.sink = bus
	}
}

func WithResultDelivery(mode eventbus.DeliveryMode) ReactorOption {
	return func(config *config) {
		config.resultDelivery = mode
	}
}

func WithEventTimeout(timeout time.Duration) ReactorOption {
	return func(config *config) {
		config.eventTimeout = timeout
	}
}

func WithEffectTimeout(timeout time.Duration) ReactorOption {
	return func(config *config) {
		config.effectTimeout = timeout
	}
}

func WithMaxConcurrentEvents(limit int) ReactorOption {
	return func(config *config) {
		config.maxConcurrentEvents = limit
	}
}

func WithMaxConcurrentEffects(limit int) ReactorOption {
	return func(config *config) {
		config.maxConcurrentEffects = limit
	}
}

func WithSource(source string) ReactorOption {
	return func(config *config) {
		config.source = source
	}
}

func WithInternalTopic(topic string) ReactorOption {
	return func(config *config) {
		config.internalTopic = topic
	}
}

type Reactor struct {
	resolver             RuntimeResolver
	executors            *ExecutorRegistry
	dispatcher           EffectDispatcher
	sink                 EventSink
	resultDelivery       eventbus.DeliveryMode
	eventTimeout         time.Duration
	effectTimeout        time.Duration
	maxConcurrentEffects int
	source               string
	internalTopic        string
	eventSem             chan struct{}
	taskLocks            *taskLockPool
	cancelMu             sync.Mutex
	cancelSeq            int
	cancels              map[string]map[int]context.CancelFunc
}

type AdvanceResult struct {
	TaskID     string            `json:"task_id,omitempty"`
	Event      eventbus.Event    `json:"event"`
	State      StateResult       `json:"state"`
	Executions []EffectExecution `json:"executions,omitempty"`
	Events     []eventbus.Event  `json:"events,omitempty"`
	Ignored    bool              `json:"ignored,omitempty"`
}

type EffectExecution struct {
	Effect Effect           `json:"effect"`
	Result EffectResult     `json:"result,omitempty"`
	Events []eventbus.Event `json:"events,omitempty"`
	Error  string           `json:"error,omitempty"`
	err    error
}

func New(options ...ReactorOption) (*Reactor, error) {
	cfg := config{
		executors:            NewExecutorRegistry(),
		resultDelivery:       eventbus.DeliveryAsync,
		maxConcurrentEffects: 1,
		source:               "reactor",
		internalTopic:        DefaultInternalTopic,
	}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	if cfg.resolver == nil {
		return nil, fmt.Errorf("reactor: runtime resolver is required")
	}
	if cfg.executors == nil {
		cfg.executors = NewExecutorRegistry()
	}
	if cfg.dispatcher == nil {
		cfg.dispatcher = NewAsyncEffectDispatcher()
	}
	if cfg.resultDelivery != eventbus.DeliverySync {
		cfg.resultDelivery = eventbus.DeliveryAsync
	}
	if cfg.maxConcurrentEffects <= 0 {
		cfg.maxConcurrentEffects = 1
	}
	source := strings.TrimSpace(cfg.source)
	if source == "" {
		source = "reactor"
	}
	internalTopic := strings.TrimSpace(cfg.internalTopic)
	if internalTopic == "" {
		internalTopic = DefaultInternalTopic
	}

	var eventSem chan struct{}
	if cfg.maxConcurrentEvents > 0 {
		eventSem = make(chan struct{}, cfg.maxConcurrentEvents)
	}

	return &Reactor{
		resolver:             cfg.resolver,
		executors:            cfg.executors,
		dispatcher:           cfg.dispatcher,
		sink:                 cfg.sink,
		resultDelivery:       cfg.resultDelivery,
		eventTimeout:         cfg.eventTimeout,
		effectTimeout:        cfg.effectTimeout,
		maxConcurrentEffects: cfg.maxConcurrentEffects,
		source:               source,
		internalTopic:        internalTopic,
		eventSem:             eventSem,
		taskLocks:            newTaskLockPool(),
		cancels:              make(map[string]map[int]context.CancelFunc),
	}, nil
}

func (r *Reactor) Attach(bus *eventbus.Bus, filter eventbus.Filter, options ...eventbus.SubscribeOption) (eventbus.Subscription, error) {
	if r == nil {
		return eventbus.Subscription{}, fmt.Errorf("reactor is nil")
	}
	if bus == nil {
		return eventbus.Subscription{}, fmt.Errorf("reactor attach: event bus is required")
	}
	return bus.Subscribe(filter, r, options...)
}

func (r *Reactor) HandleEvent(ctx context.Context, event eventbus.Event) error {
	_, err := r.Advance(ctx, event)
	return err
}

func (r *Reactor) Advance(ctx context.Context, event eventbus.Event) (AdvanceResult, error) {
	if r == nil {
		return AdvanceResult{}, fmt.Errorf("reactor is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	event, err := completeEvent(event)
	if err != nil {
		return AdvanceResult{Event: event}, err
	}
	result := AdvanceResult{Event: event.Clone(), TaskID: event.TaskID}
	if r.isInternalEvent(event) {
		result.Ignored = true
		return result, nil
	}

	if err := acquire(ctx, r.eventSem); err != nil {
		return result, r.publishError(ctx, event, Effect{}, StageEventEntry, err)
	}
	defer release(r.eventSem)

	if r.eventTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.eventTimeout)
		defer cancel()
	}

	runtime, err := r.resolveRuntime(ctx, event)
	if err != nil {
		return result, r.publishError(ctx, event, Effect{}, StageTaskRoute, err)
	}
	result.TaskID = runtime.TaskID

	unlockTask := r.taskLocks.lock(runtime.TaskID)
	taskCtx, releaseCancel := r.registerTaskCancel(ctx, runtime.TaskID)
	stateResult, stateErr := runtime.StateMachine.HandleEvent(taskCtx, event.Clone())
	unlockTask()
	if stateErr != nil {
		releaseCancel()
		return result, r.publishError(taskCtx, event, Effect{}, StageStateMachine, stateErr)
	}
	stateResult = normalizeStateResult(runtime.TaskID, stateResult)
	result.State = stateResult.Clone()

	published, err := r.publishEvents(taskCtx, runtime, Effect{}, stateResult.Events)
	if err != nil {
		releaseCancel()
		return result, r.publishError(taskCtx, event, Effect{}, StagePublish, err)
	}
	result.Events = append(result.Events, published...)

	executions, effectErr := r.dispatchEffects(taskCtx, runtime, stateResult.Effects)
	releaseCancel()
	result.Executions = executions
	if effectErr != nil {
		return result, effectErr
	}
	return result, nil
}

func (r *Reactor) CancelTask(taskID string) bool {
	if r == nil {
		return false
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return false
	}
	r.cancelMu.Lock()
	cancellations := r.cancels[taskID]
	if len(cancellations) == 0 {
		r.cancelMu.Unlock()
		return false
	}
	toCancel := make([]context.CancelFunc, 0, len(cancellations))
	for _, cancel := range cancellations {
		toCancel = append(toCancel, cancel)
	}
	r.cancelMu.Unlock()
	for _, cancel := range toCancel {
		cancel()
	}
	return true
}

func (r *Reactor) resolveRuntime(ctx context.Context, event eventbus.Event) (TaskRuntime, error) {
	if r.resolver == nil {
		return TaskRuntime{}, fmt.Errorf("runtime resolver is required")
	}
	runtime, err := r.resolver.ResolveRuntime(ctx, event.Clone())
	if err != nil {
		return TaskRuntime{}, err
	}
	runtime.TaskID = strings.TrimSpace(runtime.TaskID)
	eventTaskID := strings.TrimSpace(event.TaskID)
	if runtime.TaskID == "" {
		runtime.TaskID = eventTaskID
	}
	if runtime.TaskID == "" {
		return TaskRuntime{}, fmt.Errorf("task runtime id is required")
	}
	if eventTaskID != "" && runtime.TaskID != eventTaskID {
		return TaskRuntime{}, fmt.Errorf("resolved task runtime %q does not match event task_id %q", runtime.TaskID, eventTaskID)
	}
	if runtime.StateMachine == nil {
		return TaskRuntime{}, fmt.Errorf("task runtime %q: state machine is required", runtime.TaskID)
	}
	return runtime, nil
}

func normalizeStateResult(taskID string, result StateResult) StateResult {
	result = result.Clone()
	if strings.TrimSpace(result.TaskID) == "" {
		result.TaskID = taskID
	}
	for i := range result.Effects {
		if strings.TrimSpace(result.Effects[i].TaskID) == "" {
			result.Effects[i].TaskID = taskID
		}
	}
	for i := range result.Events {
		if strings.TrimSpace(result.Events[i].TaskID) == "" {
			result.Events[i].TaskID = taskID
		}
	}
	return result
}

func (r *Reactor) dispatchEffects(ctx context.Context, runtime TaskRuntime, effects []Effect) ([]EffectExecution, error) {
	if len(effects) == 0 {
		return nil, nil
	}

	normalized := make([]Effect, 0, len(effects))
	for _, effect := range effects {
		if strings.TrimSpace(effect.TaskID) == "" {
			effect.TaskID = runtime.TaskID
		}
		completed, err := completeEffect(effect)
		if err != nil {
			failure := EffectExecution{Effect: effect.Clone(), Error: err.Error(), err: err}
			return []EffectExecution{failure}, r.publishError(ctx, eventbus.Event{TaskID: runtime.TaskID}, effect, StageEffectRoute, err)
		}
		normalized = append(normalized, completed)
	}

	limit := runtime.MaxConcurrentEffects
	if limit <= 0 {
		limit = r.maxConcurrentEffects
	}
	if limit <= 0 {
		limit = 1
	}
	sem := make(chan struct{}, limit)
	executions := make([]EffectExecution, 0, len(normalized))
	var failures ExecutionErrors

	for _, effect := range normalized {
		execution := EffectExecution{Effect: effect.Clone()}
		if effect.Type == EffectNoop {
			executions = append(executions, execution)
			continue
		}

		executor, ok := r.lookupExecutor(runtime, effect.Type)
		if !ok {
			err := fmt.Errorf("executor for effect %q: not found", effect.Type)
			execution.Error = err.Error()
			execution.err = err
			_ = r.EffectFailed(ctx, runtime, effect, StageEffectRoute, err)
			executions = append(executions, execution)
			failures = append(failures, ExecutionError{
				EffectID:   effect.ID,
				EffectType: effect.Type,
				Err:        err,
			})
			continue
		}

		effectCtx, releaseCancel := r.registerTaskCancel(context.Background(), runtime.TaskID)
		timeout := runtime.EffectTimeout
		if timeout <= 0 {
			timeout = r.effectTimeout
		}
		err := r.dispatcher.DispatchEffect(ctx, EffectDispatchRequest{
			Context:   effectCtx,
			Runtime:   runtime.Clone(),
			Effect:    effect.Clone(),
			Executor:  executor,
			Timeout:   timeout,
			Semaphore: sem,
			Reporter:  r,
			OnDone:    releaseCancel,
		})
		if err != nil {
			releaseCancel()
			execution.Error = err.Error()
			execution.err = err
			_ = r.EffectFailed(ctx, runtime, effect, StageEffectExecute, err)
			err := execution.err
			if err == nil {
				err = fmt.Errorf("%s", execution.Error)
			}
			failures = append(failures, ExecutionError{
				EffectID:   execution.Effect.ID,
				EffectType: execution.Effect.Type,
				Err:        err,
			})
		}
		executions = append(executions, execution)
	}
	if len(failures) > 0 {
		return executions, failures
	}
	return executions, nil
}

func (r *Reactor) lookupExecutor(runtime TaskRuntime, effectType EffectType) (EffectExecutor, bool) {
	if runtime.Executors != nil {
		if executor, ok := runtime.Executors.Lookup(effectType); ok {
			return executor, true
		}
	}
	return r.executors.Lookup(effectType)
}

func (r *Reactor) EffectStarted(ctx context.Context, runtime TaskRuntime, effect Effect) error {
	return r.publishEffectLifecycle(ctx, runtime, effect, EventEffectStarted, "started", nil)
}

func (r *Reactor) EffectSucceeded(ctx context.Context, runtime TaskRuntime, effect Effect) error {
	return r.publishEffectLifecycle(ctx, runtime, effect, EventEffectSucceeded, "succeeded", nil)
}

func (r *Reactor) EffectFailed(ctx context.Context, runtime TaskRuntime, effect Effect, stage ErrorStage, err error) error {
	if err == nil {
		return nil
	}
	lifecycleErr := r.publishEffectLifecycle(ctx, runtime, effect, EventEffectFailed, "failed", err)
	errorErr := r.publishError(ctx, eventbus.Event{TaskID: runtime.TaskID}, effect, stage, err)
	if lifecycleErr != nil && errorErr != nil {
		return fmt.Errorf("%v; publish effect lifecycle: %w", errorErr, lifecycleErr)
	}
	if errorErr != nil {
		return errorErr
	}
	return lifecycleErr
}

func (r *Reactor) EffectResultEvents(ctx context.Context, runtime TaskRuntime, effect Effect, events []eventbus.Event) ([]eventbus.Event, error) {
	return r.publishEvents(ctx, runtime, effect, events)
}

func (r *Reactor) publishEvents(ctx context.Context, runtime TaskRuntime, effect Effect, events []eventbus.Event) ([]eventbus.Event, error) {
	if len(events) == 0 {
		return nil, nil
	}
	published := make([]eventbus.Event, 0, len(events))
	for _, event := range events {
		event = event.Clone()
		if strings.TrimSpace(event.TaskID) == "" {
			event.TaskID = runtime.TaskID
		}
		if strings.TrimSpace(event.Source) == "" {
			event.Source = r.source
		}
		if strings.TrimSpace(effect.ID) != "" {
			if event.Metadata == nil {
				event.Metadata = make(map[string]string, 1)
			}
			if event.Metadata["effect_id"] == "" {
				event.Metadata["effect_id"] = effect.ID
			}
		}
		completed, err := completeEvent(event)
		if err != nil {
			return published, err
		}
		if err := r.publish(ctx, completed); err != nil {
			return published, err
		}
		published = append(published, completed)
	}
	return published, nil
}

func (r *Reactor) publish(ctx context.Context, event eventbus.Event) error {
	if r.sink == nil {
		return nil
	}
	if r.resultDelivery == eventbus.DeliverySync {
		_, err := r.sink.Publish(ctx, event)
		return err
	}
	_, err := r.sink.PublishAsync(ctx, event)
	return err
}

func (r *Reactor) publishEffectLifecycle(ctx context.Context, runtime TaskRuntime, effect Effect, eventType eventbus.EventType, status string, effectErr error) error {
	payload := EffectLifecyclePayload{
		TaskID:     runtime.TaskID,
		EffectID:   effect.ID,
		EffectType: effect.Type,
		Status:     status,
	}
	if effectErr != nil {
		payload.Error = effectErr.Error()
	}
	event, err := r.internalEvent(eventType, runtime.TaskID, payload)
	if err != nil {
		return err
	}
	return r.publish(liveContext(ctx), event)
}

func (r *Reactor) publishError(ctx context.Context, event eventbus.Event, effect Effect, stage ErrorStage, err error) error {
	if err == nil {
		return nil
	}
	payload := ErrorPayload{
		Stage:      stage,
		TaskID:     event.TaskID,
		EventID:    event.ID,
		EventType:  event.Type,
		EffectID:   effect.ID,
		EffectType: effect.Type,
		Message:    err.Error(),
	}
	if payload.TaskID == "" {
		payload.TaskID = effect.TaskID
	}
	errorEvent, createErr := r.internalEvent(EventReactorError, payload.TaskID, payload)
	if createErr != nil {
		return fmt.Errorf("%v; create error event: %w", err, createErr)
	}
	if publishErr := r.publish(liveContext(ctx), errorEvent); publishErr != nil {
		return fmt.Errorf("%v; publish error event: %w", err, publishErr)
	}
	return err
}

func (r *Reactor) internalEvent(eventType eventbus.EventType, taskID string, payload any) (eventbus.Event, error) {
	return eventbus.NewEvent(r.internalTopic, eventType, payload,
		eventbus.WithTaskID(taskID),
		eventbus.WithSource(r.source),
		eventbus.WithMetadataValue(InternalMetadataKey, "true"),
	)
}

func (r *Reactor) isInternalEvent(event eventbus.Event) bool {
	if event.Metadata[InternalMetadataKey] == "true" {
		return true
	}
	return event.Topic == r.internalTopic &&
		(event.Type == EventReactorError ||
			event.Type == EventEffectStarted ||
			event.Type == EventEffectSucceeded ||
			event.Type == EventEffectFailed)
}

func (r *Reactor) registerTaskCancel(ctx context.Context, taskID string) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	taskCtx, cancel := context.WithCancel(ctx)
	r.cancelMu.Lock()
	r.cancelSeq++
	token := r.cancelSeq
	if r.cancels[taskID] == nil {
		r.cancels[taskID] = make(map[int]context.CancelFunc, 1)
	}
	r.cancels[taskID][token] = cancel
	r.cancelMu.Unlock()
	return taskCtx, func() {
		cancel()
		r.cancelMu.Lock()
		if cancellations := r.cancels[taskID]; cancellations != nil {
			delete(cancellations, token)
		}
		if len(r.cancels[taskID]) == 0 {
			delete(r.cancels, taskID)
		}
		r.cancelMu.Unlock()
	}
}

func completeEvent(event eventbus.Event) (eventbus.Event, error) {
	if strings.TrimSpace(string(event.Type)) == "" {
		return eventbus.Event{}, fmt.Errorf("event type is required")
	}
	options := make([]eventbus.EventOption, 0, 6)
	if strings.TrimSpace(event.ID) != "" {
		options = append(options, eventbus.WithEventID(event.ID))
	}
	if strings.TrimSpace(event.TaskID) != "" {
		options = append(options, eventbus.WithTaskID(event.TaskID))
	}
	if !event.OccurredAt.IsZero() {
		options = append(options, eventbus.WithOccurredAt(event.OccurredAt))
	}
	if strings.TrimSpace(event.Source) != "" {
		options = append(options, eventbus.WithSource(event.Source))
	}
	if len(event.Metadata) > 0 {
		options = append(options, eventbus.WithMetadata(event.Metadata))
	}
	return eventbus.NewEvent(event.Topic, event.Type, event.Payload, options...)
}

func acquire(ctx context.Context, sem chan struct{}) error {
	if sem == nil {
		return nil
	}
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func release(sem chan struct{}) {
	if sem == nil {
		return
	}
	<-sem
}

func liveContext(ctx context.Context) context.Context {
	if ctx == nil || ctx.Err() != nil {
		return context.Background()
	}
	return ctx
}

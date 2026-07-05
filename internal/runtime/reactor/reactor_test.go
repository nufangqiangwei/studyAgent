package reactor

import (
	"agent/internal/runtime/eventbus"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReactorRoutesEventToRuntimeStateMachineAndDispatchesEffectAsync(t *testing.T) {
	ctx := context.Background()
	sink := &recordingSink{}
	machine := &recordingMachine{}
	effect := mustEffect(t, EffectToolDispatch, WithEffectID("eff_tool"))
	machine.result = StateResult{Effects: []Effect{effect}}

	executors := NewExecutorRegistry()
	if err := executors.RegisterFunc(EffectToolDispatch, func(_ context.Context, runtime TaskRuntime, effect Effect) (EffectResult, error) {
		if runtime.TaskID != "task_1" || runtime.Agent != "code" {
			t.Fatalf("runtime = %#v, want task_1 code", runtime)
		}
		if effect.TaskID != "task_1" || effect.ID != "eff_tool" {
			t.Fatalf("effect = %#v, want task filled and original id", effect)
		}
		return EffectResult{Events: []eventbus.Event{{
			Topic: "runtime",
			Type:  "ToolCallCompleted",
		}}}, nil
	}); err != nil {
		t.Fatalf("RegisterFunc returned error: %v", err)
	}
	runtime := TaskRuntime{
		TaskID:       "task_1",
		Agent:        "code",
		StateMachine: machine,
	}
	reactor := mustReactor(t, runtime, executors, sink)

	result, err := reactor.Advance(ctx, mustEvent(t, "runtime", "ToolCallRequested", "task_1"))
	if err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}

	if len(machine.events) != 1 || machine.events[0].Type != "ToolCallRequested" {
		t.Fatalf("machine events = %#v, want ToolCallRequested", machine.events)
	}
	if len(result.Executions) != 1 || result.Executions[0].Effect.Type != EffectToolDispatch {
		t.Fatalf("executions = %#v, want one tool dispatch", result.Executions)
	}
	if len(result.Events) != 0 {
		t.Fatalf("result events = %#v, want no effect result events from Advance", result.Events)
	}
	published := waitForNonInternalEvent(t, sink, "ToolCallCompleted")
	if published.TaskID != "task_1" {
		t.Fatalf("published event = %#v, want task-scoped ToolCallCompleted", published)
	}
}

func TestReactorAdvanceReturnsBeforeAsyncEffectCompletes(t *testing.T) {
	sink := &recordingSink{}
	machine := &recordingMachine{result: StateResult{Effects: []Effect{mustEffect(t, EffectToolDispatch, WithEffectID("eff_blocking"))}}}
	started := make(chan struct{})
	release := make(chan struct{})

	executors := NewExecutorRegistry()
	if err := executors.RegisterFunc(EffectToolDispatch, func(ctx context.Context, _ TaskRuntime, _ Effect) (EffectResult, error) {
		close(started)
		select {
		case <-release:
			return EffectResult{Events: []eventbus.Event{{
				Topic: "runtime",
				Type:  "ToolCallCompleted",
			}}}, nil
		case <-ctx.Done():
			return EffectResult{}, ctx.Err()
		}
	}); err != nil {
		t.Fatalf("RegisterFunc returned error: %v", err)
	}
	reactor := mustReactor(t, TaskRuntime{TaskID: "task_1", StateMachine: machine}, executors, sink)

	done := make(chan error, 1)
	go func() {
		_, err := reactor.Advance(context.Background(), mustEvent(t, "runtime", "ToolCallRequested", "task_1"))
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Advance returned error: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Advance blocked while effect executor was still running")
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async effect executor to start")
	}
	close(release)
	waitForNonInternalEvent(t, sink, "ToolCallCompleted")
}

func TestReactorPublishesEffectResultEventsAsyncByDefault(t *testing.T) {
	sink := &recordingSink{}
	machine := &recordingMachine{result: StateResult{Effects: []Effect{mustEffect(t, EffectToolDispatch)}}}

	executors := NewExecutorRegistry()
	if err := executors.RegisterFunc(EffectToolDispatch, func(context.Context, TaskRuntime, Effect) (EffectResult, error) {
		return EffectResult{Events: []eventbus.Event{{
			Topic: "runtime",
			Type:  "ToolCallCompleted",
		}}}, nil
	}); err != nil {
		t.Fatalf("RegisterFunc returned error: %v", err)
	}
	registry := mustRuntimeRegistry(t, TaskRuntime{TaskID: "task_1", StateMachine: machine})
	reactor, err := New(
		WithRuntimeResolver(registry),
		WithExecutorRegistry(executors),
		WithEventSink(sink),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	if _, err := reactor.Advance(context.Background(), mustEvent(t, "runtime", "ToolCallRequested", "task_1")); err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}
	waitForNonInternalEvent(t, sink, "ToolCallCompleted")

	if sink.syncPublishCount() != 0 {
		t.Fatalf("sync publishes = %d, want 0 by default", sink.syncPublishCount())
	}
	if sink.asyncPublishCount() == 0 {
		t.Fatal("async publishes = 0, want effect result event published asynchronously")
	}
}

func TestReactorConvertsStateMachineErrorToEvent(t *testing.T) {
	sink := &recordingSink{}
	machine := &recordingMachine{err: errors.New("state exploded")}
	reactor := mustReactor(t, TaskRuntime{TaskID: "task_1", StateMachine: machine}, nil, sink)

	_, err := reactor.Advance(context.Background(), mustEvent(t, "runtime", "RunStarted", "task_1"))
	if err == nil || !strings.Contains(err.Error(), "state exploded") {
		t.Fatalf("Advance error = %v, want state error", err)
	}

	payload := latestErrorPayload(t, sink)
	if payload.Stage != StageStateMachine || payload.TaskID != "task_1" || payload.EventType != "RunStarted" {
		t.Fatalf("error payload = %#v, want state_machine for task_1 RunStarted", payload)
	}
}

func TestReactorAttachReceivesEventsFromEventBus(t *testing.T) {
	bus, err := eventbus.New()
	if err != nil {
		t.Fatalf("eventbus.New returned error: %v", err)
	}
	defer bus.Close()

	delivered := make(chan eventbus.Event, 1)
	machine := StateMachineFunc(func(_ context.Context, event eventbus.Event) (StateResult, error) {
		delivered <- event
		return StateResult{}, nil
	})
	registry := mustRuntimeRegistry(t, TaskRuntime{TaskID: "task_1", StateMachine: machine})
	reactor, err := New(WithRuntimeResolver(registry), WithEventBus(bus), WithResultDelivery(eventbus.DeliverySync))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if _, err := reactor.Attach(bus, eventbus.Filter{Topic: "runtime"}, eventbus.WithSubscriptionID("reactor")); err != nil {
		t.Fatalf("Attach returned error: %v", err)
	}

	if _, err := bus.Publish(context.Background(), mustEvent(t, "runtime", "RunStarted", "task_1")); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	select {
	case event := <-delivered:
		if event.TaskID != "task_1" || event.Type != "RunStarted" {
			t.Fatalf("delivered = %#v, want task_1 RunStarted", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reactor bus delivery")
	}
}

func TestReactorAppliesEffectTimeoutAndPublishesErrorEvent(t *testing.T) {
	sink := &recordingSink{}
	machine := &recordingMachine{result: StateResult{Effects: []Effect{mustEffect(t, EffectAgentStart, WithEffectID("eff_slow"))}}}
	executors := NewExecutorRegistry()
	if err := executors.RegisterFunc(EffectAgentStart, func(ctx context.Context, _ TaskRuntime, _ Effect) (EffectResult, error) {
		<-ctx.Done()
		return EffectResult{}, ctx.Err()
	}); err != nil {
		t.Fatalf("RegisterFunc returned error: %v", err)
	}
	reactor := mustReactorWithOptions(t,
		TaskRuntime{TaskID: "task_1", StateMachine: machine},
		executors,
		sink,
		WithEffectTimeout(10*time.Millisecond),
	)

	if _, err := reactor.Advance(context.Background(), mustEvent(t, "runtime", "RunStarted", "task_1")); err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}

	payload := waitForLatestErrorPayload(t, sink)
	if payload.Stage != StageEffectExecute || payload.EffectID != "eff_slow" || payload.EffectType != EffectAgentStart {
		t.Fatalf("error payload = %#v, want effect_execute for eff_slow", payload)
	}
	if !waitForInternalEvent(t, sink, EventEffectFailed) {
		t.Fatalf("published events = %#v, want effect failed lifecycle", sink.events)
	}
}

func TestReactorCancelTaskCancelsRunningEffect(t *testing.T) {
	sink := &recordingSink{}
	started := make(chan struct{})
	machine := &recordingMachine{result: StateResult{Effects: []Effect{mustEffect(t, EffectAgentResume, WithEffectID("eff_resume"))}}}
	executors := NewExecutorRegistry()
	if err := executors.RegisterFunc(EffectAgentResume, func(ctx context.Context, _ TaskRuntime, _ Effect) (EffectResult, error) {
		close(started)
		<-ctx.Done()
		return EffectResult{}, ctx.Err()
	}); err != nil {
		t.Fatalf("RegisterFunc returned error: %v", err)
	}
	reactor := mustReactor(t, TaskRuntime{TaskID: "task_1", StateMachine: machine}, executors, sink)

	if _, err := reactor.Advance(context.Background(), mustEvent(t, "runtime", "RunResumed", "task_1")); err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for effect start")
	}
	if !reactor.CancelTask("task_1") {
		t.Fatal("CancelTask returned false, want true for running task")
	}
	payload := waitForLatestErrorPayload(t, sink)
	if payload.Stage != StageEffectExecute || payload.EffectID != "eff_resume" || !strings.Contains(payload.Message, context.Canceled.Error()) {
		t.Fatalf("error payload = %#v, want cancelled eff_resume execution", payload)
	}
}

func TestReactorSerializesStateMachineByTask(t *testing.T) {
	machine := &concurrencyCheckingMachine{delay: 20 * time.Millisecond}
	reactor := mustReactor(t, TaskRuntime{TaskID: "task_1", StateMachine: machine}, nil, &recordingSink{})

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := reactor.Advance(context.Background(), mustEvent(t, "runtime", "RunStarted", "task_1"))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Advance returned error: %v", err)
		}
	}
	if machine.maxActive != 1 {
		t.Fatalf("max active state machines = %d, want 1", machine.maxActive)
	}
}

func mustReactor(t *testing.T, runtime TaskRuntime, executors *ExecutorRegistry, sink EventSink) *Reactor {
	t.Helper()
	return mustReactorWithOptions(t, runtime, executors, sink)
}

func mustReactorWithOptions(t *testing.T, runtime TaskRuntime, executors *ExecutorRegistry, sink EventSink, options ...ReactorOption) *Reactor {
	t.Helper()
	registry := mustRuntimeRegistry(t, runtime)
	opts := []ReactorOption{
		WithRuntimeResolver(registry),
		WithEventSink(sink),
		WithResultDelivery(eventbus.DeliverySync),
	}
	if executors != nil {
		opts = append(opts, WithExecutorRegistry(executors))
	}
	opts = append(opts, options...)
	reactor, err := New(opts...)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return reactor
}

func mustRuntimeRegistry(t *testing.T, runtimes ...TaskRuntime) *RuntimeRegistry {
	t.Helper()
	registry, err := NewRuntimeRegistry(runtimes...)
	if err != nil {
		t.Fatalf("NewRuntimeRegistry returned error: %v", err)
	}
	return registry
}

func mustEvent(t *testing.T, topic string, eventType eventbus.EventType, taskID string) eventbus.Event {
	t.Helper()
	event, err := eventbus.NewEvent(topic, eventType, nil, eventbus.WithTaskID(taskID))
	if err != nil {
		t.Fatalf("NewEvent returned error: %v", err)
	}
	return event
}

func mustEffect(t *testing.T, effectType EffectType, options ...EffectOption) Effect {
	t.Helper()
	effect, err := NewEffect("", effectType, nil, options...)
	if err != nil {
		t.Fatalf("NewEffect returned error: %v", err)
	}
	return effect
}

func latestErrorPayload(t *testing.T, sink *recordingSink) ErrorPayload {
	t.Helper()
	payload, ok := readLatestErrorPayload(t, sink)
	if ok {
		return payload
	}
	t.Fatalf("events = %#v, want %s", sink.events, EventReactorError)
	return ErrorPayload{}
}

func waitForLatestErrorPayload(t *testing.T, sink *recordingSink) ErrorPayload {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if payload, ok := readLatestErrorPayload(t, sink); ok {
			return payload
		}
		select {
		case <-deadline:
			t.Fatalf("events = %#v, want %s", sink.events, EventReactorError)
		case <-tick.C:
		}
	}
}

func readLatestErrorPayload(t *testing.T, sink *recordingSink) (ErrorPayload, bool) {
	t.Helper()
	sink.mu.Lock()
	events := make([]eventbus.Event, len(sink.events))
	copy(events, sink.events)
	sink.mu.Unlock()
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.Type != EventReactorError {
			continue
		}
		var payload ErrorPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("decode error payload: %v", err)
		}
		return payload, true
	}
	return ErrorPayload{}, false
}

func waitForNonInternalEvent(t *testing.T, sink *recordingSink, eventType eventbus.EventType) eventbus.Event {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		for _, event := range sink.nonInternalEvents() {
			if event.Type == eventType {
				return event
			}
		}
		select {
		case <-deadline:
			t.Fatalf("events = %#v, want non-internal %s", sink.events, eventType)
		case <-tick.C:
		}
	}
}

func waitForInternalEvent(t *testing.T, sink *recordingSink, eventType eventbus.EventType) bool {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if sink.hasInternalEvent(eventType) {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-tick.C:
		}
	}
}

type recordingSink struct {
	mu             sync.Mutex
	events         []eventbus.Event
	syncPublishes  int
	asyncPublishes int
}

func (s *recordingSink) Publish(_ context.Context, event eventbus.Event) (eventbus.PublishResult, error) {
	s.mu.Lock()
	s.syncPublishes++
	s.mu.Unlock()
	s.append(event)
	return eventbus.PublishResult{Event: event.Clone(), Mode: eventbus.DeliverySync}, nil
}

func (s *recordingSink) PublishAsync(_ context.Context, event eventbus.Event) (eventbus.PublishResult, error) {
	s.mu.Lock()
	s.asyncPublishes++
	s.mu.Unlock()
	s.append(event)
	return eventbus.PublishResult{Event: event.Clone(), Mode: eventbus.DeliveryAsync, Queued: true}, nil
}

func (s *recordingSink) append(event eventbus.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event.Clone())
}

func (s *recordingSink) syncPublishCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncPublishes
}

func (s *recordingSink) asyncPublishCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.asyncPublishes
}

func (s *recordingSink) nonInternalEvents() []eventbus.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]eventbus.Event, 0, len(s.events))
	for _, event := range s.events {
		if event.Metadata[InternalMetadataKey] == "true" {
			continue
		}
		events = append(events, event.Clone())
	}
	return events
}

func (s *recordingSink) hasInternalEvent(eventType eventbus.EventType) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.events {
		if event.Type == eventType && event.Metadata[InternalMetadataKey] == "true" {
			return true
		}
	}
	return false
}

type recordingMachine struct {
	mu     sync.Mutex
	events []eventbus.Event
	result StateResult
	err    error
}

func (m *recordingMachine) HandleEvent(_ context.Context, event eventbus.Event) (StateResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event.Clone())
	if m.err != nil {
		return StateResult{}, m.err
	}
	return m.result.Clone(), nil
}

type concurrencyCheckingMachine struct {
	mu        sync.Mutex
	active    int
	maxActive int
	delay     time.Duration
}

func (m *concurrencyCheckingMachine) HandleEvent(ctx context.Context, event eventbus.Event) (StateResult, error) {
	_ = event
	m.mu.Lock()
	m.active++
	if m.active > m.maxActive {
		m.maxActive = m.active
	}
	m.mu.Unlock()

	select {
	case <-time.After(m.delay):
	case <-ctx.Done():
		return StateResult{}, ctx.Err()
	}

	m.mu.Lock()
	m.active--
	m.mu.Unlock()
	return StateResult{}, nil
}

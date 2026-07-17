package runservice

import (
	"agent/internal/foundation/identity"
	appruntime "agent/internal/runtime"
	"agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/persistence"
	"agent/internal/runtime/projection"
	"agent/internal/runtime/reactor"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
)

const (
	StatusEventEnqueued    = "event_enqueued"
	StatusEventProcessed   = "event_processed"
	StatusEffectDispatched = "effect_dispatched"
	StatusIdle             = "idle"
)

type Builder interface {
	Build(ctx context.Context, taskID string, agentName string) (*Setup, error)
	OpenStorage(ctx context.Context) (persistence.RuntimeStorage, *persistence.WorkQueue, func(), error)
	DefaultWorkDir() string
}

type Setup struct {
	Runtime     *appruntime.Runtime
	Storage     persistence.RuntimeStorage
	Queue       *persistence.WorkQueue
	TaskRuntime *appruntime.TaskRuntime
	WorkDir     string
}

func (s *Setup) Close() {
	if s != nil && s.Runtime != nil {
		_ = s.Runtime.Close()
	}
}

func (s *Setup) ExecuteEffect(ctx context.Context, effect reactor.Effect) ([]string, error) {
	if s == nil || s.Runtime == nil || s.Queue == nil {
		return nil, fmt.Errorf("runtime setup is incomplete")
	}
	taskRuntime, err := s.Runtime.RuntimeRegistry().ResolveRuntime(ctx, eventbus.Event{TaskID: effect.TaskID})
	if err != nil {
		return nil, err
	}
	executor, ok := s.Runtime.ExecutorRegistry().Lookup(effect.Type)
	if !ok {
		return nil, fmt.Errorf("executor for effect %q not found", effect.Type)
	}
	result, err := executor.ExecuteEffect(ctx, taskRuntime, effect.Clone())
	if err != nil {
		return nil, err
	}
	produced := make([]string, 0, len(result.Events))
	for _, event := range result.Events {
		if strings.TrimSpace(event.TaskID) == "" {
			event.TaskID = effect.TaskID
		}
		produced = append(produced, string(event.Type))
		if event.Topic == statemachine.TopicTask {
			if err := s.Queue.EnqueueEvent(ctx, event); err != nil {
				return produced, err
			}
			continue
		}
		if _, err := s.Runtime.Publish(ctx, event); err != nil {
			return produced, err
		}
	}
	return produced, nil
}

type Options struct {
	Builder      Builder
	Agents       *agents.FactoryRegistry
	InitialAgent string
	MaxSteps     int
	Out          io.Writer
}

type Service struct {
	builder  Builder
	agents   *agents.FactoryRegistry
	maxSteps int
	out      io.Writer

	mu      sync.RWMutex
	current string
}

type RecoverResult = projection.RecoverResult

type WorkResult = projection.WorkResult

func New(options Options) (*Service, error) {
	if options.Builder == nil {
		return nil, fmt.Errorf("run service: builder is required")
	}
	if options.Agents == nil {
		return nil, fmt.Errorf("run service: agent factory registry is required")
	}
	canonical, ok := options.Agents.CanonicalName(options.InitialAgent)
	if !ok {
		return nil, fmt.Errorf("agent %q not found", strings.TrimSpace(options.InitialAgent))
	}
	maxSteps := options.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 20
	}
	return &Service{
		builder: options.Builder, agents: options.Agents, current: canonical,
		maxSteps: maxSteps, out: options.Out,
	}, nil
}

func (s *Service) Run(ctx context.Context, task string) error {
	status, err := s.Submit(ctx, task)
	if err != nil {
		return err
	}
	for i := 0; i < maxInt(s.maxSteps*4, 80); i++ {
		if IsTerminalPhase(status.Phase) {
			if s.out != nil && strings.TrimSpace(status.FinalAnswer) != "" {
				_, err := fmt.Fprintln(s.out, status.FinalAnswer)
				return err
			}
			return nil
		}
		switch {
		case status.PendingEvents > 0:
			status, err = s.Advance(ctx, status.RunID)
		case status.PendingEffects > 0:
			status, err = s.DispatchNextEffect(ctx, status.RunID)
		default:
			return nil
		}
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("run service: synchronous run reached step limit for %s", status.RunID)
}

func (s *Service) Submit(ctx context.Context, task string) (projection.RunStatus, error) {
	task = strings.TrimSpace(task)
	if task == "" {
		return projection.RunStatus{}, fmt.Errorf("run service: task is required")
	}
	taskID, err := identity.New("task")
	if err != nil {
		return projection.RunStatus{}, err
	}
	agentName := s.ActiveAgentName()
	setup, err := s.builder.Build(ctx, taskID, agentName)
	if err != nil {
		return projection.RunStatus{}, err
	}
	defer setup.Close()
	event, err := eventbus.NewEvent(statemachine.TopicTask, statemachine.EventTaskStartRequested, statemachine.TaskStartPayload{
		Agent: setup.TaskRuntime.AgentName(), Input: task,
		Metadata: map[string]string{
			"entry": "app", "input": task, "selected_agent": agentName, "work_dir": setup.WorkDir,
		},
	}, eventbus.WithTaskID(taskID), eventbus.WithSource(s.source(agentName)))
	if err != nil {
		return projection.RunStatus{}, err
	}
	if err := setup.Queue.EnqueueEvent(ctx, event); err != nil {
		return projection.RunStatus{}, err
	}
	return s.project(ctx, setup.Storage, setup.Queue, taskID, projection.StatusUpdate{
		AdvanceStatus: StatusEventEnqueued, EventType: string(event.Type),
	})
}

func (s *Service) Recover(ctx context.Context) (RecoverResult, error) {
	storage, queue, closeStore, err := s.builder.OpenStorage(ctx)
	if err != nil {
		return RecoverResult{}, err
	}
	defer closeStore()
	runtimes, err := storage.Runtimes().List(ctx)
	if err != nil {
		return RecoverResult{}, err
	}
	statuses := make([]projection.RunStatus, 0, len(runtimes))
	for _, snapshot := range runtimes {
		status, err := s.project(ctx, storage, queue, snapshot.TaskID, projection.StatusUpdate{})
		if err != nil {
			return RecoverResult{}, err
		}
		if IsTerminalPhase(status.Phase) && status.PendingEvents == 0 && status.PendingEffects == 0 {
			continue
		}
		statuses = append(statuses, status)
	}
	return RecoverResult{Runs: statuses}, nil
}

func (s *Service) Work(ctx context.Context) (WorkResult, error) {
	_, queue, closeStore, err := s.builder.OpenStorage(ctx)
	if err != nil {
		return WorkResult{}, err
	}
	defer closeStore()
	work, ok, err := queue.Next(ctx)
	if err != nil || !ok {
		return WorkResult{}, err
	}
	var status projection.RunStatus
	switch work.Kind {
	case persistence.WorkEvent:
		status, err = s.Advance(ctx, work.TaskID)
	case persistence.WorkEffect:
		status, err = s.DispatchNextEffect(ctx, work.TaskID)
	default:
		err = fmt.Errorf("run service: unsupported queued work kind %q", work.Kind)
	}
	if err != nil {
		return WorkResult{}, err
	}
	return WorkResult{Ran: true, Status: status}, nil
}

func (s *Service) Advance(ctx context.Context, runID string) (projection.RunStatus, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return projection.RunStatus{}, fmt.Errorf("advance: run id is required")
	}
	setup, err := s.builder.Build(ctx, runID, s.ActiveAgentName())
	if err != nil {
		return projection.RunStatus{}, err
	}
	defer setup.Close()
	event, ok, err := setup.Queue.PopEvent(ctx, runID)
	if err != nil {
		return projection.RunStatus{}, err
	}
	if !ok {
		return s.project(ctx, setup.Storage, setup.Queue, runID, projection.StatusUpdate{AdvanceStatus: StatusIdle})
	}
	if _, err := setup.Runtime.Publish(ctx, event); err != nil {
		return projection.RunStatus{}, err
	}
	return s.project(ctx, setup.Storage, setup.Queue, runID, projection.StatusUpdate{
		AdvanceStatus: StatusEventProcessed, EventType: string(event.Type),
	})
}

func (s *Service) DispatchNextEffect(ctx context.Context, runID string) (projection.RunStatus, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return projection.RunStatus{}, fmt.Errorf("effect: run id is required")
	}
	setup, err := s.builder.Build(ctx, runID, s.ActiveAgentName())
	if err != nil {
		return projection.RunStatus{}, err
	}
	defer setup.Close()
	effect, ok, err := setup.Queue.PopEffect(ctx, runID)
	if err != nil {
		return projection.RunStatus{}, err
	}
	if !ok {
		return s.project(ctx, setup.Storage, setup.Queue, runID, projection.StatusUpdate{AdvanceStatus: StatusIdle})
	}
	produced, err := setup.ExecuteEffect(ctx, effect)
	if err != nil {
		return projection.RunStatus{}, err
	}
	return s.project(ctx, setup.Storage, setup.Queue, runID, projection.StatusUpdate{
		AdvanceStatus: StatusEffectDispatched, EffectType: string(effect.Type), ProducedEventTypes: produced,
	})
}

func (s *Service) SubmitUserInput(ctx context.Context, runID string, answer string) (projection.RunStatus, error) {
	return s.submitUserEvent(ctx, runID, strings.TrimSpace(answer), "")
}

func (s *Service) SubmitUserApproval(ctx context.Context, runID string, approved bool, reason string) (projection.RunStatus, error) {
	answer := "no"
	if approved {
		answer = "yes"
	}
	if strings.TrimSpace(reason) != "" {
		answer += ": " + strings.TrimSpace(reason)
	}
	return s.submitUserEvent(ctx, runID, answer, "approval")
}

func (s *Service) Result(ctx context.Context, runID string) (projection.RunStatus, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return projection.RunStatus{}, fmt.Errorf("result: run id is required")
	}
	storage, queue, closeStore, err := s.builder.OpenStorage(ctx)
	if err != nil {
		return projection.RunStatus{}, err
	}
	defer closeStore()
	return s.project(ctx, storage, queue, runID, projection.StatusUpdate{})
}

func (s *Service) ActiveAgentName() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

func (s *Service) ListAgentNames() []string {
	if s == nil {
		return nil
	}
	return s.agents.ListNames()
}

func (s *Service) SelectAgent(name string) error {
	if s == nil {
		return fmt.Errorf("run service is nil")
	}
	canonical, ok := s.agents.CanonicalName(name)
	if !ok {
		return fmt.Errorf("agent %q not found", strings.TrimSpace(name))
	}
	s.mu.Lock()
	s.current = canonical
	s.mu.Unlock()
	return nil
}

func (s *Service) submitUserEvent(ctx context.Context, runID string, answer string, reason string) (projection.RunStatus, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return projection.RunStatus{}, fmt.Errorf("input: run id is required")
	}
	storage, queue, closeStore, err := s.builder.OpenStorage(ctx)
	if err != nil {
		return projection.RunStatus{}, err
	}
	defer closeStore()
	requestID := ""
	if state, ok, loadErr := storage.TaskStates().Load(ctx, runID); loadErr != nil {
		return projection.RunStatus{}, loadErr
	} else if ok && state.PendingUserInput != nil {
		requestID = state.PendingUserInput.RequestID
	}
	if requestID == "" {
		requestID, err = identity.New("input")
		if err != nil {
			return projection.RunStatus{}, err
		}
	}
	metadata := json.RawMessage(nil)
	if reason != "" {
		raw, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			return projection.RunStatus{}, err
		}
		metadata = raw
	}
	event, err := eventbus.NewEvent(statemachine.TopicTask, statemachine.EventUserInputReceived, statemachine.UserInputPayload{
		RequestID: requestID, Answer: answer, Metadata: metadata,
	}, eventbus.WithTaskID(runID), eventbus.WithSource(s.source(s.ActiveAgentName())))
	if err != nil {
		return projection.RunStatus{}, err
	}
	if err := queue.EnqueueEvent(ctx, event); err != nil {
		return projection.RunStatus{}, err
	}
	return s.project(ctx, storage, queue, runID, projection.StatusUpdate{
		AdvanceStatus: StatusEventEnqueued, EventType: string(event.Type),
	})
}

func (s *Service) project(ctx context.Context, storage persistence.RuntimeStorage, queue *persistence.WorkQueue, runID string, update projection.StatusUpdate) (projection.RunStatus, error) {
	return (projection.Projector{DefaultWorkDir: s.builder.DefaultWorkDir()}).Project(ctx, storage, queue, runID, update)
}

func (s *Service) source(agentName string) string {
	agentName = strings.TrimSpace(agentName)
	if agentName == "" {
		return "app"
	}
	return "app." + agentName
}

func IsTerminalPhase(phase string) bool {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

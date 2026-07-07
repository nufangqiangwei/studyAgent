package app

import (
	"agent/internal/content"
	"agent/internal/foundation/llmClient"
	"agent/internal/foundation/policy"
	appruntime "agent/internal/runtime"
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/agents/builtinagents"
	eventbus2 "agent/internal/runtime/eventbus"
	"agent/internal/runtime/persistence"
	reactor2 "agent/internal/runtime/reactor"
	statemachine2 "agent/internal/runtime/statemachine"
	runtimetools "agent/internal/runtime/tools"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	systemIO "io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	analyzeAgentName     = builtinagents.AnalyzeAgentName
	defaultAgentName     = builtinagents.DefaultAgentName
	toolsTesterAgentName = builtinagents.ToolsTesterAgentName

	asyncStatusEventEnqueued    = "event_enqueued"
	asyncStatusEventProcessed   = "event_processed"
	asyncStatusEffectDispatched = "effect_dispatched"
	asyncStatusIdle             = "idle"
)

type appLLMClient interface {
	Complete(ctx context.Context, req llmClient.Request) (llmClient.Response, error)
}

type AppAgentRunnerOptions struct {
	LLM              appLLMClient
	Model            string
	Logger           content.Logger
	MaxSteps         int
	WorkDir          string
	In               systemIO.Reader
	Out              systemIO.Writer
	RuntimeStoreRoot string
	Policy           policy.Checker
}

type AppAgentRunner struct {
	ctx     context.Context
	opts    AppAgentRunnerOptions
	current string
}

func newAppAgentRunner(ctx context.Context, initialName string, opts AppAgentRunnerOptions) (*AppAgentRunner, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runner := &AppAgentRunner{
		ctx:  ctx,
		opts: opts,
	}
	if err := runner.SelectAgent(initialName); err != nil {
		return nil, err
	}
	return runner, nil
}

func availableAgentNames() []string {
	return []string{analyzeAgentName, defaultAgentName, toolsTesterAgentName}
}

func (s *AppAgentRunner) Run(ctx context.Context, task string) error {
	status, err := s.Submit(ctx, task)
	if err != nil {
		return err
	}
	for i := 0; i < maxInt(s.opts.MaxSteps*4, 80); i++ {
		if isTerminalPhase(status.Phase) {
			if s.opts.Out != nil && strings.TrimSpace(status.FinalAnswer) != "" {
				_, err := fmt.Fprintln(s.opts.Out, status.FinalAnswer)
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
	return fmt.Errorf("app agent runner: synchronous run reached step limit for %s", status.RunID)
}

func (s *AppAgentRunner) Submit(ctx context.Context, task string) (content.AsyncRunStatus, error) {
	if s == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("app agent runner is nil")
	}
	task = strings.TrimSpace(task)
	if task == "" {
		return content.AsyncRunStatus{}, fmt.Errorf("app agent runner: task is required")
	}
	taskID, err := newAppID("task")
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	setup, err := s.setupRuntime(ctx, taskID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	defer setup.close()

	event, err := eventbus2.NewEvent(statemachine2.TopicTask, statemachine2.EventTaskStartRequested, statemachine2.TaskStartPayload{
		Agent: setup.taskRuntime.AgentName(),
		Input: task,
		Metadata: map[string]string{
			"entry":          "app",
			"input":          task,
			"selected_agent": s.current,
			"work_dir":       setup.workDir,
		},
	}, eventbus2.WithTaskID(taskID), eventbus2.WithSource(s.source()))
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	if err := setup.queue.EnqueueEvent(ctx, event); err != nil {
		return content.AsyncRunStatus{}, err
	}
	return s.status(ctx, setup.storage, setup.queue, taskID, statusUpdate{
		AdvanceStatus: asyncStatusEventEnqueued,
		EventType:     string(event.Type),
	})
}

func (s *AppAgentRunner) Recover(ctx context.Context) (content.AsyncRecoverResult, error) {
	storage, queue, closeStore, err := s.openStorage(ctx)
	if err != nil {
		return content.AsyncRecoverResult{}, err
	}
	defer closeStore()

	runtimes, err := storage.Runtimes().List(ctx)
	if err != nil {
		return content.AsyncRecoverResult{}, err
	}
	statuses := make([]content.AsyncRunStatus, 0, len(runtimes))
	for _, snapshot := range runtimes {
		status, err := s.status(ctx, storage, queue, snapshot.TaskID, statusUpdate{})
		if err != nil {
			return content.AsyncRecoverResult{}, err
		}
		if isTerminalPhase(status.Phase) && status.PendingEvents == 0 && status.PendingEffects == 0 {
			continue
		}
		statuses = append(statuses, status)
	}
	return content.AsyncRecoverResult{Runs: statuses}, nil
}

func (s *AppAgentRunner) Work(ctx context.Context) (content.AsyncWorkResult, error) {
	_, queue, closeStore, err := s.openStorage(ctx)
	if err != nil {
		return content.AsyncWorkResult{}, err
	}
	defer closeStore()

	work, ok, err := queue.Next(ctx)
	if err != nil {
		return content.AsyncWorkResult{}, err
	}
	if !ok {
		return content.AsyncWorkResult{}, nil
	}
	var status content.AsyncRunStatus
	switch work.Kind {
	case queuedWorkEvent:
		status, err = s.Advance(ctx, work.TaskID)
	case queuedWorkEffect:
		status, err = s.DispatchNextEffect(ctx, work.TaskID)
	default:
		err = fmt.Errorf("app agent runner: unsupported queued work kind %q", work.Kind)
	}
	if err != nil {
		return content.AsyncWorkResult{}, err
	}
	return content.AsyncWorkResult{Ran: true, Status: status}, nil
}

func (s *AppAgentRunner) Advance(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return content.AsyncRunStatus{}, fmt.Errorf("advance: run id is required")
	}
	setup, err := s.setupRuntime(ctx, runID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	defer setup.close()

	event, ok, err := setup.queue.PopEvent(ctx, runID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	if !ok {
		return s.status(ctx, setup.storage, setup.queue, runID, statusUpdate{AdvanceStatus: asyncStatusIdle})
	}
	if _, err := setup.runtime.Publish(ctx, event); err != nil {
		return content.AsyncRunStatus{}, err
	}
	return s.status(ctx, setup.storage, setup.queue, runID, statusUpdate{
		AdvanceStatus: asyncStatusEventProcessed,
		EventType:     string(event.Type),
	})
}

func (s *AppAgentRunner) DispatchNextEffect(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return content.AsyncRunStatus{}, fmt.Errorf("effect: run id is required")
	}
	setup, err := s.setupRuntime(ctx, runID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	defer setup.close()

	effect, ok, err := setup.queue.PopEffect(ctx, runID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	if !ok {
		return s.status(ctx, setup.storage, setup.queue, runID, statusUpdate{AdvanceStatus: asyncStatusIdle})
	}
	produced, err := setup.executeEffect(ctx, effect)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return s.status(ctx, setup.storage, setup.queue, runID, statusUpdate{
		AdvanceStatus:      asyncStatusEffectDispatched,
		EffectType:         string(effect.Type),
		ProducedEventTypes: produced,
	})
}

func (s *AppAgentRunner) SubmitUserInput(ctx context.Context, runID string, answer string) (content.AsyncRunStatus, error) {
	return s.submitUserEvent(ctx, runID, strings.TrimSpace(answer), "")
}

func (s *AppAgentRunner) SubmitUserApproval(ctx context.Context, runID string, approved bool, reason string) (content.AsyncRunStatus, error) {
	answer := "no"
	if approved {
		answer = "yes"
	}
	if strings.TrimSpace(reason) != "" {
		answer += ": " + strings.TrimSpace(reason)
	}
	return s.submitUserEvent(ctx, runID, answer, "approval")
}

func (s *AppAgentRunner) Result(ctx context.Context, runID string) (content.AsyncRunStatus, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return content.AsyncRunStatus{}, fmt.Errorf("result: run id is required")
	}
	storage, queue, closeStore, err := s.openStorage(ctx)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	defer closeStore()
	return s.status(ctx, storage, queue, runID, statusUpdate{})
}

func (s *AppAgentRunner) ActiveAgentName() string {
	if s == nil {
		return ""
	}
	return s.current
}

func (s *AppAgentRunner) ListAgentNames() []string {
	return availableAgentNames()
}

func (s *AppAgentRunner) SelectAgent(name string) error {
	if s == nil {
		return fmt.Errorf("app agent runner is nil")
	}
	canonical, err := canonicalAgentName(name)
	if err != nil {
		return err
	}
	s.current = canonical
	return nil
}

func (s *AppAgentRunner) submitUserEvent(ctx context.Context, runID string, answer string, reason string) (content.AsyncRunStatus, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return content.AsyncRunStatus{}, fmt.Errorf("input: run id is required")
	}
	storage, queue, closeStore, err := s.openStorage(ctx)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	defer closeStore()

	requestID := ""
	if state, ok, err := storage.TaskStates().Load(ctx, runID); err != nil {
		return content.AsyncRunStatus{}, err
	} else if ok && state.PendingUserInput != nil {
		requestID = state.PendingUserInput.RequestID
	}
	if requestID == "" {
		requestID, err = newAppID("input")
		if err != nil {
			return content.AsyncRunStatus{}, err
		}
	}
	metadata := json.RawMessage(nil)
	if reason != "" {
		raw, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			return content.AsyncRunStatus{}, err
		}
		metadata = raw
	}
	event, err := eventbus2.NewEvent(statemachine2.TopicTask, statemachine2.EventUserInputReceived, statemachine2.UserInputPayload{
		RequestID: requestID,
		Answer:    answer,
		Metadata:  metadata,
	}, eventbus2.WithTaskID(runID), eventbus2.WithSource(s.source()))
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	if err := queue.EnqueueEvent(ctx, event); err != nil {
		return content.AsyncRunStatus{}, err
	}
	return s.status(ctx, storage, queue, runID, statusUpdate{
		AdvanceStatus: asyncStatusEventEnqueued,
		EventType:     string(event.Type),
	})
}

type runtimeSetup struct {
	runtime     *appruntime.Runtime
	storage     *persistence.LocalStore
	queue       *asyncWorkQueue
	taskRuntime *appruntime.TaskRuntime
	workDir     string
}

func (s *AppAgentRunner) setupRuntime(ctx context.Context, taskID string) (*runtimeSetup, error) {
	storage, queue, closeStore, err := s.openStorage(ctx)
	if err != nil {
		return nil, err
	}
	closed := false
	closeOnError := func() {
		if !closed {
			closed = true
			closeStore()
		}
	}

	workDir := s.workDirForTask(ctx, storage, taskID)
	env := content.Env{
		IO: content.IO{
			In:  s.opts.In,
			Out: s.opts.Out,
		},
		Config: content.Config{
			Model:     s.opts.Model,
			AgentName: s.current,
			WorkDir:   workDir,
		},
	}
	toolAdapter, err := runtimetools.NewDefault(
		runtimetools.WithPolicy(s.opts.Policy),
		runtimetools.WithWorkDir(workDir),
		runtimetools.WithEnv(env),
		runtimetools.WithSource(s.source()+".tools"),
	)
	if err != nil {
		closeOnError()
		return nil, err
	}
	modelExecutor, err := agents2.NewModelExecutor(&appRunnerModelAdapter{client: s.opts.LLM}, agents2.WithModelExecutorSource(s.source()+".model"))
	if err != nil {
		closeOnError()
		return nil, err
	}
	rt, err := appruntime.New(
		appruntime.WithOwnedStorage(storage),
		appruntime.WithSource(s.source()),
		appruntime.WithEffectDispatcher(queueEffectDispatcher{queue: queue}),
		appruntime.WithResultDelivery(eventbus2.DeliverySync),
		appruntime.WithEffectExecutor(reactor2.EffectModelCall, modelExecutor),
		appruntime.WithEffectExecutor(reactor2.EffectToolDispatch, toolAdapter),
		appruntime.WithEffectExecutor(reactor2.EffectUserInputRequest, userInputRequestExecutor{}),
	)
	if err != nil {
		closeOnError()
		return nil, err
	}
	agent, err := s.newRuntimeAgent(workDir, rt.SnapshotStore(), toolAdapter.Specs())
	if err != nil {
		_ = rt.Close()
		return nil, err
	}
	taskRuntime, err := rt.CreateTaskRuntime(ctx, taskID, agent)
	if err != nil {
		_ = rt.Close()
		return nil, err
	}
	closed = true
	return &runtimeSetup{
		runtime:     rt,
		storage:     storage,
		queue:       queue,
		taskRuntime: taskRuntime,
		workDir:     workDir,
	}, nil
}

func (s *AppAgentRunner) newRuntimeAgent(workDir string, snapshots agents2.SnapshotStore, tools []agents2.ToolSpec) (agents2.Agent, error) {
	maxTurns := s.opts.MaxSteps
	if maxTurns <= 0 {
		maxTurns = 20
	}
	options := []builtinagents.AgentOption{
		builtinagents.WithModelName(s.opts.Model),
		builtinagents.WithSnapshotStore(snapshots),
		builtinagents.WithTools(tools),
		builtinagents.WithSystemPrompt(decisionSystemPrompt(workDir)),
		builtinagents.WithAgentSource(s.source() + ".agent"),
		builtinagents.WithMaxTurns(maxTurns),
	}
	switch runtimeAgentName(s.current) {
	case defaultAgentName:
		return builtinagents.NewDefaultAgent(options...)
	case toolsTesterAgentName:
		return builtinagents.NewToolsTesterAgent(options...)
	default:
		return builtinagents.NewAnalyzeAgent(options...)
	}
}

func (s *AppAgentRunner) openStorage(_ context.Context) (*persistence.LocalStore, *asyncWorkQueue, func(), error) {
	root := strings.TrimSpace(s.opts.RuntimeStoreRoot)
	if root == "" {
		return nil, nil, nil, fmt.Errorf("app agent runner: runtime store root is required")
	}
	storage, err := persistence.NewFileStore(root)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create runtime storage: %w", err)
	}
	queue, err := newAsyncWorkQueue(filepath.Join(root, "queue"))
	if err != nil {
		_ = storage.Close()
		return nil, nil, nil, err
	}
	return storage, queue, func() { _ = storage.Close() }, nil
}

func (s *AppAgentRunner) workDirForTask(ctx context.Context, storage *persistence.LocalStore, taskID string) string {
	if storage != nil && storage.TaskStates() != nil {
		state, ok, err := storage.TaskStates().Load(ctx, taskID)
		if err == nil && ok {
			if workDir := strings.TrimSpace(state.Metadata["work_dir"]); workDir != "" {
				return workDir
			}
		}
	}
	return strings.TrimSpace(s.opts.WorkDir)
}

func (s *AppAgentRunner) status(ctx context.Context, storage *persistence.LocalStore, queue *asyncWorkQueue, runID string, update statusUpdate) (content.AsyncRunStatus, error) {
	status := content.AsyncRunStatus{
		RunID:         runID,
		AdvanceStatus: update.AdvanceStatus,
		EventType:     update.EventType,
		EffectType:    update.EffectType,
	}
	status.ProducedEventTypes = append(status.ProducedEventTypes, update.ProducedEventTypes...)

	state, ok, err := storage.TaskStates().Load(ctx, runID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	if ok {
		status.Phase = strings.ToLower(string(state.Phase))
		status.FinalAnswer = resultText(state.Result)
		status.WorkDir = strings.TrimSpace(state.Metadata["work_dir"])
		status.WaitingReason, status.WaitingTarget = waitingStatus(state)
		if state.LastError != nil {
			status.Error = state.LastError.Message
			if status.Error == "" {
				status.Error = state.LastError.Code
			}
		}
		if snapshot, found, err := storage.AgentSnapshots().Load(ctx, state.Agent.Name, runID); err == nil && found {
			status.StepsUsed = snapshot.StepIndex
		}
	} else {
		status.Phase = strings.ToLower(string(statemachine2.PhaseCreated))
	}
	if status.WorkDir == "" {
		status.WorkDir = strings.TrimSpace(s.opts.WorkDir)
	}
	pendingEvents, pendingEffects, err := queue.PendingCounts(ctx, runID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	status.PendingEvents = pendingEvents
	status.PendingEffects = pendingEffects
	return status, nil
}

func (s *AppAgentRunner) source() string {
	if s == nil {
		return "app"
	}
	name := strings.TrimSpace(s.current)
	if name == "" {
		name = analyzeAgentName
	}
	return "app." + name
}

func (s *runtimeSetup) close() {
	if s != nil && s.runtime != nil {
		_ = s.runtime.Close()
	}
}

func (s *runtimeSetup) executeEffect(ctx context.Context, effect reactor2.Effect) ([]string, error) {
	if s == nil || s.runtime == nil {
		return nil, fmt.Errorf("runtime setup is nil")
	}
	taskRuntime, err := s.runtime.RuntimeRegistry().ResolveRuntime(ctx, eventbus2.Event{TaskID: effect.TaskID})
	if err != nil {
		return nil, err
	}
	executor, ok := s.runtime.ExecutorRegistry().Lookup(effect.Type)
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
		if event.Topic == statemachine2.TopicTask {
			if err := s.queue.EnqueueEvent(ctx, event); err != nil {
				return produced, err
			}
			continue
		}
		if _, err := s.runtime.Publish(ctx, event); err != nil {
			return produced, err
		}
	}
	return produced, nil
}

type queueEffectDispatcher struct {
	queue *asyncWorkQueue
}

func (d queueEffectDispatcher) DispatchEffect(ctx context.Context, request reactor2.EffectDispatchRequest) error {
	if request.OnDone != nil {
		defer request.OnDone()
	}
	if d.queue == nil {
		return fmt.Errorf("queue effect dispatcher: queue is required")
	}
	return d.queue.EnqueueEffect(ctx, request.Effect)
}

type userInputRequestExecutor struct{}

func (userInputRequestExecutor) ExecuteEffect(_ context.Context, _ reactor2.TaskRuntime, effect reactor2.Effect) (reactor2.EffectResult, error) {
	if effect.Type != reactor2.EffectUserInputRequest {
		return reactor2.EffectResult{}, fmt.Errorf("user input executor: unsupported effect type %q", effect.Type)
	}
	return reactor2.EffectResult{}, nil
}

type appRunnerModelAdapter struct {
	client appLLMClient
}

func (m *appRunnerModelAdapter) Complete(ctx context.Context, request agents2.ModelRequest) (agents2.ModelResponse, error) {
	if m == nil || m.client == nil {
		return agents2.ModelResponse{}, fmt.Errorf("app model client is not configured")
	}
	response, err := m.client.Complete(ctx, llmClient.Request{
		Model:       request.Model,
		Messages:    appRunnerLLMMessages(request.Messages),
		Tools:       appRunnerLLMTools(request.Tools),
		Temperature: request.Temperature,
		Metadata:    cloneStringMap(request.Metadata),
	})
	if err != nil {
		return agents2.ModelResponse{}, err
	}
	metadata := map[string]string{
		"provider": response.Provider,
		"model":    response.Model,
	}
	if len(response.ToolCalls) > 0 {
		call := response.ToolCalls[0]
		arguments := append(json.RawMessage(nil), call.Input...)
		if strings.TrimSpace(string(arguments)) == "" {
			arguments = json.RawMessage(`{}`)
		}
		if call.Name == "ask_user" {
			return agents2.ModelResponse{
				Content: strings.TrimSpace(response.Content),
				Decision: &agents2.Decision{
					Action: agents2.ActionAskUser,
					UserInput: &agents2.UserInputIntent{
						RequestID: call.ID,
						Prompt:    askUserPrompt(arguments),
					},
				},
				Metadata: metadata,
			}, nil
		}
		return agents2.ModelResponse{
			Content: strings.TrimSpace(response.Content),
			Decision: &agents2.Decision{
				Action: agents2.ActionUseTool,
				Tool: &agents2.ToolIntent{
					ToolCallID: call.ID,
					ToolName:   call.Name,
					Arguments:  arguments,
				},
			},
			Metadata: metadata,
		}, nil
	}
	content := strings.TrimSpace(response.Content)
	if content == "" {
		return agents2.ModelResponse{
			Decision: &agents2.Decision{Action: agents2.ActionComplete},
			Metadata: metadata,
		}, nil
	}
	modelResponse := agents2.ModelResponse{Content: content, Metadata: metadata}
	if _, err := modelResponse.ResolveDecision(); err == nil {
		return modelResponse, nil
	}
	modelResponse.Decision = &agents2.Decision{
		Action:      agents2.ActionComplete,
		FinalAnswer: content,
	}
	return modelResponse, nil
}

func appRunnerLLMMessages(messages []agents2.Message) []llmClient.Message {
	if len(messages) == 0 {
		return nil
	}
	out := make([]llmClient.Message, 0, len(messages))
	for _, message := range messages {
		role := appRunnerLLMRole(message.Role)
		content := message.Content
		name := ""
		toolCallID := ""
		if strings.TrimSpace(message.Role) == string(llmClient.RoleTool) {
			role = llmClient.RoleTool
			content, name, toolCallID = appRunnerToolObservation(message)
		} else if strings.TrimSpace(message.Role) == string(llmClient.RoleUser) && len(message.Data) > 0 {
			if toolContent, toolName, id, ok := appRunnerUserInputAsTool(message); ok {
				role = llmClient.RoleTool
				content = toolContent
				name = toolName
				toolCallID = id
			} else if strings.TrimSpace(content) == "" {
				content = string(message.Data)
			}
		} else if strings.TrimSpace(content) == "" && len(message.Data) > 0 {
			content = string(message.Data)
		}
		out = append(out, llmClient.Message{
			Role:       role,
			Content:    content,
			Name:       name,
			ToolCallID: toolCallID,
		})
	}
	return out
}

func appRunnerLLMRole(role string) llmClient.Role {
	switch strings.TrimSpace(role) {
	case string(llmClient.RoleSystem):
		return llmClient.RoleSystem
	case string(llmClient.RoleAssistant):
		return llmClient.RoleAssistant
	case string(llmClient.RoleTool):
		return llmClient.RoleTool
	default:
		return llmClient.RoleUser
	}
}

func appRunnerToolObservation(message agents2.Message) (string, string, string) {
	var payload statemachine2.ToolCallPayload
	if len(message.Data) > 0 && json.Unmarshal(message.Data, &payload) == nil && payload.ToolCallID != "" {
		if payload.Error != "" {
			return "error: " + payload.Error, payload.ToolName, payload.ToolCallID
		}
		if len(payload.Result) > 0 {
			return string(payload.Result), payload.ToolName, payload.ToolCallID
		}
		return "{}", payload.ToolName, payload.ToolCallID
	}
	if strings.TrimSpace(message.Content) != "" {
		return message.Content, "", ""
	}
	if len(message.Data) > 0 {
		return string(message.Data), "", ""
	}
	return "{}", "", ""
}

func appRunnerUserInputAsTool(message agents2.Message) (string, string, string, bool) {
	var payload statemachine2.UserInputPayload
	if len(message.Data) == 0 || json.Unmarshal(message.Data, &payload) != nil || payload.RequestID == "" {
		return "", "", "", false
	}
	if strings.TrimSpace(payload.Answer) == "" {
		return "", "", "", false
	}
	return payload.Answer, "ask_user", payload.RequestID, true
}

func askUserPrompt(arguments json.RawMessage) string {
	var input struct {
		Question string `json:"question"`
		Prompt   string `json:"prompt"`
	}
	if len(arguments) > 0 {
		_ = json.Unmarshal(arguments, &input)
	}
	prompt := strings.TrimSpace(input.Question)
	if prompt == "" {
		prompt = strings.TrimSpace(input.Prompt)
	}
	if prompt == "" {
		prompt = "Input requested"
	}
	return prompt
}

func appRunnerLLMTools(specs []agents2.ToolSpec) []llmClient.ToolDefinition {
	if len(specs) == 0 {
		return nil
	}
	out := make([]llmClient.ToolDefinition, 0, len(specs))
	for _, spec := range specs {
		out = append(out, llmClient.ToolDefinition{
			Name:        spec.Name,
			Description: spec.Description,
			InputSchema: append(json.RawMessage(nil), spec.InputSchema...),
		})
	}
	return out
}

type queuedWorkKind string

const (
	queuedWorkEvent  queuedWorkKind = "event"
	queuedWorkEffect queuedWorkKind = "effect"
)

type queuedWork struct {
	Kind   queuedWorkKind
	TaskID string
}

type asyncWorkQueue struct {
	mu         sync.Mutex
	eventPath  string
	effectPath string
}

type queuedEventRecord struct {
	ID        string          `json:"id"`
	TaskID    string          `json:"task_id"`
	Status    string          `json:"status"`
	WrittenAt time.Time       `json:"written_at"`
	Event     eventbus2.Event `json:"event,omitempty"`
}

type queuedEffectRecord struct {
	ID        string          `json:"id"`
	TaskID    string          `json:"task_id"`
	Status    string          `json:"status"`
	WrittenAt time.Time       `json:"written_at"`
	Effect    reactor2.Effect `json:"effect,omitempty"`
}

func newAsyncWorkQueue(root string) (*asyncWorkQueue, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("async queue: root is required")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create async queue %s: %w", root, err)
	}
	return &asyncWorkQueue{
		eventPath:  filepath.Join(root, "events.jsonl"),
		effectPath: filepath.Join(root, "effects.jsonl"),
	}, nil
}

func (q *asyncWorkQueue) EnqueueEvent(ctx context.Context, event eventbus2.Event) error {
	if q == nil {
		return fmt.Errorf("async queue is nil")
	}
	if strings.TrimSpace(event.ID) == "" {
		return fmt.Errorf("queued event id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return appendQueueRecord(ctx, q.eventPath, queuedEventRecord{
		ID:        event.ID,
		TaskID:    event.TaskID,
		Status:    "pending",
		WrittenAt: time.Now().UTC(),
		Event:     event.Clone(),
	})
}

func (q *asyncWorkQueue) EnqueueEffect(ctx context.Context, effect reactor2.Effect) error {
	if q == nil {
		return fmt.Errorf("async queue is nil")
	}
	if strings.TrimSpace(effect.ID) == "" {
		return fmt.Errorf("queued effect id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return appendQueueRecord(ctx, q.effectPath, queuedEffectRecord{
		ID:        effect.ID,
		TaskID:    effect.TaskID,
		Status:    "pending",
		WrittenAt: time.Now().UTC(),
		Effect:    effect.Clone(),
	})
}

func (q *asyncWorkQueue) PopEvent(ctx context.Context, taskID string) (eventbus2.Event, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	events, err := q.pendingEventsLocked(ctx, taskID)
	if err != nil || len(events) == 0 {
		return eventbus2.Event{}, false, err
	}
	event := events[0]
	if err := appendQueueRecord(ctx, q.eventPath, queuedEventRecord{
		ID:        event.ID,
		TaskID:    event.TaskID,
		Status:    "done",
		WrittenAt: time.Now().UTC(),
	}); err != nil {
		return eventbus2.Event{}, false, err
	}
	return event.Clone(), true, nil
}

func (q *asyncWorkQueue) PopEffect(ctx context.Context, taskID string) (reactor2.Effect, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	effects, err := q.pendingEffectsLocked(ctx, taskID)
	if err != nil || len(effects) == 0 {
		return reactor2.Effect{}, false, err
	}
	effect := effects[0]
	if err := appendQueueRecord(ctx, q.effectPath, queuedEffectRecord{
		ID:        effect.ID,
		TaskID:    effect.TaskID,
		Status:    "done",
		WrittenAt: time.Now().UTC(),
	}); err != nil {
		return reactor2.Effect{}, false, err
	}
	return effect.Clone(), true, nil
}

func (q *asyncWorkQueue) PendingCounts(ctx context.Context, taskID string) (int, int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	events, err := q.pendingEventsLocked(ctx, taskID)
	if err != nil {
		return 0, 0, err
	}
	effects, err := q.pendingEffectsLocked(ctx, taskID)
	if err != nil {
		return 0, 0, err
	}
	return len(events), len(effects), nil
}

func (q *asyncWorkQueue) Next(ctx context.Context) (queuedWork, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	events, err := q.pendingEventsLocked(ctx, "")
	if err != nil {
		return queuedWork{}, false, err
	}
	if len(events) > 0 {
		return queuedWork{Kind: queuedWorkEvent, TaskID: events[0].TaskID}, true, nil
	}
	effects, err := q.pendingEffectsLocked(ctx, "")
	if err != nil {
		return queuedWork{}, false, err
	}
	if len(effects) > 0 {
		return queuedWork{Kind: queuedWorkEffect, TaskID: effects[0].TaskID}, true, nil
	}
	return queuedWork{}, false, nil
}

func (q *asyncWorkQueue) pendingEventsLocked(ctx context.Context, taskID string) ([]eventbus2.Event, error) {
	records, err := readQueueRecords[queuedEventRecord](ctx, q.eventPath)
	if err != nil {
		return nil, err
	}
	pending := make(map[string]eventbus2.Event)
	order := make([]string, 0, len(records))
	for _, record := range records {
		if taskID != "" && record.TaskID != taskID {
			continue
		}
		if record.ID == "" {
			continue
		}
		if record.Status == "done" {
			delete(pending, record.ID)
			continue
		}
		if _, exists := pending[record.ID]; !exists {
			order = append(order, record.ID)
		}
		pending[record.ID] = record.Event.Clone()
	}
	out := make([]eventbus2.Event, 0, len(order))
	for _, id := range order {
		if event, ok := pending[id]; ok {
			out = append(out, event.Clone())
		}
	}
	return out, nil
}

func (q *asyncWorkQueue) pendingEffectsLocked(ctx context.Context, taskID string) ([]reactor2.Effect, error) {
	records, err := readQueueRecords[queuedEffectRecord](ctx, q.effectPath)
	if err != nil {
		return nil, err
	}
	pending := make(map[string]reactor2.Effect)
	order := make([]string, 0, len(records))
	for _, record := range records {
		if taskID != "" && record.TaskID != taskID {
			continue
		}
		if record.ID == "" {
			continue
		}
		if record.Status == "done" {
			delete(pending, record.ID)
			continue
		}
		if _, exists := pending[record.ID]; !exists {
			order = append(order, record.ID)
		}
		pending[record.ID] = record.Effect.Clone()
	}
	out := make([]reactor2.Effect, 0, len(order))
	for _, id := range order {
		if effect, ok := pending[id]; ok {
			out = append(out, effect.Clone())
		}
	}
	return out, nil
}

func appendQueueRecord(ctx context.Context, path string, record any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create queue directory for %s: %w", path, err)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal queue record: %w", err)
	}
	data = append(data, '\n')
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("open queue file %s: %w", path, err)
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write queue file %s: %w", path, err)
	}
	return nil
}

func readQueueRecords[T any](ctx context.Context, path string) ([]T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read queue file %s: %w", path, err)
	}
	lines := strings.Split(string(data), "\n")
	records := make([]T, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record T
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("parse queue file %s: %w", path, err)
		}
		records = append(records, record)
	}
	return records, nil
}

type statusUpdate struct {
	AdvanceStatus      string
	EventType          string
	EffectType         string
	ProducedEventTypes []string
}

func waitingStatus(state statemachine2.TaskState) (string, string) {
	switch state.Phase {
	case statemachine2.PhaseWaitingModel:
		if state.PendingModel != nil {
			return "model", state.PendingModel.ModelCallID
		}
		return "model", ""
	case statemachine2.PhaseWaitingTool:
		if state.PendingTool != nil {
			return "tool", state.PendingTool.ToolName
		}
		return "tool", ""
	case statemachine2.PhaseWaitingUserInput:
		if state.PendingUserInput != nil {
			return "user_input", state.PendingUserInput.RequestID
		}
		return "user_input", ""
	case statemachine2.PhaseWaitingSubAgent:
		if state.PendingSubAgent != nil {
			return "sub_agent", state.PendingSubAgent.SubTaskID
		}
		return "sub_agent", ""
	default:
		return "", ""
	}
}

func resultText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var envelope struct {
		Answer      string `json:"answer"`
		FinalAnswer string `json:"final_answer"`
		Summary     string `json:"summary"`
	}
	if err := json.Unmarshal(raw, &envelope); err == nil {
		switch {
		case strings.TrimSpace(envelope.Answer) != "":
			return envelope.Answer
		case strings.TrimSpace(envelope.FinalAnswer) != "":
			return envelope.FinalAnswer
		case strings.TrimSpace(envelope.Summary) != "":
			return envelope.Summary
		}
	}
	var pretty any
	if err := json.Unmarshal(raw, &pretty); err == nil {
		formatted, err := json.MarshalIndent(pretty, "", "  ")
		if err == nil {
			return string(formatted)
		}
	}
	return string(raw)
}

func decisionSystemPrompt(workDir string) string {
	return strings.TrimSpace(fmt.Sprintf(`You are an interactive CLI coding agent.
Workspace: %s

Return exactly one JSON object matching this decision protocol:
- To answer: {"action":"complete","final_answer":"..."}
- To use a tool: {"action":"use_tool","tool":{"tool_name":"read_file","arguments":{"path":"go.mod"}}}
- To ask the user: {"action":"ask_user","user_input":{"prompt":"..."}}
- To fail: {"action":"fail","error":"..."}

Use tools when repository context is needed. Ask the user only when required information is missing.
Do not include markdown outside the JSON object. Do not expose hidden reasoning or chain-of-thought.`, strings.TrimSpace(workDir)))
}

func canonicalAgentName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("app agent runner: agent name is required")
	}
	for _, candidate := range availableAgentNames() {
		if strings.EqualFold(candidate, name) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("agent %s not found", name)
}

func runtimeAgentName(name string) string {
	switch {
	case strings.EqualFold(strings.TrimSpace(name), defaultAgentName):
		return defaultAgentName
	case strings.EqualFold(strings.TrimSpace(name), toolsTesterAgentName):
		return toolsTesterAgentName
	default:
		return analyzeAgentName
	}
}

func isTerminalPhase(phase string) bool {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
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

func newAppID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("app agent runner: generate id: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	encoded := make([]byte, 32)
	hex.Encode(encoded, raw[:])
	id := fmt.Sprintf("%s-%s-%s-%s-%s",
		encoded[0:8],
		encoded[8:12],
		encoded[12:16],
		encoded[16:20],
		encoded[20:32],
	)
	if strings.TrimSpace(prefix) == "" {
		return id, nil
	}
	return prefix + "_" + id, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

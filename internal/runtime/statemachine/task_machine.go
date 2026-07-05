package statemachine

import (
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Clock func() time.Time

type TaskStateMachineOption func(*taskMachineConfig)

type taskMachineConfig struct {
	store       StateStore
	agentFlows  *AgentFlowRegistry
	clock       Clock
	maxFailures int
}

func WithStateStore(store StateStore) TaskStateMachineOption {
	return func(config *taskMachineConfig) {
		config.store = store
	}
}

func WithAgentFlows(flows *AgentFlowRegistry) TaskStateMachineOption {
	return func(config *taskMachineConfig) {
		config.agentFlows = flows
	}
}

func WithClock(clock Clock) TaskStateMachineOption {
	return func(config *taskMachineConfig) {
		config.clock = clock
	}
}

func WithMaxFailures(maxFailures int) TaskStateMachineOption {
	return func(config *taskMachineConfig) {
		config.maxFailures = maxFailures
	}
}

type TaskStateMachine struct {
	store       StateStore
	agentFlows  *AgentFlowRegistry
	clock       Clock
	maxFailures int
	handlers    map[eventbus.EventType]taskEventHandler
}

type taskEventHandler func(ctx context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error)

func NewTaskStateMachine(options ...TaskStateMachineOption) (*TaskStateMachine, error) {
	config := taskMachineConfig{
		store:      NewMemoryStateStore(),
		agentFlows: NewAgentFlowRegistry(),
		clock: func() time.Time {
			return time.Now().UTC()
		},
		maxFailures: 3,
	}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.store == nil {
		return nil, fmt.Errorf("task state machine: state store is required")
	}
	if config.agentFlows == nil {
		config.agentFlows = NewAgentFlowRegistry()
	}
	if config.clock == nil {
		return nil, fmt.Errorf("task state machine: clock is required")
	}
	if config.maxFailures < 0 {
		return nil, fmt.Errorf("task state machine: max failures must be >= 0")
	}

	machine := &TaskStateMachine{
		store:       config.store,
		agentFlows:  config.agentFlows,
		clock:       config.clock,
		maxFailures: config.maxFailures,
	}
	machine.handlers = map[eventbus.EventType]taskEventHandler{
		EventTaskCreated:             machine.handleTaskCreated,
		EventTaskStartRequested:      machine.handleTaskStartRequested,
		EventTaskCancelRequested:     machine.handleTaskCancelRequested,
		EventAgentModelRequested:     machine.handleAgentModelRequested,
		EventModelResponseReceived:   machine.handleModelResponseReceived,
		EventModelResponseFailed:     machine.handleModelResponseFailed,
		EventAgentToolRequested:      machine.handleAgentToolRequested,
		EventToolCompleted:           machine.handleToolCompleted,
		EventToolFailed:              machine.handleToolFailed,
		EventAgentUserInputRequested: machine.handleAgentUserInputRequested,
		EventUserInputReceived:       machine.handleUserInputReceived,
		EventAgentSubAgentRequested:  machine.handleAgentSubAgentRequested,
		EventSubAgentCompleted:       machine.handleSubAgentCompleted,
		EventSubAgentFailed:          machine.handleSubAgentFailed,
		EventAgentCompleted:          machine.handleAgentCompleted,
		EventAgentFailed:             machine.handleAgentFailed,
	}
	return machine, nil
}

func (m *TaskStateMachine) HandleEvent(ctx context.Context, event eventbus.Event) (reactor.StateResult, error) {
	if m == nil {
		return reactor.StateResult{}, fmt.Errorf("task state machine is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(event.TaskID) == "" {
		return reactor.StateResult{}, fmt.Errorf("task event %s: task_id is required", event.Type)
	}
	if strings.TrimSpace(string(event.Type)) == "" {
		return reactor.StateResult{}, fmt.Errorf("task event type is required")
	}

	state, exists, err := m.store.Load(ctx, event.TaskID)
	if err != nil {
		return reactor.StateResult{}, err
	}
	if !exists {
		state = NewTaskState(event.TaskID, m.now())
		state.MaxFailures = m.maxFailures
		if event.Type != EventTaskCreated && event.Type != EventTaskStartRequested {
			return reactor.StateResult{}, illegalEvent(state, event, "task state does not exist")
		}
	}
	if state.LastEventID != "" && state.LastEventID == event.ID {
		effects, err := m.replayEffects(ctx, state, event)
		if err != nil {
			return reactor.StateResult{}, err
		}
		return reactor.StateResult{
			TaskID:  state.TaskID,
			Effects: fillEffectTaskID(state.TaskID, effects),
		}, nil
	}

	previous := state.Clone()
	handler := m.handlers[event.Type]
	var effects []reactor.Effect
	if handler != nil {
		state, effects, err = handler(ctx, state, event)
	} else {
		state, effects, err = m.handleAgentFlow(ctx, state, event)
	}
	if err != nil {
		return reactor.StateResult{}, err
	}

	state.LastEventID = event.ID
	state.UpdatedAt = m.now()
	appendLifecycle(&state, previous, event, state.UpdatedAt)
	if err := m.store.Save(ctx, state); err != nil {
		return reactor.StateResult{}, err
	}
	effects = fillEffectTaskID(state.TaskID, effects)
	return reactor.StateResult{
		TaskID:  state.TaskID,
		Effects: effects,
	}, nil
}

func (m *TaskStateMachine) State(ctx context.Context, taskID string) (TaskState, bool, error) {
	if m == nil {
		return TaskState{}, false, fmt.Errorf("task state machine is nil")
	}
	return m.store.Load(ctx, taskID)
}

func (m *TaskStateMachine) handleTaskCreated(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseCreated || state.LastEventID != "" {
		return state, nil, illegalEvent(state, event, "task already exists")
	}
	payload, err := decodePayload[TaskCreatedPayload](event)
	if err != nil {
		return state, nil, err
	}
	m.applyCreationPayload(&state, payload.Agent, payload.MaxFailures, payload.Metadata)
	return state, nil, nil
}

func (m *TaskStateMachine) handleTaskStartRequested(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseCreated {
		return state, nil, illegalEvent(state, event, "task can only start from Created")
	}
	payload, err := decodePayload[TaskStartPayload](event)
	if err != nil {
		return state, nil, err
	}
	m.applyCreationPayload(&state, payload.Agent, payload.MaxFailures, payload.Metadata)
	state.Phase = PhaseRunning
	now := m.now()
	state.Agent.StartedAt = &now
	state.Agent.UpdatedAt = &now
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectAgentStart, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleTaskCancelRequested(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.IsTerminal() {
		return state, nil, illegalEvent(state, event, "terminal task cannot be cancelled")
	}
	state.Phase = PhaseCancelled
	state.PendingModel = nil
	state.PendingTool = nil
	state.PendingUserInput = nil
	state.PendingSubAgent = nil
	completedAt := m.now()
	state.CompletedAt = &completedAt
	effect, err := terminalEffect(state, EffectEmitTaskCancelled, nil)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleAgentModelRequested(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseRunning {
		return state, nil, illegalEvent(state, event, "model calls are only accepted while Running")
	}
	payload, err := decodePayload[ModelCallPayload](event)
	if err != nil {
		return state, nil, err
	}
	if strings.TrimSpace(payload.ModelCallID) == "" {
		return state, nil, fmt.Errorf("model call id is required")
	}
	if len(payload.Request) == 0 {
		return state, nil, fmt.Errorf("model request payload is required")
	}
	state.Phase = PhaseWaitingModel
	state.PendingModel = &PendingModelCall{
		ModelCallID: payload.ModelCallID,
		Agent:       payload.Agent,
		Request:     append(json.RawMessage(nil), payload.Request...),
		CreatedAt:   m.now(),
	}
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectModelCall, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleModelResponseReceived(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseWaitingModel {
		return state, nil, illegalEvent(state, event, "model response requires WaitingModel")
	}
	payload, err := decodePayload[ModelCallPayload](event)
	if err != nil {
		return state, nil, err
	}
	if err := validatePendingModel(state, payload.ModelCallID); err != nil {
		return state, nil, err
	}
	if len(payload.Response) == 0 {
		return state, nil, fmt.Errorf("model response payload is required")
	}
	state.Phase = PhaseRunning
	state.PendingModel = nil
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectAgentResume, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleModelResponseFailed(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseWaitingModel {
		return state, nil, illegalEvent(state, event, "model failure requires WaitingModel")
	}
	payload, err := decodePayload[ModelCallPayload](event)
	if err != nil {
		return state, nil, err
	}
	if err := validatePendingModel(state, payload.ModelCallID); err != nil {
		return state, nil, err
	}
	if strings.TrimSpace(payload.Error) == "" {
		return state, nil, fmt.Errorf("model failure error is required")
	}
	state.Phase = PhaseRunning
	state.PendingModel = nil
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectAgentResume, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleAgentToolRequested(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseRunning {
		return state, nil, illegalEvent(state, event, "tool calls are only accepted while Running")
	}
	payload, err := decodePayload[ToolCallPayload](event)
	if err != nil {
		return state, nil, err
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return state, nil, fmt.Errorf("tool call id is required")
	}
	state.Phase = PhaseWaitingTool
	state.PendingTool = &PendingToolCall{
		ToolCallID: payload.ToolCallID,
		ToolName:   payload.ToolName,
		Arguments:  append(json.RawMessage(nil), payload.Arguments...),
		CreatedAt:  m.now(),
	}
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectToolDispatch, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleToolCompleted(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseWaitingTool {
		return state, nil, illegalEvent(state, event, "tool completion requires WaitingTool")
	}
	payload, err := decodePayload[ToolCallPayload](event)
	if err != nil {
		return state, nil, err
	}
	if err := validatePendingTool(state, payload.ToolCallID); err != nil {
		return state, nil, err
	}
	state.Phase = PhaseRunning
	state.PendingTool = nil
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectAgentResume, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleToolFailed(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseWaitingTool {
		return state, nil, illegalEvent(state, event, "tool failure requires WaitingTool")
	}
	payload, err := decodePayload[ToolCallPayload](event)
	if err != nil {
		return state, nil, err
	}
	if err := validatePendingTool(state, payload.ToolCallID); err != nil {
		return state, nil, err
	}
	state.PendingTool = nil
	return m.handleRecoverableFailure(state, event, TaskError{Code: "tool_failed", Message: payload.Error}, payload)
}

func (m *TaskStateMachine) handleAgentUserInputRequested(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseRunning {
		return state, nil, illegalEvent(state, event, "user input requests are only accepted while Running")
	}
	payload, err := decodePayload[UserInputPayload](event)
	if err != nil {
		return state, nil, err
	}
	if strings.TrimSpace(payload.RequestID) == "" {
		return state, nil, fmt.Errorf("user input request id is required")
	}
	state.Phase = PhaseWaitingUserInput
	state.PendingUserInput = &PendingUserInput{
		RequestID: payload.RequestID,
		Prompt:    payload.Prompt,
		Metadata:  append(json.RawMessage(nil), payload.Metadata...),
		CreatedAt: m.now(),
	}
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectUserInputRequest, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleUserInputReceived(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseWaitingUserInput && state.Phase != PhaseRunning {
		return state, nil, illegalEvent(state, event, "user input requires Running or WaitingUserInput")
	}
	payload, err := decodePayload[UserInputPayload](event)
	if err != nil {
		return state, nil, err
	}
	if state.Phase == PhaseWaitingUserInput && state.PendingUserInput != nil && payload.RequestID != "" && state.PendingUserInput.RequestID != payload.RequestID {
		return state, nil, illegalEvent(state, event, "user input request id does not match pending request")
	}
	state.Phase = PhaseRunning
	state.PendingUserInput = nil
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectAgentResume, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleAgentSubAgentRequested(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseRunning {
		return state, nil, illegalEvent(state, event, "sub-agent requests are only accepted while Running")
	}
	payload, err := decodePayload[SubAgentPayload](event)
	if err != nil {
		return state, nil, err
	}
	if strings.TrimSpace(payload.SubTaskID) == "" {
		return state, nil, fmt.Errorf("sub task id is required")
	}
	state.Phase = PhaseWaitingSubAgent
	state.PendingSubAgent = &PendingSubAgent{
		SubTaskID: payload.SubTaskID,
		Agent:     payload.Agent,
		Input:     payload.Input,
		CreatedAt: m.now(),
	}
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectSubAgentDispatch, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleSubAgentCompleted(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseWaitingSubAgent {
		return state, nil, illegalEvent(state, event, "sub-agent completion requires WaitingSubAgent")
	}
	payload, err := decodePayload[SubAgentPayload](event)
	if err != nil {
		return state, nil, err
	}
	if err := validatePendingSubAgent(state, payload.SubTaskID); err != nil {
		return state, nil, err
	}
	state.Phase = PhaseRunning
	state.PendingSubAgent = nil
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectAgentResume, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleSubAgentFailed(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseWaitingSubAgent {
		return state, nil, illegalEvent(state, event, "sub-agent failure requires WaitingSubAgent")
	}
	payload, err := decodePayload[SubAgentPayload](event)
	if err != nil {
		return state, nil, err
	}
	if err := validatePendingSubAgent(state, payload.SubTaskID); err != nil {
		return state, nil, err
	}
	state.PendingSubAgent = nil
	return m.handleRecoverableFailure(state, event, TaskError{Code: "sub_agent_failed", Message: payload.Error}, payload)
}

func (m *TaskStateMachine) handleAgentCompleted(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseRunning {
		return state, nil, illegalEvent(state, event, "agent completion is only accepted while Running")
	}
	payload, err := decodePayload[AgentCompletedPayload](event)
	if err != nil {
		return state, nil, err
	}
	state.Phase = PhaseCompleted
	state.Result = append(json.RawMessage(nil), payload.Result...)
	completedAt := m.now()
	state.CompletedAt = &completedAt
	effect, err := terminalEffect(state, EffectEmitTaskCompleted, payload.Result)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) handleAgentFailed(_ context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseRunning && state.Phase != PhaseWaitingModel && state.Phase != PhaseWaitingTool && state.Phase != PhaseWaitingUserInput && state.Phase != PhaseWaitingSubAgent {
		return state, nil, illegalEvent(state, event, "agent failure requires an active task")
	}
	payload, err := decodePayload[AgentFailedPayload](event)
	if err != nil {
		return state, nil, err
	}
	taskErr := TaskError{Code: payload.Code, Message: payload.Message}
	if taskErr.Code == "" {
		taskErr.Code = "agent_failed"
	}
	if taskErr.Message == "" {
		taskErr.Message = "agent failed"
	}
	state.PendingModel = nil
	state.PendingTool = nil
	state.PendingUserInput = nil
	state.PendingSubAgent = nil
	return m.handleRecoverableFailure(state, event, taskErr, payload)
}

func (m *TaskStateMachine) handleAgentFlow(ctx context.Context, state TaskState, event eventbus.Event) (TaskState, []reactor.Effect, error) {
	if state.Phase != PhaseRunning {
		return state, nil, illegalEvent(state, event, "agent flow events are only accepted while Running")
	}
	flow, ok := m.agentFlows.Lookup(state.Agent.Name)
	if !ok {
		return state, nil, illegalEvent(state, event, fmt.Sprintf("no flow registered for agent %q", state.Agent.Name))
	}
	result, err := flow.HandleAgentEvent(ctx, state.Clone(), event)
	if err != nil {
		return state, nil, err
	}
	if !result.Handled {
		return state, nil, illegalEvent(state, event, "no transition matched")
	}
	if result.NextPhase != AgentPhaseUnknown {
		state.Agent.Phase = result.NextPhase
		now := m.now()
		state.Agent.UpdatedAt = &now
	}
	return state, result.Effects, nil
}

func (m *TaskStateMachine) replayEffects(_ context.Context, state TaskState, event eventbus.Event) ([]reactor.Effect, error) {
	switch event.Type {
	case EventTaskCreated:
		return nil, nil
	case EventTaskStartRequested:
		if state.Phase != PhaseRunning {
			return nil, nil
		}
		payload, err := decodePayload[TaskStartPayload](event)
		if err != nil {
			return nil, err
		}
		effect, err := reactor.NewEffect(state.TaskID, reactor.EffectAgentStart, payload)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventTaskCancelRequested:
		if state.Phase != PhaseCancelled {
			return nil, nil
		}
		effect, err := terminalEffect(state, EffectEmitTaskCancelled, nil)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventAgentModelRequested:
		if state.Phase != PhaseWaitingModel {
			return nil, nil
		}
		payload, err := replayModelCallPayload(state, event)
		if err != nil {
			return nil, err
		}
		effect, err := reactor.NewEffect(state.TaskID, reactor.EffectModelCall, payload)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventModelResponseReceived, EventModelResponseFailed:
		if state.Phase != PhaseRunning {
			return nil, nil
		}
		effect, err := replayAgentResumeEffect(state, event)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventAgentToolRequested:
		if state.Phase != PhaseWaitingTool {
			return nil, nil
		}
		payload, err := replayToolCallPayload(state, event)
		if err != nil {
			return nil, err
		}
		effect, err := reactor.NewEffect(state.TaskID, reactor.EffectToolDispatch, payload)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventToolCompleted, EventUserInputReceived, EventSubAgentCompleted:
		if state.Phase != PhaseRunning {
			return nil, nil
		}
		effect, err := replayAgentResumeEffect(state, event)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventToolFailed, EventSubAgentFailed, EventAgentFailed:
		if state.Phase == PhaseFailed {
			effect, err := terminalEffect(state, EffectEmitTaskFailed, nil)
			if err != nil {
				return nil, err
			}
			return []reactor.Effect{effect}, nil
		}
		if state.Phase != PhaseRunning {
			return nil, nil
		}
		effect, err := replayAgentResumeEffect(state, event)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventAgentUserInputRequested:
		if state.Phase != PhaseWaitingUserInput {
			return nil, nil
		}
		payload, err := replayUserInputPayload(state, event)
		if err != nil {
			return nil, err
		}
		effect, err := reactor.NewEffect(state.TaskID, reactor.EffectUserInputRequest, payload)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventAgentSubAgentRequested:
		if state.Phase != PhaseWaitingSubAgent {
			return nil, nil
		}
		payload, err := replaySubAgentPayload(state, event)
		if err != nil {
			return nil, err
		}
		effect, err := reactor.NewEffect(state.TaskID, reactor.EffectSubAgentDispatch, payload)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	case EventAgentCompleted:
		if state.Phase != PhaseCompleted {
			return nil, nil
		}
		effect, err := terminalEffect(state, EffectEmitTaskCompleted, state.Result)
		if err != nil {
			return nil, err
		}
		return []reactor.Effect{effect}, nil
	default:
		return nil, nil
	}
}

func (m *TaskStateMachine) applyCreationPayload(state *TaskState, agent string, maxFailures int, metadata map[string]string) {
	if strings.TrimSpace(agent) != "" {
		state.Agent.Name = strings.TrimSpace(agent)
	}
	if maxFailures > 0 {
		state.MaxFailures = maxFailures
	}
	if state.MaxFailures == 0 {
		state.MaxFailures = m.maxFailures
	}
	if len(metadata) > 0 {
		if state.Metadata == nil {
			state.Metadata = make(map[string]string, len(metadata))
		}
		for key, value := range metadata {
			state.Metadata[key] = value
		}
	}
	if flow, ok := m.agentFlows.Lookup(state.Agent.Name); ok && state.Agent.Phase == AgentPhaseUnknown {
		state.Agent.Phase = flow.InitialPhase()
	}
}

func (m *TaskStateMachine) handleRecoverableFailure(state TaskState, event eventbus.Event, taskErr TaskError, payload any) (TaskState, []reactor.Effect, error) {
	if taskErr.Message == "" {
		taskErr.Message = string(event.Type)
	}
	state.FailureCount++
	state.LastError = &taskErr
	if state.MaxFailures >= 0 && state.FailureCount > state.MaxFailures {
		state.Phase = PhaseFailed
		completedAt := m.now()
		state.CompletedAt = &completedAt
		effect, err := terminalEffect(state, EffectEmitTaskFailed, nil)
		if err != nil {
			return state, nil, err
		}
		return state, []reactor.Effect{effect}, nil
	}
	state.Phase = PhaseRunning
	effect, err := reactor.NewEffect(state.TaskID, reactor.EffectAgentResume, payload)
	if err != nil {
		return state, nil, err
	}
	return state, []reactor.Effect{effect}, nil
}

func (m *TaskStateMachine) now() time.Time {
	return m.clock().UTC()
}

func terminalEffect(state TaskState, effectType reactor.EffectType, result json.RawMessage) (reactor.Effect, error) {
	payload := TaskTerminalPayload{
		TaskID:       state.TaskID,
		Agent:        state.Agent.Name,
		AgentPhase:   state.Agent.Phase,
		FailureCount: state.FailureCount,
		Result:       append(json.RawMessage(nil), result...),
	}
	if state.LastError != nil {
		errState := *state.LastError
		payload.Error = &errState
	}
	return reactor.NewEffect(state.TaskID, effectType, payload)
}

func fillEffectTaskID(taskID string, effects []reactor.Effect) []reactor.Effect {
	if len(effects) == 0 {
		return nil
	}
	filled := make([]reactor.Effect, 0, len(effects))
	for _, effect := range effects {
		if effect.TaskID == "" {
			effect.TaskID = taskID
		}
		filled = append(filled, effect.Clone())
	}
	return filled
}

func validatePendingTool(state TaskState, toolCallID string) error {
	if state.PendingTool == nil {
		return fmt.Errorf("pending tool call is required")
	}
	if toolCallID != "" && state.PendingTool.ToolCallID != toolCallID {
		return fmt.Errorf("tool call %q does not match pending tool call %q", toolCallID, state.PendingTool.ToolCallID)
	}
	return nil
}

func validatePendingModel(state TaskState, modelCallID string) error {
	if state.PendingModel == nil {
		return fmt.Errorf("pending model call is required")
	}
	if modelCallID != "" && state.PendingModel.ModelCallID != modelCallID {
		return fmt.Errorf("model call %q does not match pending model call %q", modelCallID, state.PendingModel.ModelCallID)
	}
	return nil
}

func validatePendingSubAgent(state TaskState, subTaskID string) error {
	if state.PendingSubAgent == nil {
		return fmt.Errorf("pending sub-agent is required")
	}
	if subTaskID != "" && state.PendingSubAgent.SubTaskID != subTaskID {
		return fmt.Errorf("sub task %q does not match pending sub task %q", subTaskID, state.PendingSubAgent.SubTaskID)
	}
	return nil
}

func replayModelCallPayload(state TaskState, event eventbus.Event) (ModelCallPayload, error) {
	payload, err := decodePayload[ModelCallPayload](event)
	if err != nil {
		return ModelCallPayload{}, err
	}
	if payload.ModelCallID == "" && state.PendingModel != nil {
		payload.ModelCallID = state.PendingModel.ModelCallID
		payload.Agent = state.PendingModel.Agent
		payload.Request = append(json.RawMessage(nil), state.PendingModel.Request...)
	}
	return payload, nil
}

func replayToolCallPayload(state TaskState, event eventbus.Event) (ToolCallPayload, error) {
	payload, err := decodePayload[ToolCallPayload](event)
	if err != nil {
		return ToolCallPayload{}, err
	}
	if payload.ToolCallID == "" && state.PendingTool != nil {
		payload.ToolCallID = state.PendingTool.ToolCallID
		payload.ToolName = state.PendingTool.ToolName
		payload.Arguments = append(json.RawMessage(nil), state.PendingTool.Arguments...)
	}
	return payload, nil
}

func replayUserInputPayload(state TaskState, event eventbus.Event) (UserInputPayload, error) {
	payload, err := decodePayload[UserInputPayload](event)
	if err != nil {
		return UserInputPayload{}, err
	}
	if payload.RequestID == "" && state.PendingUserInput != nil {
		payload.RequestID = state.PendingUserInput.RequestID
		payload.Prompt = state.PendingUserInput.Prompt
		payload.Metadata = append(json.RawMessage(nil), state.PendingUserInput.Metadata...)
	}
	return payload, nil
}

func replaySubAgentPayload(state TaskState, event eventbus.Event) (SubAgentPayload, error) {
	payload, err := decodePayload[SubAgentPayload](event)
	if err != nil {
		return SubAgentPayload{}, err
	}
	if payload.SubTaskID == "" && state.PendingSubAgent != nil {
		payload.SubTaskID = state.PendingSubAgent.SubTaskID
		payload.Agent = state.PendingSubAgent.Agent
		payload.Input = state.PendingSubAgent.Input
	}
	return payload, nil
}

func replayAgentResumeEffect(state TaskState, event eventbus.Event) (reactor.Effect, error) {
	return reactor.NewEffect(state.TaskID, reactor.EffectAgentResume, append(json.RawMessage(nil), event.Payload...))
}

func appendLifecycle(state *TaskState, previous TaskState, event eventbus.Event, at time.Time) {
	if previous.Phase == state.Phase && previous.Agent.Phase == state.Agent.Phase {
		return
	}
	state.Lifecycle = append(state.Lifecycle, LifecycleRecord{
		Time:               at,
		EventID:            event.ID,
		EventType:          event.Type,
		PreviousPhase:      previous.Phase,
		NextPhase:          state.Phase,
		PreviousAgentPhase: previous.Agent.Phase,
		NextAgentPhase:     state.Agent.Phase,
	})
}

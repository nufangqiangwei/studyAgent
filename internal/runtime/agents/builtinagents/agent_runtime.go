package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const defaultMaxTurns = 20

type agentRuntimeDefaults struct {
	name        string
	source      string
	errorPrefix string
}

type agentRuntime struct {
	name         string
	model        agents2.ModelClient
	modelName    string
	temperature  float64
	store        agents2.SnapshotStore
	tools        []agents2.ToolSpec
	systemPrompt string
	source       string
	clock        func() time.Time
	maxTurns     int
	errorPrefix  string
}

func newAgentRuntime(defaults agentRuntimeDefaults, options ...AgentOption) (*agentRuntime, error) {
	errorPrefix := strings.TrimSpace(defaults.errorPrefix)
	if errorPrefix == "" {
		errorPrefix = "agent"
	}
	config := agentConfig{
		name:   strings.TrimSpace(defaults.name),
		store:  agents2.NewMemorySnapshotStore(),
		source: strings.TrimSpace(defaults.source),
		clock: func() time.Time {
			return time.Now().UTC()
		},
		maxTurns: defaultMaxTurns,
	}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.store == nil {
		return nil, fmt.Errorf("%s: snapshot store is required", errorPrefix)
	}
	if config.clock == nil {
		return nil, fmt.Errorf("%s: clock is required", errorPrefix)
	}
	if config.maxTurns <= 0 {
		config.maxTurns = defaultMaxTurns
	}
	name := strings.TrimSpace(config.name)
	if name == "" {
		name = strings.TrimSpace(defaults.name)
	}
	if name == "" {
		return nil, fmt.Errorf("%s: name is required", errorPrefix)
	}
	source := strings.TrimSpace(config.source)
	if source == "" {
		source = strings.TrimSpace(defaults.source)
	}
	return &agentRuntime{
		name:         name,
		model:        config.model,
		modelName:    strings.TrimSpace(config.modelName),
		temperature:  config.temperature,
		store:        config.store,
		tools:        cloneToolSpecs(config.tools),
		systemPrompt: config.systemPrompt,
		source:       source,
		clock:        config.clock,
		maxTurns:     config.maxTurns,
		errorPrefix:  errorPrefix,
	}, nil
}

func (r *agentRuntime) agentName() string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.name)
}

func (r *agentRuntime) start(ctx context.Context, input agents2.AgentStartInput) (agents2.AgentResult, error) {
	if r == nil {
		return agents2.AgentResult{}, fmt.Errorf("agent runtime is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(input.TaskID) == "" {
		return agents2.AgentResult{}, fmt.Errorf("%s start: task_id is required", r.errorPrefix)
	}
	if strings.TrimSpace(input.Input) == "" {
		return agents2.AgentResult{}, fmt.Errorf("%s start: input is required", r.errorPrefix)
	}

	now := r.now()
	snapshot := agents2.NewAgentSnapshot(r.agentName(), input, now)
	if r.systemPrompt != "" {
		snapshot.Messages = append(snapshot.Messages, agents2.Message{Role: "system", Content: r.systemPrompt})
	}
	snapshot.Messages = append(snapshot.Messages, agents2.Message{Role: "user", Content: input.Input})
	return r.requestDecision(ctx, "start", input.Input, snapshot)
}

func (r *agentRuntime) resume(ctx context.Context, input agents2.AgentResumeInput) (agents2.AgentResult, error) {
	if r == nil {
		return agents2.AgentResult{}, fmt.Errorf("agent runtime is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(input.TaskID) == "" {
		return agents2.AgentResult{}, fmt.Errorf("%s resume: task_id is required", r.errorPrefix)
	}
	snapshot, ok, err := r.store.Load(ctx, r.agentName(), input.TaskID)
	if err != nil {
		return agents2.AgentResult{}, err
	}
	if !ok {
		return agents2.AgentResult{}, fmt.Errorf("%s resume: snapshot for task %q not found", r.errorPrefix, input.TaskID)
	}
	if modelResponse, ok, err := decodeModelResponse(input.Payload); ok || err != nil {
		if err != nil {
			return agents2.AgentResult{}, err
		}
		return r.handleModelResponse(ctx, snapshot, modelResponse)
	}
	if modelFailure, ok, err := decodeModelFailure(input.Payload); ok || err != nil {
		if err != nil {
			return agents2.AgentResult{}, err
		}
		return r.failFromError(ctx, snapshot, "model_error", fmt.Errorf("%s", modelFailure.Error))
	}
	trigger := r.applyObservation(&snapshot, input)
	return r.requestDecision(ctx, trigger, "", snapshot)
}

func (r *agentRuntime) snapshot(ctx context.Context, taskID string) (agents2.AgentSnapshot, bool, error) {
	if r == nil {
		return agents2.AgentSnapshot{}, false, fmt.Errorf("agent runtime is nil")
	}
	return r.store.Load(ctx, r.agentName(), taskID)
}

func (r *agentRuntime) requestDecision(ctx context.Context, trigger string, input string, snapshot agents2.AgentSnapshot) (agents2.AgentResult, error) {
	if snapshot.StepIndex >= r.maxTurns {
		snapshot.FailureCount++
		snapshot.LastError = "max agent turns reached"
		snapshot.Phase = agents2.BusinessPhaseFailed
		snapshot.UpdatedAt = r.now()
		event, err := r.agentFailedEvent(snapshot.TaskID, "max_turns_reached", snapshot.LastError)
		if err != nil {
			return agents2.AgentResult{}, err
		}
		if err := r.store.Save(ctx, snapshot); err != nil {
			return agents2.AgentResult{}, err
		}
		return agents2.AgentResult{TaskID: snapshot.TaskID, Agent: r.agentName(), Snapshot: snapshot.Clone(), Events: []eventbus.Event{event}}, nil
	}

	modelCallID, err := newID("model_call")
	if err != nil {
		return agents2.AgentResult{}, err
	}
	snapshot.Phase = agents2.BusinessPhaseCallingModel
	snapshot.PendingModelCallID = modelCallID
	snapshot.UpdatedAt = r.now()
	request := agents2.ModelRequest{
		ModelCallID: modelCallID,
		TaskID:      snapshot.TaskID,
		Agent:       r.agentName(),
		Model:       r.modelName,
		Temperature: r.temperature,
		Trigger:     trigger,
		Input:       input,
		Messages:    cloneMessages(snapshot.Messages),
		Snapshot:    snapshot.Clone(),
		Tools:       cloneToolSpecs(r.tools),
		Metadata:    cloneStringMap(snapshot.Metadata),
	}
	rawRequest, err := agents2.MarshalModelRequest(request)
	if err != nil {
		return agents2.AgentResult{}, err
	}
	event, err := r.newTaskEvent(statemachine.EventAgentModelRequested, snapshot.TaskID, statemachine.ModelCallPayload{
		ModelCallID: modelCallID,
		Agent:       r.agentName(),
		Request:     rawRequest,
	})
	if err != nil {
		return agents2.AgentResult{}, err
	}
	if err := r.store.Save(ctx, snapshot); err != nil {
		return agents2.AgentResult{}, err
	}
	return agents2.AgentResult{
		TaskID:   snapshot.TaskID,
		Agent:    r.agentName(),
		Snapshot: snapshot.Clone(),
		Events:   []eventbus.Event{event},
	}, nil
}

func (r *agentRuntime) handleModelResponse(ctx context.Context, snapshot agents2.AgentSnapshot, modelResponse modelResponseObservation) (agents2.AgentResult, error) {
	if modelResponse.ModelCallID != "" && snapshot.PendingModelCallID != "" && modelResponse.ModelCallID != snapshot.PendingModelCallID {
		return agents2.AgentResult{}, fmt.Errorf("model call %q does not match pending model call %q", modelResponse.ModelCallID, snapshot.PendingModelCallID)
	}
	snapshot.PendingModelCallID = ""
	response := modelResponse.Response
	decision, err := response.ResolveDecision()
	if err != nil {
		return r.failFromError(ctx, snapshot, "invalid_model_decision", err)
	}
	if err := decision.Validate(); err != nil {
		return r.failFromError(ctx, snapshot, "invalid_agent_decision", err)
	}

	event, updated, err := r.applyDecision(snapshot, decision)
	if err != nil {
		return agents2.AgentResult{}, err
	}
	updated.StepIndex++
	updated.UpdatedAt = r.now()
	if strings.TrimSpace(response.Content) != "" {
		updated.Messages = append(updated.Messages, agents2.Message{Role: "assistant", Content: response.Content})
	} else if strings.TrimSpace(decision.Thought) != "" {
		updated.Messages = append(updated.Messages, agents2.Message{Role: "assistant", Content: decision.Thought})
	}
	if err := r.store.Save(ctx, updated); err != nil {
		return agents2.AgentResult{}, err
	}
	return agents2.AgentResult{
		TaskID:   updated.TaskID,
		Agent:    r.agentName(),
		Snapshot: updated.Clone(),
		Events:   []eventbus.Event{event},
	}, nil
}

func (r *agentRuntime) applyDecision(snapshot agents2.AgentSnapshot, decision agents2.Decision) (eventbus.Event, agents2.AgentSnapshot, error) {
	updated := snapshot.Clone()
	if len(decision.Plan) > 0 {
		updated.Plan = append([]agents2.PlanStep(nil), decision.Plan...)
	}
	if decision.StepIndex != nil {
		updated.StepIndex = *decision.StepIndex
	}
	if strings.TrimSpace(decision.Scratchpad) != "" {
		updated.Scratchpad = decision.Scratchpad
	}
	if decision.Phase != "" {
		updated.Phase = decision.Phase
	}

	switch decision.Action {
	case agents2.ActionUseTool:
		intent := decision.Tool.Clone()
		if strings.TrimSpace(intent.ToolCallID) == "" {
			id, err := newID("tool_call")
			if err != nil {
				return eventbus.Event{}, updated, err
			}
			intent.ToolCallID = id
		}
		if decision.Phase == "" {
			updated.Phase = agents2.BusinessPhaseCallingTool
		}
		updated.PendingToolCallID = intent.ToolCallID
		payload := statemachine.ToolCallPayload{
			ToolCallID: intent.ToolCallID,
			ToolName:   intent.ToolName,
			Arguments:  append(json.RawMessage(nil), intent.Arguments...),
		}
		event, err := r.newTaskEvent(statemachine.EventAgentToolRequested, updated.TaskID, payload)
		return event, updated, err
	case agents2.ActionAskUser:
		intent := *decision.UserInput
		if strings.TrimSpace(intent.RequestID) == "" {
			id, err := newID("input")
			if err != nil {
				return eventbus.Event{}, updated, err
			}
			intent.RequestID = id
		}
		if decision.Phase == "" {
			updated.Phase = agents2.BusinessPhaseWaitingUser
		}
		updated.PendingUserInputID = intent.RequestID
		payload := statemachine.UserInputPayload{
			RequestID: intent.RequestID,
			Prompt:    intent.Prompt,
			Metadata:  append(json.RawMessage(nil), intent.Metadata...),
		}
		event, err := r.newTaskEvent(statemachine.EventAgentUserInputRequested, updated.TaskID, payload)
		return event, updated, err
	case agents2.ActionCreateSubAgent:
		intent := *decision.SubAgent
		if strings.TrimSpace(intent.SubTaskID) == "" {
			id, err := newID("sub_task")
			if err != nil {
				return eventbus.Event{}, updated, err
			}
			intent.SubTaskID = id
		}
		if decision.Phase == "" {
			updated.Phase = agents2.BusinessPhaseWaitingSubAgent
		}
		updated.SubTasks = append(updated.SubTasks, agents2.SubTaskSnapshot{
			SubTaskID: intent.SubTaskID,
			Agent:     intent.Agent,
			Input:     intent.Input,
			Status:    "pending",
		})
		payload := statemachine.SubAgentPayload{
			SubTaskID: intent.SubTaskID,
			Agent:     intent.Agent,
			Input:     intent.Input,
		}
		event, err := r.newTaskEvent(statemachine.EventAgentSubAgentRequested, updated.TaskID, payload)
		return event, updated, err
	case agents2.ActionComplete:
		updated.Phase = agents2.BusinessPhaseCompleted
		updated.PendingToolCallID = ""
		updated.PendingUserInputID = ""
		result, err := completionResult(decision, updated)
		if err != nil {
			return eventbus.Event{}, updated, err
		}
		event, err := r.newTaskEvent(statemachine.EventAgentCompleted, updated.TaskID, statemachine.AgentCompletedPayload{Result: result})
		return event, updated, err
	case agents2.ActionFail:
		updated.Phase = agents2.BusinessPhaseFailed
		updated.FailureCount++
		updated.LastError = strings.TrimSpace(decision.Error)
		if updated.LastError == "" {
			updated.LastError = "agent failed"
		}
		event, err := r.agentFailedEvent(updated.TaskID, "agent_failed", updated.LastError)
		return event, updated, err
	default:
		return eventbus.Event{}, updated, fmt.Errorf("unsupported decision action %q", decision.Action)
	}
}

func (r *agentRuntime) applyObservation(snapshot *agents2.AgentSnapshot, input agents2.AgentResumeInput) string {
	if snapshot == nil {
		return "resume"
	}
	if tool, ok := decodeToolObservation(input.Payload); ok {
		snapshot.LastToolResult = &tool
		if tool.ToolCallID != "" && snapshot.PendingToolCallID == tool.ToolCallID {
			snapshot.PendingToolCallID = ""
		}
		snapshot.Messages = append(snapshot.Messages, agents2.Message{Role: "tool", Data: append(json.RawMessage(nil), input.Payload...)})
		return "tool_result"
	}
	if userInput, ok := decodeUserInput(input.Payload); ok {
		if userInput.RequestID != "" && snapshot.PendingUserInputID == userInput.RequestID {
			snapshot.PendingUserInputID = ""
		}
		snapshot.Messages = append(snapshot.Messages, agents2.Message{Role: "user", Content: userInput.Answer, Data: append(json.RawMessage(nil), input.Payload...)})
		return "user_input"
	}
	if subAgent, ok := decodeSubAgent(input.Payload); ok {
		updateSubTask(snapshot, subAgent)
		snapshot.Messages = append(snapshot.Messages, agents2.Message{Role: "sub_agent", Data: append(json.RawMessage(nil), input.Payload...)})
		return "sub_agent_result"
	}
	snapshot.Messages = append(snapshot.Messages, agents2.Message{Role: "system", Data: append(json.RawMessage(nil), input.Payload...)})
	return "resume"
}

func (r *agentRuntime) failFromError(ctx context.Context, snapshot agents2.AgentSnapshot, code string, cause error) (agents2.AgentResult, error) {
	snapshot.FailureCount++
	snapshot.LastError = cause.Error()
	snapshot.Phase = agents2.BusinessPhaseFailed
	snapshot.PendingModelCallID = ""
	snapshot.UpdatedAt = r.now()
	event, err := r.agentFailedEvent(snapshot.TaskID, code, cause.Error())
	if err != nil {
		return agents2.AgentResult{}, err
	}
	if err := r.store.Save(ctx, snapshot); err != nil {
		return agents2.AgentResult{}, err
	}
	return agents2.AgentResult{
		TaskID:   snapshot.TaskID,
		Agent:    r.agentName(),
		Snapshot: snapshot.Clone(),
		Events:   []eventbus.Event{event},
	}, nil
}

func (r *agentRuntime) agentFailedEvent(taskID string, code string, message string) (eventbus.Event, error) {
	return r.newTaskEvent(statemachine.EventAgentFailed, taskID, statemachine.AgentFailedPayload{
		Code:    code,
		Message: message,
	})
}

func (r *agentRuntime) newTaskEvent(eventType eventbus.EventType, taskID string, payload any) (eventbus.Event, error) {
	return eventbus.NewEvent(statemachine.TopicTask, eventType, payload,
		eventbus.WithTaskID(taskID),
		eventbus.WithSource(r.source),
		eventbus.WithMetadataValue("agent", r.agentName()),
	)
}

func (r *agentRuntime) now() time.Time {
	return r.clock().UTC()
}

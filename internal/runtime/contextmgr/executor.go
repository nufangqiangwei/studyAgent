package contextmgr

import (
	"agent/internal/foundation/llmClient"
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	reactor2 "agent/internal/runtime/reactor"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ModelExecutorOption func(*modelExecutorConfig)

type modelExecutorConfig struct {
	source string
}

func WithModelExecutorSource(source string) ModelExecutorOption {
	return func(config *modelExecutorConfig) {
		config.source = strings.TrimSpace(source)
	}
}

type ModelExecutor struct {
	manager *Manager
	source  string
}

func NewModelExecutor(manager *Manager, options ...ModelExecutorOption) (*ModelExecutor, error) {
	config := modelExecutorConfig{source: "contextmgr.model_executor"}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if manager == nil {
		return nil, fmt.Errorf("context manager model executor: manager is required")
	}
	source := strings.TrimSpace(config.source)
	if source == "" {
		source = "contextmgr.model_executor"
	}
	return &ModelExecutor{manager: manager, source: source}, nil
}

func (e *ModelExecutor) ExecuteEffect(ctx context.Context, runtime reactor2.TaskRuntime, effect reactor2.Effect) (reactor2.EffectResult, error) {
	if e == nil {
		return reactor2.EffectResult{}, fmt.Errorf("context manager model executor is nil")
	}
	if e.manager == nil {
		return reactor2.EffectResult{}, fmt.Errorf("context manager model executor: manager is required")
	}
	if effect.Type != reactor2.EffectModelCall {
		return reactor2.EffectResult{}, fmt.Errorf("context manager model executor: unsupported effect type %q", effect.Type)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload, request, err := decodeModelCallEffect(effect)
	if err != nil {
		return reactor2.EffectResult{}, err
	}
	result, err := e.manager.CallModel(ctx, modelCallInputFromAgentRequest(request, payload, runtime, effect))
	if err != nil {
		event, eventErr := e.modelResponseFailedEvent(effect.TaskID, payload, err)
		if eventErr != nil {
			return reactor2.EffectResult{}, eventErr
		}
		return reactor2.EffectResult{Events: []eventbus.Event{event}}, nil
	}
	event, err := e.modelResponseReceivedEvent(effect.TaskID, payload, result)
	if err != nil {
		return reactor2.EffectResult{}, err
	}
	return reactor2.EffectResult{Events: []eventbus.Event{event}}, nil
}

func decodeModelCallEffect(effect reactor2.Effect) (statemachine.ModelCallPayload, agents2.ModelRequest, error) {
	var payload statemachine.ModelCallPayload
	if err := json.Unmarshal(effect.Payload, &payload); err != nil {
		return payload, agents2.ModelRequest{}, fmt.Errorf("context manager model executor: decode model call payload: %w", err)
	}
	if strings.TrimSpace(payload.ModelCallID) == "" {
		return payload, agents2.ModelRequest{}, fmt.Errorf("context manager model executor: model_call_id is required")
	}
	request, err := agents2.UnmarshalModelRequest(payload.Request)
	if err != nil {
		return payload, agents2.ModelRequest{}, err
	}
	if request.ModelCallID == "" {
		request.ModelCallID = payload.ModelCallID
	}
	return payload, request, nil
}

func modelCallInputFromAgentRequest(request agents2.ModelRequest, payload statemachine.ModelCallPayload, runtime reactor2.TaskRuntime, effect reactor2.Effect) ModelCallInput {
	metadata := cloneStringMap(request.Metadata)
	if request.ModelCallID != "" {
		metadata["model_call_id"] = request.ModelCallID
	}
	if runtime.Agent != "" {
		metadata["runtime_agent"] = runtime.Agent
	}
	return ModelCallInput{
		RunID: request.TaskID,
		Step:  request.Snapshot.StepIndex + 1,
		Agent: AgentProfile{
			Name:        request.Agent,
			Model:       request.Model,
			Temperature: request.Temperature,
			Tools:       toolDefinitionsFromAgentSpecs(request.Tools),
			Metadata:    metadata,
		},
		Messages:      messagesFromAgentMessages(request.Messages),
		CorrelationID: payload.ModelCallID,
		CausationID:   effect.ID,
	}
}

func (e *ModelExecutor) modelResponseReceivedEvent(taskID string, call statemachine.ModelCallPayload, result ModelCallResult) (eventbus.Event, error) {
	response := agents2.ModelResponse{
		Content: result.Response.Content,
		Metadata: map[string]string{
			"provider": result.Response.Provider,
			"model":    result.Response.Model,
		},
	}
	rawResponse, err := agents2.MarshalModelResponse(response)
	if err != nil {
		return eventbus.Event{}, err
	}
	return eventbus.NewEvent(statemachine.TopicTask, statemachine.EventModelResponseReceived, statemachine.ModelCallPayload{
		ModelCallID: call.ModelCallID,
		Agent:       call.Agent,
		Request:     append(json.RawMessage(nil), call.Request...),
		Response:    rawResponse,
	}, eventbus.WithTaskID(taskID), eventbus.WithSource(e.source))
}

func (e *ModelExecutor) modelResponseFailedEvent(taskID string, call statemachine.ModelCallPayload, callErr error) (eventbus.Event, error) {
	return eventbus.NewEvent(statemachine.TopicTask, statemachine.EventModelResponseFailed, statemachine.ModelCallPayload{
		ModelCallID: call.ModelCallID,
		Agent:       call.Agent,
		Request:     append(json.RawMessage(nil), call.Request...),
		Error:       callErr.Error(),
	}, eventbus.WithTaskID(taskID), eventbus.WithSource(e.source))
}

func messagesFromAgentMessages(messages []agents2.Message) []llmClient.Message {
	if len(messages) == 0 {
		return nil
	}
	converted := make([]llmClient.Message, 0, len(messages))
	for _, message := range messages {
		content := message.Content
		if strings.TrimSpace(content) == "" && len(message.Data) > 0 {
			content = string(message.Data)
		}
		converted = append(converted, llmClient.Message{
			Role:    normalizeRole(message.Role),
			Content: content,
		})
	}
	return converted
}

func normalizeRole(role string) llmClient.Role {
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

func toolDefinitionsFromAgentSpecs(specs []agents2.ToolSpec) []llmClient.ToolDefinition {
	if len(specs) == 0 {
		return nil
	}
	defs := make([]llmClient.ToolDefinition, 0, len(specs))
	for _, spec := range specs {
		defs = append(defs, llmClient.ToolDefinition{
			Name:        spec.Name,
			Description: spec.Description,
			InputSchema: append(json.RawMessage(nil), spec.InputSchema...),
		})
	}
	return defs
}

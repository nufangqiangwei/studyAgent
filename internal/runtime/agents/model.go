package agents

import (
	"agent/internal/runtime/eventbus"
	reactor2 "agent/internal/runtime/reactor"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ModelClient interface {
	Complete(ctx context.Context, request ModelRequest) (ModelResponse, error)
}

type ModelRequest struct {
	ModelCallID string            `json:"model_call_id,omitempty"`
	TaskID      string            `json:"task_id"`
	Agent       string            `json:"agent"`
	Model       string            `json:"model,omitempty"`
	Temperature float64           `json:"temperature,omitempty"`
	Trigger     string            `json:"trigger"`
	Input       string            `json:"input,omitempty"`
	Messages    []Message         `json:"messages,omitempty"`
	Snapshot    AgentSnapshot     `json:"snapshot"`
	Tools       []ToolSpec        `json:"tools,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (r ModelRequest) Clone() ModelRequest {
	cloned := r
	cloned.Messages = cloneMessages(r.Messages)
	cloned.Snapshot = r.Snapshot.Clone()
	cloned.Tools = cloneToolSpecs(r.Tools)
	cloned.Metadata = cloneStringMap(r.Metadata)
	return cloned
}

type ModelResponse struct {
	Content  string            `json:"content,omitempty"`
	Decision *Decision         `json:"decision,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (r ModelResponse) Clone() ModelResponse {
	cloned := r
	if r.Decision != nil {
		decision := r.Decision.Clone()
		cloned.Decision = &decision
	}
	cloned.Metadata = cloneStringMap(r.Metadata)
	return cloned
}

func (r ModelResponse) ResolveDecision() (Decision, error) {
	if r.Decision != nil {
		return r.Decision.Clone(), nil
	}
	content := strings.TrimSpace(r.Content)
	if content == "" {
		return Decision{}, fmt.Errorf("model response decision is required")
	}
	content = stripJSONFence(content)
	var decision Decision
	if err := json.Unmarshal([]byte(content), &decision); err == nil && decision.Action != "" {
		return decision.Clone(), nil
	}
	var envelope struct {
		Decision Decision `json:"decision"`
	}
	if err := json.Unmarshal([]byte(content), &envelope); err != nil {
		return Decision{}, fmt.Errorf("decode model decision: %w", err)
	}
	if envelope.Decision.Action == "" {
		return Decision{}, fmt.Errorf("model decision action is required")
	}
	return envelope.Decision.Clone(), nil
}

func stripJSONFence(content string) string {
	content = strings.TrimSpace(content)
	if !strings.HasPrefix(content, "```") {
		return content
	}
	lines := strings.Split(content, "\n")
	if len(lines) < 2 {
		return content
	}
	lines = lines[1:]
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
		lines = lines[:len(lines)-1]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func MarshalModelRequest(request ModelRequest) (json.RawMessage, error) {
	raw, err := json.Marshal(request.Clone())
	if err != nil {
		return nil, fmt.Errorf("marshal model request: %w", err)
	}
	return json.RawMessage(raw), nil
}

func UnmarshalModelRequest(raw json.RawMessage) (ModelRequest, error) {
	var request ModelRequest
	if len(raw) == 0 {
		return request, fmt.Errorf("model request payload is required")
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		return request, fmt.Errorf("decode model request: %w", err)
	}
	return request.Clone(), nil
}

func MarshalModelResponse(response ModelResponse) (json.RawMessage, error) {
	raw, err := json.Marshal(response.Clone())
	if err != nil {
		return nil, fmt.Errorf("marshal model response: %w", err)
	}
	return json.RawMessage(raw), nil
}

func UnmarshalModelResponse(raw json.RawMessage) (ModelResponse, error) {
	var response ModelResponse
	if len(raw) == 0 {
		return response, fmt.Errorf("model response payload is required")
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return response, fmt.Errorf("decode model response: %w", err)
	}
	return response.Clone(), nil
}

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
	client ModelClient
	source string
}

func NewModelExecutor(client ModelClient, options ...ModelExecutorOption) (*ModelExecutor, error) {
	config := modelExecutorConfig{source: "agents.model_executor"}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if client == nil {
		return nil, fmt.Errorf("model executor: client is required")
	}
	source := strings.TrimSpace(config.source)
	if source == "" {
		source = "agents.model_executor"
	}
	return &ModelExecutor{client: client, source: source}, nil
}

func (e *ModelExecutor) ExecuteEffect(ctx context.Context, runtime reactor2.TaskRuntime, effect reactor2.Effect) (reactor2.EffectResult, error) {
	if e == nil {
		return reactor2.EffectResult{}, fmt.Errorf("model executor is nil")
	}
	if e.client == nil {
		return reactor2.EffectResult{}, fmt.Errorf("model executor: client is required")
	}
	if effect.Type != reactor2.EffectModelCall {
		return reactor2.EffectResult{}, fmt.Errorf("model executor: unsupported effect type %q", effect.Type)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload, request, err := decodeModelCallEffect(effect)
	if err != nil {
		return reactor2.EffectResult{}, err
	}
	response, err := e.client.Complete(ctx, request)
	if err != nil {
		event, eventErr := e.modelResponseFailedEvent(effect.TaskID, payload, err)
		if eventErr != nil {
			return reactor2.EffectResult{}, eventErr
		}
		return reactor2.EffectResult{Events: []eventbus.Event{event}}, nil
	}
	event, err := e.modelResponseReceivedEvent(effect.TaskID, payload, response)
	if err != nil {
		return reactor2.EffectResult{}, err
	}
	_ = runtime
	return reactor2.EffectResult{Events: []eventbus.Event{event}}, nil
}

func decodeModelCallEffect(effect reactor2.Effect) (statemachine.ModelCallPayload, ModelRequest, error) {
	var payload statemachine.ModelCallPayload
	if err := json.Unmarshal(effect.Payload, &payload); err != nil {
		return payload, ModelRequest{}, fmt.Errorf("model executor: decode model call payload: %w", err)
	}
	if strings.TrimSpace(payload.ModelCallID) == "" {
		return payload, ModelRequest{}, fmt.Errorf("model executor: model_call_id is required")
	}
	request, err := UnmarshalModelRequest(payload.Request)
	if err != nil {
		return payload, ModelRequest{}, err
	}
	if request.ModelCallID == "" {
		request.ModelCallID = payload.ModelCallID
	}
	return payload, request, nil
}

func (e *ModelExecutor) modelResponseReceivedEvent(taskID string, call statemachine.ModelCallPayload, response ModelResponse) (eventbus.Event, error) {
	rawResponse, err := MarshalModelResponse(response)
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

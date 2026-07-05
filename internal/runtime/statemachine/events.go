package statemachine

import (
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
	"encoding/json"
)

const TopicTask = "task"

const (
	EventTaskCreated             eventbus.EventType = "task.created"
	EventTaskStartRequested      eventbus.EventType = "task.start_requested"
	EventTaskCancelRequested     eventbus.EventType = "task.cancel_requested"
	EventAgentModelRequested     eventbus.EventType = "agent.model_requested"
	EventModelResponseReceived   eventbus.EventType = "model.response_received"
	EventModelResponseFailed     eventbus.EventType = "model.response_failed"
	EventAgentToolRequested      eventbus.EventType = "agent.tool_requested"
	EventToolCompleted           eventbus.EventType = "tool.completed"
	EventToolFailed              eventbus.EventType = "tool.failed"
	EventAgentUserInputRequested eventbus.EventType = "agent.user_input_requested"
	EventUserInputReceived       eventbus.EventType = "user_input.received"
	EventAgentSubAgentRequested  eventbus.EventType = "agent.sub_agent_requested"
	EventSubAgentCompleted       eventbus.EventType = "sub_agent.completed"
	EventSubAgentFailed          eventbus.EventType = "sub_agent.failed"
	EventAgentCompleted          eventbus.EventType = "agent.completed"
	EventAgentFailed             eventbus.EventType = "agent.failed"
)

const (
	EffectEmitTaskCompleted reactor.EffectType = "task.completed.emit"
	EffectEmitTaskFailed    reactor.EffectType = "task.failed.emit"
	EffectEmitTaskCancelled reactor.EffectType = "task.cancelled.emit"
)

type TaskCreatedPayload struct {
	Agent       string            `json:"agent,omitempty"`
	MaxFailures int               `json:"max_failures,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type TaskStartPayload struct {
	Agent       string            `json:"agent,omitempty"`
	Input       string            `json:"input,omitempty"`
	MaxFailures int               `json:"max_failures,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ModelCallPayload struct {
	ModelCallID string          `json:"model_call_id"`
	Agent       string          `json:"agent,omitempty"`
	Request     json.RawMessage `json:"request,omitempty"`
	Response    json.RawMessage `json:"response,omitempty"`
	Error       string          `json:"error,omitempty"`
}

type ToolCallPayload struct {
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
}

type UserInputPayload struct {
	RequestID string          `json:"request_id"`
	Prompt    string          `json:"prompt,omitempty"`
	Answer    string          `json:"answer,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type SubAgentPayload struct {
	SubTaskID string          `json:"sub_task_id"`
	Agent     string          `json:"agent,omitempty"`
	Input     string          `json:"input,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
}

type AgentCompletedPayload struct {
	Result json.RawMessage `json:"result,omitempty"`
}

type AgentFailedPayload struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type TaskTerminalPayload struct {
	TaskID       string          `json:"task_id"`
	Agent        string          `json:"agent,omitempty"`
	AgentPhase   AgentPhase      `json:"agent_phase,omitempty"`
	FailureCount int             `json:"failure_count,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	Error        *TaskError      `json:"error,omitempty"`
}

func decodePayload[T any](event eventbus.Event) (T, error) {
	var payload T
	if len(event.Payload) == 0 {
		return payload, nil
	}
	err := json.Unmarshal(event.Payload, &payload)
	return payload, err
}

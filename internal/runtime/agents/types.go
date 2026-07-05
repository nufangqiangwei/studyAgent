package agents

import (
	"agent/internal/runtime/eventbus"
	"context"
	"encoding/json"
	"time"
)

type Agent interface {
	Name() string
	Start(ctx context.Context, input AgentStartInput) (AgentResult, error)
	Resume(ctx context.Context, input AgentResumeInput) (AgentResult, error)
	Snapshot(ctx context.Context, taskID string) (AgentSnapshot, bool, error)
}

type AgentStartInput struct {
	TaskID   string            `json:"task_id"`
	Input    string            `json:"input,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type AgentResumeInput struct {
	TaskID   string            `json:"task_id"`
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type AgentResult struct {
	TaskID   string           `json:"task_id"`
	Agent    string           `json:"agent"`
	Snapshot AgentSnapshot    `json:"snapshot"`
	Events   []eventbus.Event `json:"events,omitempty"`
}

func (r AgentResult) Clone() AgentResult {
	cloned := r
	cloned.Snapshot = r.Snapshot.Clone()
	if len(r.Events) > 0 {
		cloned.Events = make([]eventbus.Event, 0, len(r.Events))
		for _, event := range r.Events {
			cloned.Events = append(cloned.Events, event.Clone())
		}
	}
	return cloned
}

type BusinessPhase string

const (
	BusinessPhaseCallingModel    BusinessPhase = "CallingModel"
	BusinessPhaseUnderstanding   BusinessPhase = "Understanding"
	BusinessPhasePlanning        BusinessPhase = "Planning"
	BusinessPhaseCallingTool     BusinessPhase = "CallingTool"
	BusinessPhaseWaitingUser     BusinessPhase = "WaitingUser"
	BusinessPhaseWaitingSubAgent BusinessPhase = "WaitingSubAgent"
	BusinessPhaseCompleted       BusinessPhase = "Completed"
	BusinessPhaseFailed          BusinessPhase = "Failed"
)

type AgentSnapshot struct {
	TaskID             string            `json:"task_id"`
	Agent              string            `json:"agent"`
	Phase              BusinessPhase     `json:"phase"`
	Input              string            `json:"input,omitempty"`
	Messages           []Message         `json:"messages,omitempty"`
	Plan               []PlanStep        `json:"plan,omitempty"`
	StepIndex          int               `json:"step_index"`
	Scratchpad         string            `json:"scratchpad,omitempty"`
	PendingModelCallID string            `json:"pending_model_call_id,omitempty"`
	LastToolResult     *ToolObservation  `json:"last_tool_result,omitempty"`
	PendingToolCallID  string            `json:"pending_tool_call_id,omitempty"`
	PendingUserInputID string            `json:"pending_user_input_id,omitempty"`
	SubTasks           []SubTaskSnapshot `json:"sub_tasks,omitempty"`
	FailureCount       int               `json:"failure_count"`
	LastError          string            `json:"last_error,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
	UpdatedAt          time.Time         `json:"updated_at"`
}

func NewAgentSnapshot(agent string, input AgentStartInput, now time.Time) AgentSnapshot {
	return AgentSnapshot{
		TaskID:    input.TaskID,
		Agent:     agent,
		Phase:     BusinessPhaseUnderstanding,
		Input:     input.Input,
		Metadata:  cloneStringMap(input.Metadata),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (s AgentSnapshot) Clone() AgentSnapshot {
	cloned := s
	if len(s.Messages) > 0 {
		cloned.Messages = make([]Message, 0, len(s.Messages))
		for _, message := range s.Messages {
			cloned.Messages = append(cloned.Messages, message.Clone())
		}
	}
	if len(s.Plan) > 0 {
		cloned.Plan = append([]PlanStep(nil), s.Plan...)
	}
	if s.LastToolResult != nil {
		last := s.LastToolResult.Clone()
		cloned.LastToolResult = &last
	}
	if len(s.SubTasks) > 0 {
		cloned.SubTasks = make([]SubTaskSnapshot, 0, len(s.SubTasks))
		for _, subTask := range s.SubTasks {
			cloned.SubTasks = append(cloned.SubTasks, subTask.Clone())
		}
	}
	if len(s.Metadata) > 0 {
		cloned.Metadata = cloneStringMap(s.Metadata)
	}
	return cloned
}

type Message struct {
	Role    string          `json:"role"`
	Content string          `json:"content,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (m Message) Clone() Message {
	cloned := m
	cloned.Data = append(json.RawMessage(nil), m.Data...)
	return cloned
}

type PlanStep struct {
	ID      string `json:"id,omitempty"`
	Goal    string `json:"goal"`
	Status  string `json:"status,omitempty"`
	Summary string `json:"summary,omitempty"`
}

type ToolObservation struct {
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
}

func (o ToolObservation) Clone() ToolObservation {
	cloned := o
	cloned.Result = append(json.RawMessage(nil), o.Result...)
	return cloned
}

type SubTaskSnapshot struct {
	SubTaskID string          `json:"sub_task_id"`
	Agent     string          `json:"agent,omitempty"`
	Input     string          `json:"input,omitempty"`
	Status    string          `json:"status,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func (s SubTaskSnapshot) Clone() SubTaskSnapshot {
	cloned := s
	cloned.Result = append(json.RawMessage(nil), s.Result...)
	return cloned
}

type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

func (s ToolSpec) Clone() ToolSpec {
	cloned := s
	cloned.InputSchema = append(json.RawMessage(nil), s.InputSchema...)
	return cloned
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

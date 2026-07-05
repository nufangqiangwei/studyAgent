package statemachine

import (
	"agent/internal/runtime/eventbus"
	"encoding/json"
	"time"
)

type TaskPhase string

const (
	PhaseCreated          TaskPhase = "Created"
	PhaseRunning          TaskPhase = "Running"
	PhaseWaitingModel     TaskPhase = "WaitingModel"
	PhaseWaitingTool      TaskPhase = "WaitingTool"
	PhaseWaitingUserInput TaskPhase = "WaitingUserInput"
	PhaseWaitingSubAgent  TaskPhase = "WaitingSubAgent"
	PhaseCompleted        TaskPhase = "Completed"
	PhaseFailed           TaskPhase = "Failed"
	PhaseCancelled        TaskPhase = "Cancelled"
)

type AgentPhase string

const AgentPhaseUnknown AgentPhase = ""

type TaskState struct {
	TaskID           string            `json:"task_id"`
	Phase            TaskPhase         `json:"phase"`
	Agent            AgentRuntimeState `json:"agent"`
	PendingModel     *PendingModelCall `json:"pending_model,omitempty"`
	PendingTool      *PendingToolCall  `json:"pending_tool,omitempty"`
	PendingUserInput *PendingUserInput `json:"pending_user_input,omitempty"`
	PendingSubAgent  *PendingSubAgent  `json:"pending_sub_agent,omitempty"`
	FailureCount     int               `json:"failure_count"`
	MaxFailures      int               `json:"max_failures"`
	LastEventID      string            `json:"last_event_id,omitempty"`
	LastError        *TaskError        `json:"last_error,omitempty"`
	Result           json.RawMessage   `json:"result,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	Lifecycle        []LifecycleRecord `json:"lifecycle,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	CompletedAt      *time.Time        `json:"completed_at,omitempty"`
}

type AgentRuntimeState struct {
	Name      string            `json:"name,omitempty"`
	Phase     AgentPhase        `json:"phase,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	StartedAt *time.Time        `json:"started_at,omitempty"`
	UpdatedAt *time.Time        `json:"updated_at,omitempty"`
}

type PendingModelCall struct {
	ModelCallID string          `json:"model_call_id"`
	Agent       string          `json:"agent,omitempty"`
	Request     json.RawMessage `json:"request,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

type PendingToolCall struct {
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

type PendingUserInput struct {
	RequestID string          `json:"request_id"`
	Prompt    string          `json:"prompt,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type PendingSubAgent struct {
	SubTaskID string    `json:"sub_task_id"`
	Agent     string    `json:"agent,omitempty"`
	Input     string    `json:"input,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type TaskError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type LifecycleRecord struct {
	Time               time.Time          `json:"time"`
	EventID            string             `json:"event_id,omitempty"`
	EventType          eventbus.EventType `json:"event_type,omitempty"`
	PreviousPhase      TaskPhase          `json:"previous_phase,omitempty"`
	NextPhase          TaskPhase          `json:"next_phase,omitempty"`
	PreviousAgentPhase AgentPhase         `json:"previous_agent_phase,omitempty"`
	NextAgentPhase     AgentPhase         `json:"next_agent_phase,omitempty"`
	Reason             string             `json:"reason,omitempty"`
}

func NewTaskState(taskID string, now time.Time) TaskState {
	return TaskState{
		TaskID:      taskID,
		Phase:       PhaseCreated,
		MaxFailures: 3,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func (s TaskState) Clone() TaskState {
	cloned := s
	cloned.Result = append(json.RawMessage(nil), s.Result...)
	if len(s.Metadata) > 0 {
		cloned.Metadata = cloneStringMap(s.Metadata)
	}
	if len(s.Agent.Metadata) > 0 {
		cloned.Agent.Metadata = cloneStringMap(s.Agent.Metadata)
	}
	if s.Agent.StartedAt != nil {
		startedAt := *s.Agent.StartedAt
		cloned.Agent.StartedAt = &startedAt
	}
	if s.Agent.UpdatedAt != nil {
		updatedAt := *s.Agent.UpdatedAt
		cloned.Agent.UpdatedAt = &updatedAt
	}
	if s.PendingModel != nil {
		pending := *s.PendingModel
		pending.Request = append(json.RawMessage(nil), s.PendingModel.Request...)
		cloned.PendingModel = &pending
	}
	if s.PendingTool != nil {
		pending := *s.PendingTool
		pending.Arguments = append(json.RawMessage(nil), s.PendingTool.Arguments...)
		cloned.PendingTool = &pending
	}
	if s.PendingUserInput != nil {
		pending := *s.PendingUserInput
		pending.Metadata = append(json.RawMessage(nil), s.PendingUserInput.Metadata...)
		cloned.PendingUserInput = &pending
	}
	if s.PendingSubAgent != nil {
		pending := *s.PendingSubAgent
		cloned.PendingSubAgent = &pending
	}
	if s.LastError != nil {
		lastError := *s.LastError
		cloned.LastError = &lastError
	}
	if s.CompletedAt != nil {
		completedAt := *s.CompletedAt
		cloned.CompletedAt = &completedAt
	}
	if len(s.Lifecycle) > 0 {
		cloned.Lifecycle = append([]LifecycleRecord(nil), s.Lifecycle...)
	}
	return cloned
}

func (s TaskState) IsTerminal() bool {
	return s.Phase == PhaseCompleted || s.Phase == PhaseFailed || s.Phase == PhaseCancelled
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

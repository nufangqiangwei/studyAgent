package agent

import (
	"agent/internal/foundation/llmClient"
	"encoding/json"
	"time"
)

type RunStatus string

const (
	RunStatusCreated                    RunStatus = "Created"
	RunStatusPreparing                  RunStatus = "Preparing"
	RunStatusCallingModel               RunStatus = "CallingModel"
	RunStatusWaitingForToolResult       RunStatus = "WaitingForToolResult"
	RunStatusWaitingForUserApproval     RunStatus = "WaitingForUserApproval"
	RunStatusWaitingForExternalCallback RunStatus = "WaitingForExternalCallback"
	RunStatusWaitingForScheduledResume  RunStatus = "WaitingForScheduledResume"
	RunStatusObservingResult            RunStatus = "ObservingResult"
	RunStatusEvaluatingStop             RunStatus = "EvaluatingStop"
	RunStatusCompleted                  RunStatus = "Completed"
	RunStatusFailed                     RunStatus = "Failed"
	RunStatusCancelled                  RunStatus = "Cancelled"
	RunStatusStepLimitReached           RunStatus = "StepLimitReached"
	RunStatusNeedsAlternativeStrategy   RunStatus = "NeedsAlternativeStrategy"
)

type ToolCallStatus string

const (
	ToolCallStatusRequested  ToolCallStatus = "Requested"
	ToolCallStatusDispatched ToolCallStatus = "Dispatched"
	ToolCallStatusCompleted  ToolCallStatus = "Completed"
	ToolCallStatusFailed     ToolCallStatus = "Failed"
)

type PendingToolCall struct {
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	Status     ToolCallStatus  `json:"status"`
	StepIndex  int             `json:"step_index"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

type LoopEvent interface {
	EventID() string
	RunID() string
	EventType() string
}

const (
	LoopEventRunStarted            = "RunStarted"
	LoopEventRunResumed            = "RunResumed"
	LoopEventRunCancelled          = "RunCancelled"
	LoopEventModelResponseReceived = "ModelResponseReceived"
	LoopEventModelResponseFailed   = "ModelResponseFailed"
	LoopEventToolCallCompleted     = "ToolCallCompleted"
)

type RunStartedEvent struct {
	ID         string    `json:"event_id,omitempty"`
	RunIDValue string    `json:"run_id,omitempty"`
	Task       Task      `json:"task"`
	CreatedAt  time.Time `json:"created_at,omitempty"`
}

func NewRunStartedEvent(task Task) RunStartedEvent {
	return RunStartedEvent{Task: task, CreatedAt: time.Now().UTC()}
}

func (e RunStartedEvent) EventID() string   { return e.ID }
func (e RunStartedEvent) RunID() string     { return e.RunIDValue }
func (e RunStartedEvent) EventType() string { return LoopEventRunStarted }

type RunResumedEvent struct {
	ID         string    `json:"event_id,omitempty"`
	RunIDValue string    `json:"run_id"`
	ResumedAt  time.Time `json:"resumed_at,omitempty"`
}

func (e RunResumedEvent) EventID() string   { return e.ID }
func (e RunResumedEvent) RunID() string     { return e.RunIDValue }
func (e RunResumedEvent) EventType() string { return LoopEventRunResumed }

type RunCancelledEvent struct {
	ID          string    `json:"event_id,omitempty"`
	RunIDValue  string    `json:"run_id"`
	Reason      string    `json:"reason,omitempty"`
	CancelledAt time.Time `json:"cancelled_at,omitempty"`
}

func (e RunCancelledEvent) EventID() string   { return e.ID }
func (e RunCancelledEvent) RunID() string     { return e.RunIDValue }
func (e RunCancelledEvent) EventType() string { return LoopEventRunCancelled }

type ModelResponseReceivedEvent struct {
	ID          string             `json:"event_id,omitempty"`
	RunIDValue  string             `json:"run_id"`
	Response    llmClient.Response `json:"response"`
	StartedAt   time.Time          `json:"started_at,omitempty"`
	CompletedAt time.Time          `json:"completed_at,omitempty"`
}

func (e ModelResponseReceivedEvent) EventID() string   { return e.ID }
func (e ModelResponseReceivedEvent) RunID() string     { return e.RunIDValue }
func (e ModelResponseReceivedEvent) EventType() string { return LoopEventModelResponseReceived }

type ModelResponseFailedEvent struct {
	ID          string    `json:"event_id,omitempty"`
	RunIDValue  string    `json:"run_id"`
	Error       string    `json:"error"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

func (e ModelResponseFailedEvent) EventID() string   { return e.ID }
func (e ModelResponseFailedEvent) RunID() string     { return e.RunIDValue }
func (e ModelResponseFailedEvent) EventType() string { return LoopEventModelResponseFailed }

type ToolCallCompletedEvent struct {
	ID          string     `json:"event_id,omitempty"`
	RunIDValue  string     `json:"run_id"`
	ToolCallID  string     `json:"tool_call_id"`
	ToolName    string     `json:"tool_name"`
	Result      ToolResult `json:"result"`
	Error       string     `json:"error,omitempty"`
	StartedAt   time.Time  `json:"started_at,omitempty"`
	CompletedAt time.Time  `json:"completed_at,omitempty"`
}

func (e ToolCallCompletedEvent) EventID() string   { return e.ID }
func (e ToolCallCompletedEvent) RunID() string     { return e.RunIDValue }
func (e ToolCallCompletedEvent) EventType() string { return LoopEventToolCallCompleted }

type LoopActionKind string

const (
	LoopActionCallModel    LoopActionKind = "CallModel"
	LoopActionDispatchTool LoopActionKind = "DispatchTool"
	LoopActionEmitFinal    LoopActionKind = "EmitFinalOutput"
)

type LoopAction struct {
	ID           string             `json:"id"`
	Kind         LoopActionKind     `json:"kind"`
	RunID        string             `json:"run_id"`
	TurnID       string             `json:"turn_id,omitempty"`
	Step         int                `json:"step"`
	Task         Task               `json:"task"`
	ModelRequest *llmClient.Request `json:"model_request,omitempty"`
	ToolCall     *PendingToolCall   `json:"tool_call,omitempty"`
	FinalAnswer  string             `json:"final_answer,omitempty"`
	Result       *Result            `json:"result,omitempty"`
}

type LoopAdvanceResult struct {
	RunID     string       `json:"run_id"`
	Status    RunStatus    `json:"status"`
	State     RunState     `json:"state"`
	Actions   []LoopAction `json:"actions,omitempty"`
	Result    *Result      `json:"result,omitempty"`
	Suspended bool         `json:"suspended,omitempty"`
}

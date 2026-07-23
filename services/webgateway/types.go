package webgateway

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/services/task"
	"context"
	"fmt"
	"strings"
	"time"
)

type Operation string

const (
	OperationCreate Operation = "create"
	OperationGet    Operation = "get"
)

func (o Operation) valid() bool {
	return o == OperationCreate || o == OperationGet
}

type RequestPhase string

const (
	PhaseDeclaringTask     RequestPhase = "declaring_task"
	PhaseWaitingTask       RequestPhase = "waiting_task"
	PhaseMarkingReady      RequestPhase = "marking_ready"
	PhaseAssigning         RequestPhase = "assigning"
	PhaseStarting          RequestPhase = "starting"
	PhaseResolvingTerminal RequestPhase = "resolving_terminal"
	PhaseSucceeded         RequestPhase = "succeeded"
	PhaseFailed            RequestPhase = "failed"
)

func (p RequestPhase) terminal() bool {
	return p == PhaseSucceeded || p == PhaseFailed
}

func (p RequestPhase) valid() bool {
	switch p {
	case PhaseDeclaringTask, PhaseWaitingTask, PhaseMarkingReady, PhaseAssigning, PhaseStarting,
		PhaseResolvingTerminal,
		PhaseSucceeded, PhaseFailed:
		return true
	default:
		return false
	}
}

type CreateTaskRequest struct {
	RequestID     string                `json:"request_id"`
	TaskID        string                `json:"task_id,omitempty"`
	GoalID        string                `json:"goal_id,omitempty"`
	Title         string                `json:"title,omitempty"`
	Input         string                `json:"input,omitempty"`
	InputArtifact *contract.ArtifactRef `json:"input_artifact,omitempty"`
	Deadline      *time.Time            `json:"deadline,omitempty"`
}

type GetTaskRequest struct {
	RequestID string `json:"request_id"`
	TaskID    string `json:"task_id"`
}

type ErrorDTO struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

func (e ErrorDTO) validate() error {
	if strings.TrimSpace(e.Code) == "" || strings.TrimSpace(e.Message) == "" {
		return fmt.Errorf("error code and message are required")
	}
	return nil
}

type TaskDTO struct {
	TaskID        string                  `json:"task_id"`
	GoalID        string                  `json:"goal_id,omitempty"`
	UserID        string                  `json:"user_id"`
	Title         string                  `json:"title,omitempty"`
	Input         string                  `json:"input,omitempty"`
	InputArtifact *contract.ArtifactRef   `json:"input_artifact,omitempty"`
	Phase         task.Phase              `json:"phase"`
	AssignedTo    contract.ServiceAddress `json:"assigned_to,omitempty"`
	ActiveRunID   string                  `json:"active_run_id,omitempty"`
	Attempt       int                     `json:"attempt,omitempty"`
	ResultRef     *contract.ArtifactRef   `json:"result_ref,omitempty"`
	LastError     *ErrorDTO               `json:"last_error,omitempty"`
	CreatedAt     time.Time               `json:"created_at"`
	UpdatedAt     time.Time               `json:"updated_at"`
	CompletedAt   *time.Time              `json:"completed_at,omitempty"`
}

func (t TaskDTO) clone() TaskDTO {
	t.InputArtifact = cloneArtifact(t.InputArtifact)
	t.ResultRef = cloneArtifact(t.ResultRef)
	if t.LastError != nil {
		value := *t.LastError
		t.LastError = &value
	}
	t.CompletedAt = cloneTime(t.CompletedAt)
	return t
}

func (t TaskDTO) validate() error {
	if strings.TrimSpace(t.TaskID) == "" || strings.TrimSpace(t.UserID) == "" ||
		!t.Phase.Valid() || t.CreatedAt.IsZero() || t.UpdatedAt.IsZero() {
		return fmt.Errorf("task presentation identity, user, phase, and timestamps are required")
	}
	if (strings.TrimSpace(t.Input) == "") == (t.InputArtifact == nil) {
		return fmt.Errorf("task presentation requires exactly one input")
	}
	if t.InputArtifact != nil {
		if err := artifact.ValidateRef(*t.InputArtifact); err != nil {
			return fmt.Errorf("task input artifact is invalid: %w", err)
		}
	}
	if t.ResultRef != nil {
		if err := artifact.ValidateRef(*t.ResultRef); err != nil {
			return fmt.Errorf("task result artifact is invalid: %w", err)
		}
	}
	if t.LastError != nil {
		if err := t.LastError.validate(); err != nil {
			return fmt.Errorf("task error is invalid: %w", err)
		}
	}
	return nil
}

type TaskCreatedPresentation struct {
	RequestID string  `json:"request_id"`
	Task      TaskDTO `json:"task"`
}

type TaskFoundPresentation struct {
	RequestID string  `json:"request_id"`
	Task      TaskDTO `json:"task"`
}

// Presentation is delivered only after its owning request event commits.
// Presenter implementations must deduplicate calls by PresentationID.
type Presentation struct {
	PresentationID string                   `json:"presentation_id"`
	RequestID      string                   `json:"request_id"`
	Operation      Operation                `json:"operation"`
	Created        *TaskCreatedPresentation `json:"created,omitempty"`
	Found          *TaskFoundPresentation   `json:"found,omitempty"`
	Error          *ErrorDTO                `json:"error,omitempty"`
}

func (p Presentation) clone() Presentation {
	if p.Created != nil {
		value := *p.Created
		value.Task = value.Task.clone()
		p.Created = &value
	}
	if p.Found != nil {
		value := *p.Found
		value.Task = value.Task.clone()
		p.Found = &value
	}
	if p.Error != nil {
		value := *p.Error
		p.Error = &value
	}
	return p
}

func (p Presentation) validate() error {
	if strings.TrimSpace(p.PresentationID) == "" || strings.TrimSpace(p.RequestID) == "" || !p.Operation.valid() {
		return fmt.Errorf("presentation id, request id, and operation are required")
	}
	count := 0
	if p.Created != nil {
		count++
		if p.Operation != OperationCreate || p.Created.RequestID != p.RequestID {
			return fmt.Errorf("created presentation is invalid")
		}
		if err := p.Created.Task.validate(); err != nil {
			return err
		}
	}
	if p.Found != nil {
		count++
		if p.Operation != OperationGet || p.Found.RequestID != p.RequestID {
			return fmt.Errorf("found presentation is invalid")
		}
		if err := p.Found.Task.validate(); err != nil {
			return err
		}
	}
	if p.Error != nil {
		count++
		if err := p.Error.validate(); err != nil {
			return err
		}
	}
	if count != 1 {
		return fmt.Errorf("presentation requires exactly one result")
	}
	return nil
}

type Presenter interface {
	Present(ctx context.Context, presentation Presentation) error
}

type PresenterFunc func(context.Context, Presentation) error

func (f PresenterFunc) Present(ctx context.Context, presentation Presentation) error {
	return f(ctx, presentation)
}

func taskDTOFromState(value task.State) TaskDTO {
	result := TaskDTO{
		TaskID: value.TaskID, GoalID: value.GoalID, UserID: value.UserID, Title: value.Title,
		Input: value.Input, InputArtifact: cloneArtifact(value.InputArtifact),
		Phase: value.Phase, AssignedTo: value.AssignedTo, ActiveRunID: value.ActiveRunID,
		Attempt: value.Attempt, ResultRef: cloneArtifact(value.ResultRef),
		CreatedAt: value.CreatedAt.UTC(), UpdatedAt: value.UpdatedAt.UTC(),
		CompletedAt: cloneTime(value.CompletedAt),
	}
	if value.LastError != nil {
		result.LastError = &ErrorDTO{
			Code: value.LastError.Code, Message: value.LastError.Message, Retryable: value.LastError.Retryable,
		}
	}
	return result
}

func cloneArtifact(value *contract.ArtifactRef) *contract.ArtifactRef {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

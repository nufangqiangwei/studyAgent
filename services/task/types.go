package task

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"fmt"
	"strings"
	"time"
)

type Phase string

const (
	PhaseCreated   Phase = "created"
	PhaseReady     Phase = "ready"
	PhaseRunning   Phase = "running"
	PhaseWaiting   Phase = "waiting"
	PhaseSuspended Phase = "suspended"
	PhaseCompleted Phase = "completed"
	PhaseFailed    Phase = "failed"
	PhaseCancelled Phase = "cancelled"
)

func (p Phase) Valid() bool {
	switch p {
	case PhaseCreated, PhaseReady, PhaseRunning, PhaseWaiting, PhaseSuspended,
		PhaseCompleted, PhaseFailed, PhaseCancelled:
		return true
	default:
		return false
	}
}

func (p Phase) Terminal() bool {
	return p == PhaseCompleted || p == PhaseFailed || p == PhaseCancelled
}

type WaitKind string

const (
	WaitModel               WaitKind = "model"
	WaitCapability          WaitKind = "capability"
	WaitChildAgent          WaitKind = "child_agent"
	WaitUser                WaitKind = "user"
	WaitSchedule            WaitKind = "schedule"
	WaitExternalCallback    WaitKind = "external_callback"
	WaitReconciliation      WaitKind = "reconciliation"
	WaitAlternativeStrategy WaitKind = "alternative_strategy"
)

func (k WaitKind) Valid() bool {
	switch k {
	case WaitModel, WaitCapability, WaitChildAgent, WaitUser, WaitSchedule,
		WaitExternalCallback, WaitReconciliation, WaitAlternativeStrategy:
		return true
	default:
		return false
	}
}

type WaitState struct {
	Kind        WaitKind               `json:"kind"`
	References  []string               `json:"references,omitempty"`
	ResumeOn    []contract.MessageType `json:"resume_on,omitempty"`
	Deadline    *time.Time             `json:"deadline,omitempty"`
	RequestedAt time.Time              `json:"requested_at"`
}

func (w WaitState) clone() WaitState {
	w.References = append([]string(nil), w.References...)
	w.ResumeOn = append([]contract.MessageType(nil), w.ResumeOn...)
	w.Deadline = cloneTime(w.Deadline)
	return w
}

type Suspension struct {
	Reason      string    `json:"reason,omitempty"`
	SuspendedAt time.Time `json:"suspended_at"`
}

type Cancellation struct {
	ReasonCode  string    `json:"reason_code,omitempty"`
	RequestedAt time.Time `json:"requested_at"`
}

type Error struct {
	Code       string    `json:"code"`
	Message    string    `json:"message"`
	Source     string    `json:"source,omitempty"`
	RunID      string    `json:"run_id,omitempty"`
	Retryable  bool      `json:"retryable,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

type State struct {
	TaskID string `json:"task_id"`
	GoalID string `json:"goal_id,omitempty"`
	UserID string `json:"user_id,omitempty"`

	OwnerAddress contract.ServiceAddress `json:"owner_address"`
	Phase        Phase                   `json:"phase"`
	Wait         *WaitState              `json:"wait,omitempty"`
	Suspension   *Suspension             `json:"suspension,omitempty"`
	Cancellation *Cancellation           `json:"cancellation,omitempty"`

	Title         string                `json:"title,omitempty"`
	Input         string                `json:"input,omitempty"`
	InputArtifact *contract.ArtifactRef `json:"input_artifact,omitempty"`

	AssignedTo   contract.ServiceAddress `json:"assigned_to,omitempty"`
	ActiveRunID  string                  `json:"active_run_id,omitempty"`
	Attempt      int                     `json:"attempt"`
	FailureCount int                     `json:"failure_count"`

	ResultRef *contract.ArtifactRef `json:"result_ref,omitempty"`
	LastError *Error                `json:"last_error,omitempty"`
	Deadline  *time.Time            `json:"deadline,omitempty"`

	IdentityFingerprint string     `json:"identity_fingerprint"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
}

func (s State) Clone() State {
	if s.Wait != nil {
		value := s.Wait.clone()
		s.Wait = &value
	}
	if s.Suspension != nil {
		value := *s.Suspension
		s.Suspension = &value
	}
	if s.Cancellation != nil {
		value := *s.Cancellation
		s.Cancellation = &value
	}
	s.InputArtifact = cloneArtifact(s.InputArtifact)
	s.ResultRef = cloneArtifact(s.ResultRef)
	if s.LastError != nil {
		value := *s.LastError
		s.LastError = &value
	}
	s.Deadline = cloneTime(s.Deadline)
	s.CompletedAt = cloneTime(s.CompletedAt)
	return s
}

type CreateRequest struct {
	TaskID        string                `json:"task_id"`
	GoalID        string                `json:"goal_id,omitempty"`
	Title         string                `json:"title,omitempty"`
	Input         string                `json:"input,omitempty"`
	InputArtifact *contract.ArtifactRef `json:"input_artifact,omitempty"`
	Deadline      *time.Time            `json:"deadline,omitempty"`
}

type AssignRequest struct {
	AgentAddress contract.ServiceAddress `json:"agent_address"`
}

type SuspendRequest struct {
	Reason string `json:"reason,omitempty"`
}

type CancelRequest struct {
	ReasonCode string `json:"reason_code,omitempty"`
}

type GetRequest struct {
	TaskID string `json:"task_id,omitempty"`
}

type StatusResponse struct {
	Task *State `json:"task,omitempty"`
}

type Result struct {
	TaskID      string                `json:"task_id"`
	GoalID      string                `json:"goal_id,omitempty"`
	Phase       Phase                 `json:"phase"`
	Attempt     int                   `json:"attempt"`
	ResultRef   *contract.ArtifactRef `json:"result_ref,omitempty"`
	Error       *Error                `json:"error,omitempty"`
	CompletedAt time.Time             `json:"completed_at"`
}

type ExecutionWaiting struct {
	TaskID     string                 `json:"task_id"`
	RunID      string                 `json:"run_id"`
	Kind       WaitKind               `json:"kind"`
	References []string               `json:"references,omitempty"`
	ResumeOn   []contract.MessageType `json:"resume_on,omitempty"`
	Deadline   *time.Time             `json:"deadline,omitempty"`
}

type ExecutionResumed struct {
	TaskID string `json:"task_id"`
	RunID  string `json:"run_id"`
}

func (s State) validate() error {
	if strings.TrimSpace(s.TaskID) == "" || strings.TrimSpace(string(s.OwnerAddress)) == "" || !s.Phase.Valid() {
		return fmt.Errorf("task id, owner, and valid phase are required")
	}
	if s.Attempt < 0 || s.FailureCount < 0 || s.FailureCount > s.Attempt {
		return fmt.Errorf("task attempt counters are invalid")
	}
	if s.IdentityFingerprint == "" || s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
		return fmt.Errorf("task identity and timestamps are required")
	}
	if s.Phase == PhaseWaiting {
		if s.Wait == nil || !s.Wait.Kind.Valid() || s.ActiveRunID == "" {
			return fmt.Errorf("waiting task requires a valid wait state and active run")
		}
	} else if s.Wait != nil {
		return fmt.Errorf("non-waiting task cannot retain a wait state")
	}
	if s.Phase == PhaseSuspended && s.Suspension == nil {
		return fmt.Errorf("suspended task requires suspension details")
	}
	if s.Phase != PhaseSuspended && s.Suspension != nil {
		return fmt.Errorf("non-suspended task cannot retain suspension details")
	}
	if (s.Phase == PhaseRunning || s.Phase == PhaseWaiting) && (s.AssignedTo == "" || s.ActiveRunID == "" || s.Attempt <= 0) {
		return fmt.Errorf("active task requires assignee, run id, and positive attempt")
	}
	if s.Phase.Terminal() && s.CompletedAt == nil {
		return fmt.Errorf("terminal task requires completed_at")
	}
	if !s.Phase.Terminal() && s.CompletedAt != nil {
		return fmt.Errorf("non-terminal task cannot have completed_at")
	}
	if s.Phase == PhaseCompleted && s.ResultRef == nil {
		return fmt.Errorf("completed task requires a result artifact")
	}
	if s.ResultRef != nil {
		if err := artifact.ValidateRef(*s.ResultRef); err != nil {
			return fmt.Errorf("task result artifact is invalid: %w", err)
		}
	}
	if s.Phase == PhaseFailed && s.LastError == nil {
		return fmt.Errorf("failed task requires an error")
	}
	if s.LastError != nil && (strings.TrimSpace(s.LastError.Code) == "" || s.LastError.OccurredAt.IsZero()) {
		return fmt.Errorf("task error code and timestamp are required")
	}
	if (s.InputArtifact == nil) == (strings.TrimSpace(s.Input) == "") {
		return fmt.Errorf("task requires exactly one inline or artifact input")
	}
	if len(s.Input) > maxInlineTaskInputBytes {
		return fmt.Errorf("task inline input is too large")
	}
	if s.InputArtifact != nil {
		if err := artifact.ValidateRef(*s.InputArtifact); err != nil {
			return fmt.Errorf("task input artifact is invalid: %w", err)
		}
	}
	return nil
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

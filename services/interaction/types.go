package interaction

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/services/approval"
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	MaxInlineInputBytes   int64 = 16 << 10
	defaultMaxOutputBytes       = 16 << 20
	// RetainedTerminalRequests bounds the materialized state projection.
	// The Journal remains the complete source of persisted request facts.
	RetainedTerminalRequests = 5
)

type SubmitRequest struct {
	RequestID     string                `json:"request_id,omitempty"`
	Input         string                `json:"input,omitempty"`
	InputArtifact *contract.ArtifactRef `json:"input_artifact,omitempty"`
}

type RequestPhase string

const (
	PhaseRunning   RequestPhase = "running"
	PhaseCompleted RequestPhase = "completed"
	PhaseFailed    RequestPhase = "failed"
)

func (p RequestPhase) terminal() bool { return p == PhaseCompleted || p == PhaseFailed }

type RequestState struct {
	RequestID           string                  `json:"request_id"`
	RunID               string                  `json:"run_id"`
	Caller              contract.ServiceAddress `json:"caller,omitempty"`
	UserID              string                  `json:"user_id,omitempty"`
	GoalID              string                  `json:"goal_id,omitempty"`
	IdentityFingerprint string                  `json:"identity_fingerprint"`
	Phase               RequestPhase            `json:"phase"`
	Output              *contract.ArtifactRef   `json:"output,omitempty"`
	ErrorCode           string                  `json:"error_code,omitempty"`
	ErrorMessage        string                  `json:"error_message,omitempty"`
	StartedAt           time.Time               `json:"started_at"`
	CompletedAt         *time.Time              `json:"completed_at,omitempty"`
}

func (s RequestState) clone() RequestState {
	if s.Output != nil {
		value := *s.Output
		s.Output = &value
	}
	if s.CompletedAt != nil {
		value := *s.CompletedAt
		s.CompletedAt = &value
	}
	return s
}

func (s RequestState) validate() error {
	if strings.TrimSpace(s.RequestID) == "" || s.RunID != s.RequestID || strings.TrimSpace(s.IdentityFingerprint) == "" {
		return fmt.Errorf("interaction request identity is invalid")
	}
	if s.StartedAt.IsZero() {
		return fmt.Errorf("interaction request start time is required")
	}
	switch s.Phase {
	case PhaseRunning:
		if s.Output != nil || s.ErrorCode != "" || s.ErrorMessage != "" || s.CompletedAt != nil {
			return fmt.Errorf("running interaction request contains terminal state")
		}
	case PhaseCompleted:
		if s.Output == nil || s.CompletedAt == nil || s.CompletedAt.IsZero() || s.ErrorCode != "" || s.ErrorMessage != "" {
			return fmt.Errorf("completed interaction request requires output and completion time")
		}
		if err := artifact.ValidateRef(*s.Output); err != nil {
			return fmt.Errorf("completed interaction output is invalid: %w", err)
		}
	case PhaseFailed:
		if s.Output != nil || strings.TrimSpace(s.ErrorCode) == "" || strings.TrimSpace(s.ErrorMessage) == "" || s.CompletedAt == nil || s.CompletedAt.IsZero() {
			return fmt.Errorf("failed interaction request requires an error and completion time")
		}
	default:
		return fmt.Errorf("interaction request phase %q is invalid", s.Phase)
	}
	if s.CompletedAt != nil && s.CompletedAt.Before(s.StartedAt) {
		return fmt.Errorf("interaction request completion precedes its start")
	}
	return nil
}

type State struct {
	Requests         map[string]RequestState `json:"requests"`
	TerminalOrderIDs []string                `json:"terminal_order_ids,omitempty"`
}

type PresentationKind string

const (
	PresentationAnswer   PresentationKind = "answer"
	PresentationError    PresentationKind = "error"
	PresentationApproval PresentationKind = "approval"
)

// Presentation is the process-local view delivered after the Service commit.
// ID is stable and Presenter implementations should deduplicate it.
type Presentation struct {
	ID           string                `json:"id"`
	Kind         PresentationKind      `json:"kind"`
	RequestID    string                `json:"request_id,omitempty"`
	RunID        string                `json:"run_id,omitempty"`
	Output       *contract.ArtifactRef `json:"output,omitempty"`
	Content      string                `json:"content,omitempty"`
	ErrorCode    string                `json:"error_code,omitempty"`
	ErrorMessage string                `json:"error_message,omitempty"`
	Approval     *approval.Requested   `json:"approval,omitempty"`
}

func (p Presentation) clone() Presentation {
	if p.Output != nil {
		value := *p.Output
		p.Output = &value
	}
	if p.Approval != nil {
		value := *p.Approval
		if value.ArgumentsRef != nil {
			ref := *value.ArgumentsRef
			value.ArgumentsRef = &ref
		}
		if value.ExpiresAt != nil {
			expires := *value.ExpiresAt
			value.ExpiresAt = &expires
		}
		p.Approval = &value
	}
	return p
}

type Presenter interface {
	Present(ctx context.Context, presentation Presentation) error
}

type PresenterFunc func(context.Context, Presentation) error

func (f PresenterFunc) Present(ctx context.Context, presentation Presentation) error {
	return f(ctx, presentation)
}

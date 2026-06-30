package state

import "encoding/json"

const CurrentSchemaVersion = 1

type RunPhase string

const (
	PhaseIdle      RunPhase = "idle"
	PhaseRunning   RunPhase = "running"
	PhaseWaiting   RunPhase = "waiting"
	PhaseCompleted RunPhase = "completed"
	PhaseFailed    RunPhase = "failed"
	PhaseCancelled RunPhase = "cancelled"
)

type RunState struct {
	SchemaVersion int      `json:"schema_version"`
	RunID         string   `json:"run_id"`
	Phase         RunPhase `json:"phase"`

	Step     int `json:"step"`
	MaxSteps int `json:"max_steps,omitempty"`

	LastEventID string `json:"last_event_id,omitempty"`

	Waiting *WaitingState `json:"waiting,omitempty"`
	Error   *ErrorState   `json:"error,omitempty"`

	Extensions map[string]json.RawMessage `json:"extensions,omitempty"`
}

type WaitingState struct {
	Reason string `json:"reason"`
	Target string `json:"target,omitempty"`
}

type ErrorState struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	RetryCount int    `json:"retry_count,omitempty"`
}

func NewRunState(runID string, maxSteps int) RunState {
	return RunState{
		SchemaVersion: CurrentSchemaVersion,
		RunID:         runID,
		Phase:         PhaseIdle,
		MaxSteps:      maxSteps,
		Extensions:    make(map[string]json.RawMessage),
	}
}

func (s RunState) IsTerminal() bool {
	return s.Phase == PhaseCompleted ||
		s.Phase == PhaseFailed ||
		s.Phase == PhaseCancelled
}

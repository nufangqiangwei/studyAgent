package projection

import (
	"agent/internal/content"
	"agent/internal/runtime/persistence"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type RunStatus = content.AsyncRunStatus

type RecoverResult = content.AsyncRecoverResult

type WorkResult = content.AsyncWorkResult

type StatusUpdate struct {
	AdvanceStatus      string
	EventType          string
	EffectType         string
	ProducedEventTypes []string
}

// Projector derives the user-facing status view from persisted runtime state.
// Persistence remains the recovery source of truth; the projection contains no
// execution decisions.
type Projector struct {
	DefaultWorkDir string
}

func (p Projector) Project(ctx context.Context, storage persistence.RuntimeStorage, queue *persistence.WorkQueue, runID string, update StatusUpdate) (RunStatus, error) {
	if storage == nil {
		return RunStatus{}, fmt.Errorf("status projector: storage is required")
	}
	if queue == nil {
		return RunStatus{}, fmt.Errorf("status projector: work queue is required")
	}
	status := RunStatus{
		RunID: runID, AdvanceStatus: update.AdvanceStatus,
		EventType: update.EventType, EffectType: update.EffectType,
		ProducedEventTypes: append([]string(nil), update.ProducedEventTypes...),
	}

	state, ok, err := storage.TaskStates().Load(ctx, runID)
	if err != nil {
		return RunStatus{}, err
	}
	if ok {
		status.Phase = strings.ToLower(string(state.Phase))
		status.FinalAnswer = ResultText(state.Result)
		status.WorkDir = strings.TrimSpace(state.Metadata["work_dir"])
		status.WaitingReason, status.WaitingTarget = waitingStatus(state)
		if state.LastError != nil {
			status.Error = state.LastError.Message
			if status.Error == "" {
				status.Error = state.LastError.Code
			}
		}
		if snapshot, found, snapshotErr := storage.AgentSnapshots().Load(ctx, state.Agent.Name, runID); snapshotErr == nil && found {
			status.StepsUsed = snapshot.StepIndex
		}
	} else {
		status.Phase = strings.ToLower(string(statemachine.PhaseCreated))
	}
	if status.WorkDir == "" {
		status.WorkDir = strings.TrimSpace(p.DefaultWorkDir)
	}
	pendingEvents, pendingEffects, err := queue.PendingCounts(ctx, runID)
	if err != nil {
		return RunStatus{}, err
	}
	status.PendingEvents = pendingEvents
	status.PendingEffects = pendingEffects
	return status, nil
}

func waitingStatus(state statemachine.TaskState) (string, string) {
	switch state.Phase {
	case statemachine.PhaseWaitingModel:
		if state.PendingModel != nil {
			return "model", state.PendingModel.ModelCallID
		}
		return "model", ""
	case statemachine.PhaseWaitingTool:
		if state.PendingTool != nil {
			return "tool", state.PendingTool.ToolName
		}
		return "tool", ""
	case statemachine.PhaseWaitingUserInput:
		if state.PendingUserInput != nil {
			return "user_input", state.PendingUserInput.RequestID
		}
		return "user_input", ""
	case statemachine.PhaseWaitingSubAgent:
		if state.PendingSubAgent != nil {
			return "sub_agent", state.PendingSubAgent.SubTaskID
		}
		return "sub_agent", ""
	default:
		return "", ""
	}
}

func ResultText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var envelope struct {
		Answer      string `json:"answer"`
		FinalAnswer string `json:"final_answer"`
		Summary     string `json:"summary"`
	}
	if err := json.Unmarshal(raw, &envelope); err == nil {
		switch {
		case strings.TrimSpace(envelope.Answer) != "":
			return envelope.Answer
		case strings.TrimSpace(envelope.FinalAnswer) != "":
			return envelope.FinalAnswer
		case strings.TrimSpace(envelope.Summary) != "":
			return envelope.Summary
		}
	}
	var pretty any
	if err := json.Unmarshal(raw, &pretty); err == nil {
		if formatted, err := json.MarshalIndent(pretty, "", "  "); err == nil {
			return string(formatted)
		}
	}
	return string(raw)
}

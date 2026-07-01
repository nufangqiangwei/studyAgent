package state

import (
	"context"
	"encoding/json"
	"fmt"

	runtimeevent "agent/internal/event"
)

type CoreRunReducer struct{}

func (r CoreRunReducer) Match(ctx context.Context, s RunState, event runtimeevent.Event) bool {
	switch event.Type {
	case runtimeevent.EventRunStarted,
		runtimeevent.EventRunCompleted,
		runtimeevent.EventRunFailed,
		runtimeevent.EventRunCancelled,
		runtimeevent.EventWaitStarted,
		runtimeevent.EventWaitEnded,
		runtimeevent.EventStepLimitReached:
		return true
	default:
		return false
	}
}

func (r CoreRunReducer) Reduce(ctx context.Context, s RunState, event runtimeevent.Event) (RunState, []Effect, error) {
	switch event.Type {
	case runtimeevent.EventRunStarted:
		if s.Phase != PhaseIdle {
			return s, nil, fmt.Errorf("cannot start run from phase %q", s.Phase)
		}

		s.Phase = PhaseWaiting
		s.Waiting = &WaitingState{Reason: "model_result"}
		return s, []Effect{
			NewEffect(s.RunID, EffectCallModel),
		}, nil

	case runtimeevent.EventRunCompleted:
		if s.IsTerminal() {
			return s, nil, nil
		}

		s.Phase = PhaseCompleted
		s.Waiting = nil
		s.Error = nil
		return s, []Effect{
			NewEffect(s.RunID, EffectFinalize),
		}, nil

	case runtimeevent.EventRunFailed:
		if s.IsTerminal() {
			return s, nil, nil
		}

		s.Phase = PhaseFailed
		s.Waiting = nil
		s.Error = errorFromPayload(event.Payload)
		return s, nil, nil

	case runtimeevent.EventRunCancelled:
		if s.IsTerminal() {
			return s, nil, nil
		}

		s.Phase = PhaseCancelled
		s.Waiting = nil
		return s, nil, nil

	case runtimeevent.EventWaitStarted:
		if s.IsTerminal() {
			return s, nil, fmt.Errorf("cannot wait from terminal phase %q", s.Phase)
		}

		s.Phase = PhaseWaiting
		s.Waiting = waitingFromPayload(event.Payload)
		return s, nil, nil

	case runtimeevent.EventWaitEnded:
		if s.Phase != PhaseWaiting {
			return s, nil, fmt.Errorf("cannot end wait from phase %q", s.Phase)
		}

		s.Phase = PhaseRunning
		s.Waiting = nil
		return s, nil, nil

	case runtimeevent.EventStepLimitReached:
		if s.IsTerminal() {
			return s, nil, nil
		}

		s.Phase = PhaseFailed
		s.Waiting = nil
		s.Error = &ErrorState{
			Code:    "step_limit_hit",
			Message: "run stopped because max step limit was reached",
		}
		return s, nil, nil
	}

	return s, nil, nil
}

func waitingFromPayload(raw json.RawMessage) *WaitingState {
	if len(raw) == 0 {
		return &WaitingState{Reason: "unknown"}
	}

	var w WaitingState
	if err := json.Unmarshal(raw, &w); err != nil {
		return &WaitingState{Reason: "unknown"}
	}

	if w.Reason == "" {
		w.Reason = "unknown"
	}

	return &w
}

func errorFromPayload(raw json.RawMessage) *ErrorState {
	if len(raw) == 0 {
		return &ErrorState{
			Code:    "run_failed",
			Message: "run failed",
		}
	}

	var e ErrorState
	if err := json.Unmarshal(raw, &e); err != nil {
		return &ErrorState{
			Code:    "run_failed",
			Message: string(raw),
		}
	}

	if e.Code == "" {
		e.Code = "run_failed"
	}
	if e.Message == "" {
		e.Message = "run failed"
	}

	return &e
}

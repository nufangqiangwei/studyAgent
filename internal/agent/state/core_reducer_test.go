package state

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	runtimeevent "agent/internal/event"
)

func TestCoreRunReducerTransitions(t *testing.T) {
	ctx := context.Background()
	reducer := CoreRunReducer{}

	st := NewRunState("run_1", 3)

	started, effects, err := reducer.Reduce(ctx, st, mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil))
	if err != nil {
		t.Fatalf("RunStarted returned error: %v", err)
	}
	if started.Phase != PhaseRunning {
		t.Fatalf("started phase = %q, want %q", started.Phase, PhaseRunning)
	}
	if len(effects) != 1 || effects[0].Type != EffectCallModel {
		t.Fatalf("started effects = %#v, want model.call", effects)
	}

	completed, effects, err := reducer.Reduce(ctx, started, mustRuntimeEvent(t, "run_1", runtimeevent.EventRunCompleted, nil))
	if err != nil {
		t.Fatalf("RunCompleted returned error: %v", err)
	}
	if completed.Phase != PhaseCompleted {
		t.Fatalf("completed phase = %q, want %q", completed.Phase, PhaseCompleted)
	}
	if len(effects) != 1 || effects[0].Type != EffectFinalize {
		t.Fatalf("completed effects = %#v, want run.finalize", effects)
	}
}

func TestCoreRunReducerFailureCancellationWaitingAndStepLimit(t *testing.T) {
	ctx := context.Background()
	reducer := CoreRunReducer{}

	failedPayload := json.RawMessage(`{"code":"model_error","message":"model failed","retry_count":2}`)
	failed, effects, err := reducer.Reduce(ctx, runningState("run_1"), mustRuntimeEvent(t, "run_1", runtimeevent.EventRunFailed, failedPayload))
	if err != nil {
		t.Fatalf("RunFailed returned error: %v", err)
	}
	if failed.Phase != PhaseFailed || failed.Error == nil {
		t.Fatalf("failed state = %#v, want failed with error", failed)
	}
	if failed.Error.Code != "model_error" || failed.Error.Message != "model failed" || failed.Error.RetryCount != 2 {
		t.Fatalf("failed error = %#v, want payload error", failed.Error)
	}
	if len(effects) != 0 {
		t.Fatalf("failed effects = %#v, want none", effects)
	}

	cancelled, _, err := reducer.Reduce(ctx, runningState("run_1"), mustRuntimeEvent(t, "run_1", runtimeevent.EventRunCancelled, nil))
	if err != nil {
		t.Fatalf("RunCancelled returned error: %v", err)
	}
	if cancelled.Phase != PhaseCancelled {
		t.Fatalf("cancelled phase = %q, want %q", cancelled.Phase, PhaseCancelled)
	}

	waitPayload := json.RawMessage(`{"reason":"child_agents","target":"group:research"}`)
	waiting, _, err := reducer.Reduce(ctx, runningState("run_1"), mustRuntimeEvent(t, "run_1", runtimeevent.EventWaitStarted, waitPayload))
	if err != nil {
		t.Fatalf("WaitStarted returned error: %v", err)
	}
	if waiting.Phase != PhaseWaiting || waiting.Waiting == nil {
		t.Fatalf("waiting state = %#v, want waiting details", waiting)
	}
	if waiting.Waiting.Reason != "child_agents" || waiting.Waiting.Target != "group:research" {
		t.Fatalf("waiting = %#v, want child_agents/group:research", waiting.Waiting)
	}

	resumed, _, err := reducer.Reduce(ctx, waiting, mustRuntimeEvent(t, "run_1", runtimeevent.EventWaitEnded, nil))
	if err != nil {
		t.Fatalf("WaitEnded returned error: %v", err)
	}
	if resumed.Phase != PhaseRunning || resumed.Waiting != nil {
		t.Fatalf("resumed state = %#v, want running without waiting", resumed)
	}

	limitHit, _, err := reducer.Reduce(ctx, runningState("run_1"), mustRuntimeEvent(t, "run_1", runtimeevent.EventStepLimitReached, nil))
	if err != nil {
		t.Fatalf("StepLimitReached returned error: %v", err)
	}
	if limitHit.Phase != PhaseFailed || limitHit.Error == nil || limitHit.Error.Code != "step_limit_hit" {
		t.Fatalf("limit state = %#v, want failed step_limit_hit", limitHit)
	}
}

func TestCoreRunReducerIllegalTransitions(t *testing.T) {
	ctx := context.Background()
	reducer := CoreRunReducer{}

	completed := NewRunState("run_1", 1)
	completed.Phase = PhaseCompleted
	next, effects, err := reducer.Reduce(ctx, completed, mustRuntimeEvent(t, "run_1", runtimeevent.EventRunStarted, nil))
	if err == nil {
		t.Fatal("completed + RunStarted returned nil error")
	}
	if !strings.Contains(err.Error(), "cannot start run") {
		t.Fatalf("error = %q, want cannot start run", err.Error())
	}
	if next.Phase != PhaseCompleted {
		t.Fatalf("phase = %q, want %q", next.Phase, PhaseCompleted)
	}
	if len(effects) != 0 {
		t.Fatalf("effects = %#v, want none", effects)
	}

	idle := NewRunState("run_1", 1)
	_, _, err = reducer.Reduce(ctx, idle, mustRuntimeEvent(t, "run_1", runtimeevent.EventWaitEnded, nil))
	if err == nil {
		t.Fatal("idle + WaitEnded returned nil error")
	}
	if !strings.Contains(err.Error(), "cannot end wait") {
		t.Fatalf("error = %q, want cannot end wait", err.Error())
	}
}

func runningState(runID string) RunState {
	st := NewRunState(runID, 3)
	st.Phase = PhaseRunning
	return st
}

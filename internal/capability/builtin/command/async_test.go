package command

import (
	"agent/internal/content"
	"context"
	"strings"
	"testing"
)

func TestRunCommandSubmitsAsyncAgent(t *testing.T) {
	var out strings.Builder
	runner := &recordingAsyncRunner{
		submitStatus: content.AsyncRunStatus{
			RunID:         "run_1",
			Phase:         "idle",
			PendingEvents: 1,
		},
	}
	env := content.Env{
		IO:    content.IO{Out: &out},
		Agent: runner,
	}

	if err := (Run{}).Execute(context.Background(), env, []string{"hello", "async"}); err != nil {
		t.Fatalf("Run.Execute returned error: %v", err)
	}
	if runner.submittedTask != "hello async" {
		t.Fatalf("submitted task = %q, want hello async", runner.submittedTask)
	}
	if !strings.Contains(out.String(), "Submitted run: run_1") {
		t.Fatalf("output missing submitted run:\n%s", out.String())
	}
	if runner.syncRunTask != "" {
		t.Fatalf("sync Run called with %q, want async submit only", runner.syncRunTask)
	}
}

func TestStepCommandProcessesOneEventBeforeEffects(t *testing.T) {
	var out strings.Builder
	runner := &recordingAsyncRunner{
		resultStatus: content.AsyncRunStatus{
			RunID:          "run_1",
			Phase:          "waiting",
			PendingEvents:  1,
			PendingEffects: 1,
		},
		advanceStatus: content.AsyncRunStatus{
			RunID:          "run_1",
			AdvanceStatus:  "event_processed",
			Phase:          "waiting",
			EventType:      "RunStarted",
			PendingEvents:  0,
			PendingEffects: 1,
		},
	}
	env := content.Env{
		IO:    content.IO{Out: &out},
		Agent: runner,
	}

	if err := (Step{}).Execute(context.Background(), env, []string{"run_1"}); err != nil {
		t.Fatalf("Step.Execute returned error: %v", err)
	}
	if runner.advancedRunID != "run_1" {
		t.Fatalf("advanced run id = %q, want run_1", runner.advancedRunID)
	}
	if runner.dispatchedRunID != "" {
		t.Fatalf("dispatched run id = %q, want no effect dispatch", runner.dispatchedRunID)
	}
	if !strings.Contains(out.String(), "Event: RunStarted") {
		t.Fatalf("output missing event:\n%s", out.String())
	}
}

func TestWorkCommandRunsOneGlobalTick(t *testing.T) {
	var out strings.Builder
	runner := &recordingAsyncRunner{
		workResult: content.AsyncWorkResult{
			Ran: true,
			Status: content.AsyncRunStatus{
				RunID:          "run_1",
				AdvanceStatus:  "effect_dispatched",
				Phase:          "waiting",
				EffectType:     "model.call",
				PendingEvents:  1,
				PendingEffects: 0,
			},
		},
	}
	env := content.Env{
		IO:    content.IO{Out: &out},
		Agent: runner,
	}

	if err := (Work{}).Execute(context.Background(), env, nil); err != nil {
		t.Fatalf("Work.Execute returned error: %v", err)
	}
	if !runner.worked {
		t.Fatal("Work was not called")
	}
	if !strings.Contains(out.String(), "Worked run: run_1") || !strings.Contains(out.String(), "Effect: model.call") {
		t.Fatalf("output missing work status:\n%s", out.String())
	}
}

func TestInputAndApproveCommandsQueueEvents(t *testing.T) {
	var out strings.Builder
	runner := &recordingAsyncRunner{
		inputStatus: content.AsyncRunStatus{
			RunID:         "run_1",
			AdvanceStatus: "event_enqueued",
			Phase:         "waiting",
			EventType:     "UserInputReceived",
		},
		approvalStatus: content.AsyncRunStatus{
			RunID:         "run_1",
			AdvanceStatus: "event_enqueued",
			Phase:         "waiting",
			EventType:     "UserApprovalReceived",
		},
	}
	env := content.Env{
		IO:    content.IO{Out: &out},
		Agent: runner,
	}

	if err := (Input{}).Execute(context.Background(), env, []string{"run_1", "backend"}); err != nil {
		t.Fatalf("Input.Execute returned error: %v", err)
	}
	if runner.inputRunID != "run_1" || runner.inputAnswer != "backend" {
		t.Fatalf("input = %q/%q, want run_1/backend", runner.inputRunID, runner.inputAnswer)
	}

	if err := (Approve{}).Execute(context.Background(), env, []string{"run_1", "yes", "looks", "ok"}); err != nil {
		t.Fatalf("Approve.Execute returned error: %v", err)
	}
	if runner.approvalRunID != "run_1" || !runner.approved || runner.approvalReason != "looks ok" {
		t.Fatalf("approval = %q/%t/%q, want run_1/true/looks ok", runner.approvalRunID, runner.approved, runner.approvalReason)
	}
	got := out.String()
	for _, want := range []string{"Input queued run: run_1", "Approval queued run: run_1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

type recordingAsyncRunner struct {
	syncRunTask string

	submittedTask string
	submitStatus  content.AsyncRunStatus

	recoverResult content.AsyncRecoverResult
	worked        bool
	workResult    content.AsyncWorkResult

	resultRunID  string
	resultStatus content.AsyncRunStatus

	advancedRunID string
	advanceStatus content.AsyncRunStatus

	dispatchedRunID string
	dispatchStatus  content.AsyncRunStatus

	inputRunID  string
	inputAnswer string
	inputStatus content.AsyncRunStatus

	approvalRunID  string
	approved       bool
	approvalReason string
	approvalStatus content.AsyncRunStatus
}

func (r *recordingAsyncRunner) Run(_ context.Context, task string) error {
	r.syncRunTask = task
	return nil
}

func (r *recordingAsyncRunner) Submit(_ context.Context, task string) (content.AsyncRunStatus, error) {
	r.submittedTask = task
	return r.submitStatus, nil
}

func (r *recordingAsyncRunner) Recover(context.Context) (content.AsyncRecoverResult, error) {
	return r.recoverResult, nil
}

func (r *recordingAsyncRunner) Work(context.Context) (content.AsyncWorkResult, error) {
	r.worked = true
	return r.workResult, nil
}

func (r *recordingAsyncRunner) Advance(_ context.Context, runID string) (content.AsyncRunStatus, error) {
	r.advancedRunID = runID
	return r.advanceStatus, nil
}

func (r *recordingAsyncRunner) DispatchNextEffect(_ context.Context, runID string) (content.AsyncRunStatus, error) {
	r.dispatchedRunID = runID
	return r.dispatchStatus, nil
}

func (r *recordingAsyncRunner) SubmitUserInput(_ context.Context, runID string, answer string) (content.AsyncRunStatus, error) {
	r.inputRunID = runID
	r.inputAnswer = answer
	return r.inputStatus, nil
}

func (r *recordingAsyncRunner) SubmitUserApproval(_ context.Context, runID string, approved bool, reason string) (content.AsyncRunStatus, error) {
	r.approvalRunID = runID
	r.approved = approved
	r.approvalReason = reason
	return r.approvalStatus, nil
}

func (r *recordingAsyncRunner) Result(_ context.Context, runID string) (content.AsyncRunStatus, error) {
	r.resultRunID = runID
	return r.resultStatus, nil
}

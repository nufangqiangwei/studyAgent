package command

import (
	"agent/internal/content"
	"context"
	"fmt"
	"strings"
)

type Recover struct{}

func (Recover) Name() string {
	return "recover"
}

func (Recover) Description() string {
	return "enqueue one resume event for a persisted async run"
}

func (Recover) Execute(ctx context.Context, env content.Env, _ []string) error {
	async, err := asyncAgent(env)
	if err != nil {
		return err
	}
	recovered, err := async.Recover(ctx)
	if err != nil {
		return err
	}
	if len(recovered.Runs) == 0 {
		_, err = fmt.Fprintln(env.IO.Out, "No recoverable runs.")
		return err
	}
	for _, run := range recovered.Runs {
		if err := printAsyncStatus(env, "Recovered", run); err != nil {
			return err
		}
	}
	return nil
}

type Work struct{}

func (Work) Name() string {
	return "work"
}

func (Work) Description() string {
	return "run one global async worker tick"
}

func (Work) Execute(ctx context.Context, env content.Env, _ []string) error {
	async, err := asyncAgent(env)
	if err != nil {
		return err
	}
	result, err := async.Work(ctx)
	if err != nil {
		return err
	}
	if !result.Ran {
		_, err = fmt.Fprintln(env.IO.Out, "No async work ready.")
		return err
	}
	return printAsyncStatus(env, "Worked", result.Status)
}

type Advance struct{}

func (Advance) Name() string {
	return "advance"
}

func (Advance) Description() string {
	return "process one queued runtime event for a run"
}

func (Advance) Execute(ctx context.Context, env content.Env, args []string) error {
	runID, err := requiredRunID("advance", args)
	if err != nil {
		return err
	}
	async, err := asyncAgent(env)
	if err != nil {
		return err
	}
	status, err := async.Advance(ctx, runID)
	if err != nil {
		return err
	}
	return printAsyncStatus(env, "Advanced", status)
}

type Effect struct{}

func (Effect) Name() string {
	return "effect"
}

func (Effect) Description() string {
	return "dispatch one queued runtime effect for a run"
}

func (Effect) Execute(ctx context.Context, env content.Env, args []string) error {
	runID, err := requiredRunID("effect", args)
	if err != nil {
		return err
	}
	async, err := asyncAgent(env)
	if err != nil {
		return err
	}
	status, err := async.DispatchNextEffect(ctx, runID)
	if err != nil {
		return err
	}
	return printAsyncStatus(env, "Effect", status)
}

type Step struct{}

func (Step) Name() string {
	return "step"
}

func (Step) Description() string {
	return "advance one async run by exactly one event or one effect"
}

func (Step) Execute(ctx context.Context, env content.Env, args []string) error {
	async, err := asyncAgent(env)
	if err != nil {
		return err
	}

	runID, err := requiredRunID("step", args)
	if err != nil {
		return err
	}
	current, err := async.Result(ctx, runID)
	if err != nil {
		return err
	}
	if current.PendingEvents > 0 {
		next, err := async.Advance(ctx, runID)
		if err != nil {
			return err
		}
		return printAsyncStatus(env, "Advanced", next)
	}
	if current.PendingEffects > 0 {
		next, err := async.DispatchNextEffect(ctx, runID)
		if err != nil {
			return err
		}
		return printAsyncStatus(env, "Effect", next)
	}
	return printAsyncStatus(env, "Idle", current)
}

type Result struct{}

func (Result) Name() string {
	return "result"
}

func (Result) Description() string {
	return "show async run state and final answer"
}

func (Result) Execute(ctx context.Context, env content.Env, args []string) error {
	runID, err := requiredRunID("result", args)
	if err != nil {
		return err
	}
	async, err := asyncAgent(env)
	if err != nil {
		return err
	}
	status, err := async.Result(ctx, runID)
	if err != nil {
		return err
	}
	return printAsyncStatus(env, "Result", status)
}

type Input struct{}

func (Input) Name() string {
	return "input"
}

func (Input) Description() string {
	return "submit a user input event for a waiting run"
}

func (Input) Execute(ctx context.Context, env content.Env, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("input requires a run id and answer")
	}
	runID := strings.TrimSpace(args[0])
	answer := strings.TrimSpace(strings.Join(args[1:], " "))
	if runID == "" {
		return fmt.Errorf("input requires a run id")
	}
	async, err := asyncAgent(env)
	if err != nil {
		return err
	}
	status, err := async.SubmitUserInput(ctx, runID, answer)
	if err != nil {
		return err
	}
	return printAsyncStatus(env, "Input queued", status)
}

type Approve struct{}

func (Approve) Name() string {
	return "approve"
}

func (Approve) Description() string {
	return "submit a user approval event for a waiting run"
}

func (Approve) Execute(ctx context.Context, env content.Env, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("approve requires a run id and yes/no decision")
	}
	runID := strings.TrimSpace(args[0])
	if runID == "" {
		return fmt.Errorf("approve requires a run id")
	}
	approved, err := parseApprovalDecision(args[1])
	if err != nil {
		return err
	}
	reason := ""
	if len(args) > 2 {
		reason = strings.TrimSpace(strings.Join(args[2:], " "))
	}
	async, err := asyncAgent(env)
	if err != nil {
		return err
	}
	status, err := async.SubmitUserApproval(ctx, runID, approved, reason)
	if err != nil {
		return err
	}
	return printAsyncStatus(env, "Approval queued", status)
}

func asyncAgent(env content.Env) (content.AsyncAgentRunner, error) {
	if env.Agent == nil {
		return nil, fmt.Errorf("async runtime: agent runner is not configured")
	}
	async, ok := env.Agent.(content.AsyncAgentRunner)
	if !ok {
		return nil, fmt.Errorf("async runtime: active agent does not support async execution")
	}
	return async, nil
}

func parseApprovalDecision(input string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "y", "yes", "true", "approve", "approved":
		return true, nil
	case "n", "no", "false", "deny", "denied":
		return false, nil
	default:
		return false, fmt.Errorf("approval decision must be yes or no")
	}
}

func requiredRunID(commandName string, args []string) (string, error) {
	runID := strings.TrimSpace(strings.Join(args, " "))
	if runID == "" {
		return "", fmt.Errorf("%s requires a run id", commandName)
	}
	return runID, nil
}

func printAsyncStatus(env content.Env, label string, status content.AsyncRunStatus) error {
	if _, err := fmt.Fprintf(env.IO.Out, "%s run: %s\n", label, status.RunID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Phase: %s\n", status.Phase); err != nil {
		return err
	}
	if status.WorkDir != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Workspace: %s\n", status.WorkDir); err != nil {
			return err
		}
	}
	if status.AdvanceStatus != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Advance: %s\n", status.AdvanceStatus); err != nil {
			return err
		}
	}
	if status.EventType != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Event: %s\n", status.EventType); err != nil {
			return err
		}
	}
	if status.EffectType != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Effect: %s\n", status.EffectType); err != nil {
			return err
		}
	}
	if len(status.ProducedEventTypes) > 0 {
		if _, err := fmt.Fprintf(env.IO.Out, "Produced events: %s\n", strings.Join(status.ProducedEventTypes, ", ")); err != nil {
			return err
		}
	}
	if status.WaitingReason != "" {
		waiting := status.WaitingReason
		if status.WaitingTarget != "" {
			waiting += " " + status.WaitingTarget
		}
		if _, err := fmt.Fprintf(env.IO.Out, "Waiting: %s\n", waiting); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Pending events: %d\n", status.PendingEvents); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Pending effects: %d\n", status.PendingEffects); err != nil {
		return err
	}
	if status.StepsUsed > 0 {
		if _, err := fmt.Fprintf(env.IO.Out, "Steps used: %d\n", status.StepsUsed); err != nil {
			return err
		}
	}
	if status.Error != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Error: %s\n", status.Error); err != nil {
			return err
		}
	}
	if strings.TrimSpace(status.FinalAnswer) != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Final answer:\n%s\n", status.FinalAnswer); err != nil {
			return err
		}
	}
	return nil
}

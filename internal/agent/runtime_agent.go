package agent

import (
	"agent/internal/agent/runner"
	"agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/foundation/llmClient"
	"agent/internal/llm"
	"agent/internal/prompt"
	"agent/internal/state"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

type runtimeAgentParts struct {
	runtime          *llm.Runtime
	promptBuilder    *prompt.NativeBuilder
	toolRegistry     *tool.Manage
	tools            []tool.Tool
	profile          llm.AgentProfile
	workPath         string
	in               io.Reader
	out              io.Writer
	maxSteps         int
	runtimeStoreRoot string
}

func newRuntimeAgentParts(ctx context.Context, opts CreatAgentOptions, agentName string, promptOptions prompt.Options) (runtimeAgentParts, error) {
	_ = ctx
	toolManage, err := tool.NewDefaultManage(tool.WithPolicy(opts.Policy), tool.WithAsyncPolicyApproval())
	if err != nil {
		return runtimeAgentParts{}, fmt.Errorf("%s agent: select tools: %w", agentName, err)
	}
	registeredTools := toolManage.List()

	rt, err := llm.NewRuntime(llm.Options{
		LLM:    opts.LLM,
		Logger: opts.Logger,
	})
	if err != nil {
		return runtimeAgentParts{}, fmt.Errorf("%s agent: create llm: %w", agentName, err)
	}

	if promptOptions.Model == "" {
		promptOptions.Model = opts.Model
	}
	builder := prompt.NewNativeBuilder(promptOptions)

	return runtimeAgentParts{
		runtime:       rt,
		promptBuilder: builder,
		toolRegistry:  toolManage,
		tools:         registeredTools,
		profile: llm.AgentProfile{
			Name:  agentName,
			Model: opts.Model,
			Tools: toolDefinitions(registeredTools),
		},
		workPath:         opts.WorkDir,
		in:               opts.In,
		out:              opts.Out,
		maxSteps:         opts.MaxSteps,
		runtimeStoreRoot: resolveRuntimeStoreRoot(opts.RuntimeStoreRoot, opts.Session),
	}, nil
}

func runRuntimeAgent(ctx context.Context, parts runtimeAgentParts, userInput string) error {
	if parts.runtime == nil || parts.promptBuilder == nil {
		return fmt.Errorf("agent llm: not initialized")
	}

	task, err := buildRuntimeTask(ctx, parts, userInput)
	if err != nil {
		return err
	}

	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return err
	}

	result, err := reactRunner.Run(ctx, task)
	if err != nil {
		return err
	}
	if parts.out != nil && strings.TrimSpace(result.FinalAnswer) != "" {
		if _, err := fmt.Fprintln(parts.out, result.FinalAnswer); err != nil {
			return fmt.Errorf("write agent output: %w", err)
		}
	}
	return nil
}

func submitRuntimeAgent(ctx context.Context, parts runtimeAgentParts, userInput string) (content.AsyncRunStatus, error) {
	if parts.runtime == nil || parts.promptBuilder == nil {
		return content.AsyncRunStatus{}, fmt.Errorf("agent llm: not initialized")
	}
	task, err := buildRuntimeTask(ctx, parts, userInput)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	runID, err := reactRunner.Submit(ctx, task)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return runtimeStatus(ctx, reactRunner, runID, runner.LoopAdvanceResult{})
}

func recoverRuntimeAgent(ctx context.Context, parts runtimeAgentParts) (content.AsyncRecoverResult, error) {
	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return content.AsyncRecoverResult{}, err
	}
	recovered, err := reactRunner.Recover(ctx)
	if err != nil {
		return content.AsyncRecoverResult{}, err
	}
	statuses := make([]content.AsyncRunStatus, 0, len(recovered.Runs))
	for _, run := range recovered.Runs {
		status, err := runtimeStatus(ctx, reactRunner, runner.RunID(run.RunID), runner.LoopAdvanceResult{})
		if err != nil {
			return content.AsyncRecoverResult{}, err
		}
		status.PendingEvents = run.PendingEvents
		status.PendingEffects = run.PendingEffects
		statuses = append(statuses, status)
	}
	return content.AsyncRecoverResult{Runs: statuses}, nil
}

func workRuntimeAgent(ctx context.Context, parts runtimeAgentParts) (content.AsyncWorkResult, error) {
	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return content.AsyncWorkResult{}, err
	}
	work, ok, err := reactRunner.NextPendingWork(ctx)
	if err != nil {
		return content.AsyncWorkResult{}, err
	}
	if !ok {
		return content.AsyncWorkResult{}, nil
	}
	runID := runner.RunID(work.RunID)
	ctx, err = bindStoredRunContext(ctx, parts, reactRunner, runID)
	if err != nil {
		return content.AsyncWorkResult{}, err
	}

	var advanced runner.LoopAdvanceResult
	switch work.Kind {
	case runner.PendingWorkEvent:
		advanced, err = reactRunner.Advance(ctx, runID)
	case runner.PendingWorkEffect:
		advanced, err = reactRunner.DispatchNextEffect(ctx, runID)
	default:
		err = fmt.Errorf("agent runtime: unsupported pending work kind %q", work.Kind)
	}
	if err != nil {
		return content.AsyncWorkResult{}, err
	}
	status, err := runtimeStatus(ctx, reactRunner, runID, advanced)
	if err != nil {
		return content.AsyncWorkResult{}, err
	}
	return content.AsyncWorkResult{
		Ran:    advanced.Status == runner.AdvanceStatusEventProcessed || advanced.Status == runner.AdvanceStatusEffectDispatched,
		Status: status,
	}, nil
}

func advanceRuntimeAgent(ctx context.Context, parts runtimeAgentParts, runID string) (content.AsyncRunStatus, error) {
	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	ctx, err = bindStoredRunContext(ctx, parts, reactRunner, runner.RunID(runID))
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	advanced, err := reactRunner.Advance(ctx, runner.RunID(runID))
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return runtimeStatus(ctx, reactRunner, runner.RunID(runID), advanced)
}

func dispatchRuntimeAgentEffect(ctx context.Context, parts runtimeAgentParts, runID string) (content.AsyncRunStatus, error) {
	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	ctx, err = bindStoredRunContext(ctx, parts, reactRunner, runner.RunID(runID))
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	dispatched, err := reactRunner.DispatchNextEffect(ctx, runner.RunID(runID))
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return runtimeStatus(ctx, reactRunner, runner.RunID(runID), dispatched)
}

func submitRuntimeAgentUserInput(ctx context.Context, parts runtimeAgentParts, runID string, answer string) (content.AsyncRunStatus, error) {
	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	submitted, err := reactRunner.SubmitUserInput(ctx, runner.RunID(runID), answer)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return runtimeStatus(ctx, reactRunner, runner.RunID(runID), submitted)
}

func submitRuntimeAgentUserApproval(ctx context.Context, parts runtimeAgentParts, runID string, approved bool, reason string) (content.AsyncRunStatus, error) {
	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	submitted, err := reactRunner.SubmitUserApproval(ctx, runner.RunID(runID), approved, reason)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return runtimeStatus(ctx, reactRunner, runner.RunID(runID), submitted)
}

func runtimeAgentResult(ctx context.Context, parts runtimeAgentParts, runID string) (content.AsyncRunStatus, error) {
	reactRunner, err := newRuntimeRunner(parts)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	return runtimeStatus(ctx, reactRunner, runner.RunID(runID), runner.LoopAdvanceResult{})
}

func buildRuntimeTask(ctx context.Context, parts runtimeAgentParts, userInput string) (runner.Task, error) {
	promptOutput, err := parts.promptBuilder.Build(ctx, prompt.Input{
		Task:    userInput,
		WorkDir: parts.workPath,
	})
	if err != nil {
		return runner.Task{}, err
	}

	profile := parts.profile
	profile.Model = promptOutput.Model
	profile.Temperature = promptOutput.Temperature
	return runner.Task{
		Input:    userInput,
		WorkDir:  parts.workPath,
		Agent:    profile,
		Messages: promptOutput.Messages,
		MaxSteps: parts.maxSteps,
	}, nil
}

func newRuntimeRunner(parts runtimeAgentParts) (*runner.AgentRunner, error) {
	runnerOpts := runner.Options{
		LLM:                    parts.runtime,
		ToolRegistry:           parts.toolRegistry,
		MaxSteps:               parts.maxSteps,
		SuspendUserInteraction: true,
		UserInteraction:        newConsoleUserInteraction(parts.in, parts.out),
	}
	if parts.runtimeStoreRoot != "" {
		stores, err := state.NewFileStore(parts.runtimeStoreRoot)
		if err != nil {
			return nil, fmt.Errorf("create runtime state store: %w", err)
		}
		runnerOpts.StateStore = stores.States
		runnerOpts.EventStore = stores.Events
		runnerOpts.EffectStore = stores.Effects
		runnerOpts.EventInbox = stores.Inbox
	}
	return runner.NewAgentRunner(runnerOpts)
}

func runtimeStatus(ctx context.Context, reactRunner *runner.AgentRunner, runID runner.RunID, advance runner.LoopAdvanceResult) (content.AsyncRunStatus, error) {
	result, err := reactRunner.Result(ctx, runID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	pendingEvents, pendingEffects, err := reactRunner.PendingCounts(ctx, runID)
	if err != nil {
		return content.AsyncRunStatus{}, err
	}
	status := content.AsyncRunStatus{
		RunID:          result.RunID,
		AdvanceStatus:  string(advance.Status),
		Phase:          string(result.Status),
		FinalAnswer:    result.FinalAnswer,
		StepsUsed:      result.StepsUsed,
		WorkDir:        result.WorkDir,
		PendingEvents:  pendingEvents,
		PendingEffects: pendingEffects,
	}
	if result.State.Waiting != nil {
		status.WaitingReason = result.State.Waiting.Reason
		status.WaitingTarget = result.State.Waiting.Target
	}
	if result.Error != nil {
		status.Error = result.Error.Message
		if status.Error == "" {
			status.Error = result.Error.Code
		}
	}
	if advance.Event != nil {
		status.EventType = string(advance.Event.Type)
	}
	if advance.Effect != nil {
		status.EffectType = string(advance.Effect.Type)
	}
	for _, event := range advance.Events {
		status.ProducedEventTypes = append(status.ProducedEventTypes, string(event.Type))
	}
	return status, nil
}

func bindStoredRunContext(ctx context.Context, parts runtimeAgentParts, reactRunner *runner.AgentRunner, runID runner.RunID) (context.Context, error) {
	if reactRunner == nil {
		return ctx, fmt.Errorf("agent runtime runner is nil")
	}
	result, err := reactRunner.Result(ctx, runID)
	if err != nil {
		return ctx, err
	}
	workDir := strings.TrimSpace(result.WorkDir)
	if workDir == "" {
		workDir = strings.TrimSpace(parts.workPath)
	}
	if workDir == "" {
		return ctx, nil
	}
	return withRuntimeWorkDir(ctx, workDir), nil
}

func withRuntimeWorkDir(ctx context.Context, workDir string) context.Context {
	workDir = strings.TrimSpace(workDir)
	if workDir == "" {
		return ctx
	}
	nextCtx, _ := content.WithUpdatedEnv(ctx, func(env *content.Env) {
		env.Config.WorkDir = workDir
		env.EventScope.WorkDir = workDir
	})
	return nextCtx
}

type consoleUserInteraction struct {
	in  io.Reader
	out io.Writer
}

func newConsoleUserInteraction(in io.Reader, out io.Writer) runner.UserInteraction {
	if in == nil || out == nil {
		return nil
	}
	return consoleUserInteraction{in: in, out: out}
}

func (i consoleUserInteraction) ReceiveInput(ctx context.Context, request runner.UserInputRequestedPayload) (runner.UserInputReceivedPayload, error) {
	if err := ctx.Err(); err != nil {
		return runner.UserInputReceivedPayload{}, err
	}
	if strings.TrimSpace(request.Default) != "" {
		if _, err := fmt.Fprintf(i.out, "? %s [%s]\n> ", request.Question, request.Default); err != nil {
			return runner.UserInputReceivedPayload{}, fmt.Errorf("write user input prompt: %w", err)
		}
	} else {
		if _, err := fmt.Fprintf(i.out, "? %s\n> ", request.Question); err != nil {
			return runner.UserInputReceivedPayload{}, fmt.Errorf("write user input prompt: %w", err)
		}
	}

	line, readErr := bufio.NewReader(i.in).ReadString('\n')
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return runner.UserInputReceivedPayload{}, fmt.Errorf("read user input: %w", readErr)
	}
	answer := strings.TrimRight(line, "\r\n")
	usedDefault := false
	if answer == "" && request.Default != "" {
		answer = request.Default
		usedDefault = true
	}
	if answer == "" && errors.Is(readErr, io.EOF) {
		return runner.UserInputReceivedPayload{}, fmt.Errorf("no user input received")
	}
	return runner.UserInputReceivedPayload{
		ToolCallID:  request.ToolCallID,
		ToolName:    request.ToolName,
		Answer:      answer,
		UsedDefault: usedDefault,
	}, nil
}

func (i consoleUserInteraction) ReceiveApproval(ctx context.Context, request runner.UserApprovalRequiredPayload) (runner.UserApprovalReceivedPayload, error) {
	if err := ctx.Err(); err != nil {
		return runner.UserApprovalReceivedPayload{}, err
	}
	if _, err := fmt.Fprintf(i.out, "! Policy confirmation required: %s\nExecute %s? [y/N] ", request.Decision.Reason, policyRequestSummary(request)); err != nil {
		return runner.UserApprovalReceivedPayload{}, fmt.Errorf("write policy approval prompt: %w", err)
	}
	line, readErr := bufio.NewReader(i.in).ReadString('\n')
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return runner.UserApprovalReceivedPayload{}, fmt.Errorf("read policy approval: %w", readErr)
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return runner.UserApprovalReceivedPayload{
		ToolCallID: request.ToolCallID,
		ToolName:   request.ToolName,
		Approved:   answer == "y" || answer == "yes",
	}, nil
}

func policyRequestSummary(request runner.UserApprovalRequiredPayload) string {
	parts := []string{fmt.Sprintf("tool %q", request.ToolName)}
	if request.Request.Path != "" {
		parts = append(parts, "path "+request.Request.Path)
	}
	if len(request.Request.Command) > 0 {
		parts = append(parts, "command "+strings.Join(request.Request.Command, " "))
	}
	return strings.Join(parts, ", ")
}

type sessionDirProvider interface {
	SessionDir() string
}

func resolveRuntimeStoreRoot(configured string, recorder interface{}) string {
	if strings.TrimSpace(configured) != "" {
		return strings.TrimSpace(configured)
	}
	provider, ok := recorder.(sessionDirProvider)
	if !ok {
		return ""
	}
	sessionDir := strings.TrimSpace(provider.SessionDir())
	if sessionDir == "" {
		return ""
	}
	return filepath.Join(sessionDir, "runtime")
}

func toolDefinitions(tools []tool.Tool) []llmClient.ToolDefinition {
	if len(tools) == 0 {
		return nil
	}
	defs := make([]llmClient.ToolDefinition, 0, len(tools))
	for _, t := range tools {
		defs = append(defs, llmClient.ToolDefinition{
			Name:        t.Name(),
			Description: t.Description(),
			InputSchema: t.InputSchema(),
		})
	}
	return defs
}

func cloneTools(tools []tool.Tool) []tool.Tool {
	if len(tools) == 0 {
		return nil
	}
	return append([]tool.Tool(nil), tools...)
}

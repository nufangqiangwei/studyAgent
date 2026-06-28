package agent

import (
	"agent/internal/foundation/llmClient"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"agent/internal/prompt"
	"agent/internal/session"
)

func (l *NativeLoop) HandleEvent(ctx context.Context, event LoopEvent) (*LoopAdvanceResult, error) {
	if event == nil {
		return nil, fmt.Errorf("native loop: event is required")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	switch e := event.(type) {
	case RunStartedEvent:
		return l.handleRunStarted(ctx, e)
	case *RunStartedEvent:
		if e == nil {
			return nil, fmt.Errorf("native loop: run started event is nil")
		}
		return l.handleRunStarted(ctx, *e)
	case RunResumedEvent:
		return l.handleRunResumed(ctx, e)
	case *RunResumedEvent:
		if e == nil {
			return nil, fmt.Errorf("native loop: run resumed event is nil")
		}
		return l.handleRunResumed(ctx, *e)
	case RunCancelledEvent:
		return l.handleRunCancelled(ctx, e)
	case *RunCancelledEvent:
		if e == nil {
			return nil, fmt.Errorf("native loop: run cancelled event is nil")
		}
		return l.handleRunCancelled(ctx, *e)
	case ModelResponseReceivedEvent:
		return l.handleModelResponseReceived(ctx, e)
	case *ModelResponseReceivedEvent:
		if e == nil {
			return nil, fmt.Errorf("native loop: model response event is nil")
		}
		return l.handleModelResponseReceived(ctx, *e)
	case ModelResponseFailedEvent:
		return l.handleModelResponseFailed(ctx, e)
	case *ModelResponseFailedEvent:
		if e == nil {
			return nil, fmt.Errorf("native loop: model failure event is nil")
		}
		return l.handleModelResponseFailed(ctx, *e)
	case ToolCallCompletedEvent:
		return l.handleToolCallCompleted(ctx, e)
	case *ToolCallCompletedEvent:
		if e == nil {
			return nil, fmt.Errorf("native loop: tool completion event is nil")
		}
		return l.handleToolCallCompleted(ctx, *e)
	default:
		return nil, fmt.Errorf("native loop: unsupported event type %s", event.EventType())
	}
}

func (l *NativeLoop) handleRunStarted(ctx context.Context, event RunStartedEvent) (*LoopAdvanceResult, error) {
	if strings.TrimSpace(event.Task.Input) == "" {
		return nil, fmt.Errorf("native loop: task input is required")
	}

	now := time.Now().UTC()
	eventID, err := idOrNew(event.ID)
	if err != nil {
		return nil, err
	}
	runID, err := idOrNew(event.RunIDValue)
	if err != nil {
		return nil, err
	}
	turnID, err := session.NewID()
	if err != nil {
		return nil, err
	}

	promptOutput, err := l.promptBuilder.Build(ctx, prompt.Input{
		Task:    event.Task.Input,
		WorkDir: event.Task.WorkDir,
	})
	if err != nil {
		return nil, fmt.Errorf("build prompt: %w", err)
	}
	sessionRecords, err := l.loadSessionRecords(ctx)
	if err != nil {
		return nil, err
	}
	llmContext, err := l.contextBuilder.Build(ctx, ContextInput{
		Prompt:         promptOutput,
		SessionRecords: sessionRecords,
		History:        l.history,
		Tools:          l.toolDefs,
	})
	if err != nil {
		return nil, fmt.Errorf("build llm context: %w", err)
	}
	if llmContext == nil {
		return nil, fmt.Errorf("build llm context: nil context")
	}

	for _, msg := range llmContext.InitialMessages() {
		if err := l.saveMessage(ctx, event.Task, turnID, 0, msg); err != nil {
			return nil, err
		}
	}

	state := RunState{
		RunID:           runID,
		TaskID:          runID,
		TurnID:          turnID,
		Task:            event.Task,
		Status:          RunStatusCallingModel,
		CurrentStep:     1,
		Messages:        llmContext.History(),
		ToolDefinitions: cloneToolDefinitions(l.toolDefs),
		Model:           promptOutput.Model,
		Temperature:     promptOutput.Temperature,
		PromptDebugText: promptOutput.DebugText,
		LastEventID:     eventID,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	if err := l.saveEvent(ctx, state.Task, state.TurnID, 0, session.EventTypeRunStarted, map[string]any{
		"run_id":     state.RunID,
		"event_id":   eventID,
		"created_at": now,
	}); err != nil {
		return nil, err
	}
	action, err := l.planModelCall(ctx, &state)
	if err != nil {
		return nil, err
	}
	if err := l.saveRunState(ctx, state); err != nil {
		return nil, err
	}
	return advanceFromState(state, []LoopAction{action}, nil, false), nil
}

func (l *NativeLoop) handleRunResumed(ctx context.Context, event RunResumedEvent) (*LoopAdvanceResult, error) {
	state, err := l.loadRunState(ctx, event.RunIDValue)
	if err != nil {
		return nil, err
	}
	eventID, err := idOrNew(event.ID)
	if err != nil {
		return nil, err
	}
	state.LastEventID = eventID
	state.UpdatedAt = time.Now().UTC()
	if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeRunResumed, map[string]any{
		"run_id":   state.RunID,
		"event_id": eventID,
	}); err != nil {
		return nil, err
	}

	switch state.Status {
	case RunStatusCallingModel:
		action, err := l.planModelCall(ctx, &state)
		if err != nil {
			return nil, err
		}
		if err := l.saveRunState(ctx, state); err != nil {
			return nil, err
		}
		return advanceFromState(state, []LoopAction{action}, nil, false), nil
	case RunStatusWaitingForToolResult:
		actions := dispatchActionsFromPending(state)
		if err := l.saveRunState(ctx, state); err != nil {
			return nil, err
		}
		return advanceFromState(state, actions, nil, true), nil
	case RunStatusCompleted:
		result := resultFromState(state)
		return advanceFromState(state, nil, &result, false), nil
	default:
		if err := l.saveRunState(ctx, state); err != nil {
			return nil, err
		}
		return advanceFromState(state, nil, nil, isSuspendedStatus(state.Status)), nil
	}
}

func (l *NativeLoop) handleRunCancelled(ctx context.Context, event RunCancelledEvent) (*LoopAdvanceResult, error) {
	state, err := l.loadRunState(ctx, event.RunIDValue)
	if err != nil {
		return nil, err
	}
	eventID, err := idOrNew(event.ID)
	if err != nil {
		return nil, err
	}
	state.Status = RunStatusCancelled
	state.Summary = event.Reason
	state.LastEventID = eventID
	state.UpdatedAt = time.Now().UTC()
	if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeRunCancelled, map[string]any{
		"run_id":   state.RunID,
		"event_id": eventID,
		"reason":   event.Reason,
	}); err != nil {
		return nil, err
	}
	if err := l.saveRunState(ctx, state); err != nil {
		return nil, err
	}
	return advanceFromState(state, nil, nil, false), nil
}
func (l *NativeLoop) handleModelResponseReceived(ctx context.Context, event ModelResponseReceivedEvent) (*LoopAdvanceResult, error) {
	state, err := l.loadRunState(ctx, event.RunIDValue)
	if err != nil {
		return nil, err
	}
	eventID, err := idOrNew(event.ID)
	if err != nil {
		return nil, err
	}
	startedAt := event.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	completedAt := event.CompletedAt
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}

	state.Status = RunStatusObservingResult
	state.LastEventID = eventID
	state.UpdatedAt = completedAt
	state.LLMCalls++
	if event.Response.Usage != nil {
		state.Usage = state.Usage.Add(*event.Response.Usage)
	}
	if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeLLMResponse, llmResponseEventPayload{
		Response:    event.Response,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}); err != nil {
		return nil, err
	}

	toolCalls := normalizeToolCalls(event.Response.ToolCalls, state.CurrentStep)
	if msg, ok := assistantMessage(event.Response, toolCalls); ok {
		state.Messages = append(state.Messages, msg)
		if err := l.saveMessage(ctx, state.Task, state.TurnID, state.CurrentStep, msg); err != nil {
			return nil, err
		}
	}

	step := Step{
		Index:       state.CurrentStep,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		PromptText:  state.PromptDebugText,
		Output:      event.Response.Content,
	}
	state.FinalAnswer = event.Response.Content

	if len(toolCalls) == 0 {
		state.Steps = append(state.Steps, step)
		state.Status = RunStatusCompleted
		state.UpdatedAt = completedAt
		if err := l.saveUsageSummary(ctx, state.Task, state.TurnID, state.Usage, state.LLMCalls); err != nil {
			return nil, err
		}
		if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeRunCompleted, map[string]any{
			"run_id":       state.RunID,
			"event_id":     eventID,
			"final_answer": state.FinalAnswer,
		}); err != nil {
			return nil, err
		}
		if err := l.saveRunState(ctx, state); err != nil {
			return nil, err
		}
		l.history = cloneMessages(state.Messages)
		result := resultFromState(state)
		action := LoopAction{
			ID:          actionID(state.RunID, state.CurrentStep, LoopActionEmitFinal, 1),
			Kind:        LoopActionEmitFinal,
			RunID:       state.RunID,
			TurnID:      state.TurnID,
			Step:        state.CurrentStep,
			Task:        state.Task,
			FinalAnswer: state.FinalAnswer,
			Result:      &result,
		}
		return advanceFromState(state, []LoopAction{action}, &result, false), nil
	}

	if l.tools == nil {
		state.Status = RunStatusFailed
		state.Summary = "tool registry is required for tool calls"
		if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeRunFailed, map[string]any{
			"run_id":   state.RunID,
			"event_id": eventID,
			"error":    state.Summary,
		}); err != nil {
			return nil, err
		}
		if err := l.saveRunState(ctx, state); err != nil {
			return nil, err
		}
		return advanceFromState(state, nil, nil, false), fmt.Errorf("native loop: %s", state.Summary)
	}

	actions := make([]LoopAction, 0, len(toolCalls))
	pending := make([]PendingToolCall, 0, len(toolCalls))
	for i, call := range toolCalls {
		step.ToolCalls = append(step.ToolCalls, ToolCall{
			ID:    call.ID,
			Name:  call.Name,
			Input: append(json.RawMessage(nil), call.Input...),
		})
		if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeToolCall, toolCallEventPayload{
			ID:    call.ID,
			Name:  call.Name,
			Input: append(json.RawMessage(nil), call.Input...),
		}); err != nil {
			return nil, err
		}
		createdAt := time.Now().UTC()
		pendingCall := PendingToolCall{
			ToolCallID: call.ID,
			ToolName:   call.Name,
			Arguments:  append(json.RawMessage(nil), call.Input...),
			Status:     ToolCallStatusDispatched,
			StepIndex:  state.CurrentStep,
			CreatedAt:  createdAt,
			UpdatedAt:  createdAt,
		}
		pending = append(pending, pendingCall)
		if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeToolDispatched, map[string]any{
			"id":   pendingCall.ToolCallID,
			"name": pendingCall.ToolName,
		}); err != nil {
			return nil, err
		}
		actions = append(actions, dispatchActionFromPending(state, pendingCall, i+1))
	}
	state.Steps = append(state.Steps, step)
	state.PendingTools = pending
	state.Status = RunStatusWaitingForToolResult
	state.UpdatedAt = time.Now().UTC()
	if err := l.saveRunState(ctx, state); err != nil {
		return nil, err
	}
	return advanceFromState(state, actions, nil, true), nil
}

func (l *NativeLoop) handleModelResponseFailed(ctx context.Context, event ModelResponseFailedEvent) (*LoopAdvanceResult, error) {
	state, err := l.loadRunState(ctx, event.RunIDValue)
	if err != nil {
		return nil, err
	}
	eventID, err := idOrNew(event.ID)
	if err != nil {
		return nil, err
	}
	state.Status = RunStatusFailed
	state.Summary = event.Error
	state.LastEventID = eventID
	state.UpdatedAt = time.Now().UTC()
	if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeRunFailed, map[string]any{
		"run_id":   state.RunID,
		"event_id": eventID,
		"error":    event.Error,
	}); err != nil {
		return nil, err
	}
	if err := l.saveRunState(ctx, state); err != nil {
		return nil, err
	}
	return advanceFromState(state, nil, nil, false), fmt.Errorf("llm complete: %s", event.Error)
}

func (l *NativeLoop) handleToolCallCompleted(ctx context.Context, event ToolCallCompletedEvent) (*LoopAdvanceResult, error) {
	state, err := l.loadRunState(ctx, event.RunIDValue)
	if err != nil {
		return nil, err
	}
	eventID, err := idOrNew(event.ID)
	if err != nil {
		return nil, err
	}
	pendingIndex := -1
	for i, pending := range state.PendingTools {
		if pending.ToolCallID == event.ToolCallID {
			pendingIndex = i
			break
		}
	}
	if pendingIndex < 0 {
		return nil, fmt.Errorf("native loop: pending tool call %s not found", event.ToolCallID)
	}

	pending := state.PendingTools[pendingIndex]
	startedAt := event.StartedAt
	if startedAt.IsZero() {
		startedAt = event.Result.StartedAt
	}
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	completedAt := event.CompletedAt
	if completedAt.IsZero() {
		completedAt = event.Result.CompletedAt
	}
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	recorded := event.Result
	if recorded.Name == "" {
		recorded.Name = pending.ToolName
	}
	recorded.StartedAt = startedAt
	recorded.CompletedAt = completedAt
	if recorded.Error == "" {
		recorded.Error = event.Error
	}
	state.PendingTools[pendingIndex].UpdatedAt = completedAt
	if recorded.Error != "" {
		state.PendingTools[pendingIndex].Status = ToolCallStatusFailed
	} else {
		state.PendingTools[pendingIndex].Status = ToolCallStatusCompleted
	}

	if err := l.saveEvent(ctx, state.Task, state.TurnID, pending.StepIndex, session.EventTypeToolResult, toolResultEventPayload{
		ID:          pending.ToolCallID,
		Name:        pending.ToolName,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		Content:     recorded.Content,
		Metadata:    recorded.Metadata,
		Error:       recorded.Error,
	}); err != nil {
		return nil, err
	}

	call := llmClient.ToolCall{ID: pending.ToolCallID, Name: pending.ToolName, Input: append(json.RawMessage(nil), pending.Arguments...)}
	toolMessage := toolResultMessage(call, recorded)
	state.Messages = append(state.Messages, toolMessage)
	if err := l.saveMessage(ctx, state.Task, state.TurnID, pending.StepIndex, toolMessage); err != nil {
		return nil, err
	}
	state.Steps = appendToolResultToStep(state.Steps, pending.StepIndex, recorded)
	state.LastEventID = eventID
	state.UpdatedAt = completedAt

	if hasOpenPendingTool(state.PendingTools) {
		state.Status = RunStatusWaitingForToolResult
		if err := l.saveRunState(ctx, state); err != nil {
			return nil, err
		}
		return advanceFromState(state, nil, nil, true), nil
	}

	state.PendingTools = nil
	if state.CurrentStep >= l.maxSteps {
		state.Status = RunStatusStepLimitReached
		state.Summary = "reached max steps after tool calls"
		if err := l.saveUsageSummary(ctx, state.Task, state.TurnID, state.Usage, state.LLMCalls); err != nil {
			return nil, err
		}
		if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeRunFailed, map[string]any{
			"run_id":   state.RunID,
			"event_id": eventID,
			"error":    state.Summary,
			"status":   state.Status,
		}); err != nil {
			return nil, err
		}
		if err := l.saveRunState(ctx, state); err != nil {
			return nil, err
		}
		return advanceFromState(state, nil, nil, true), nil
	}

	state.CurrentStep++
	action, err := l.planModelCall(ctx, &state)
	if err != nil {
		return nil, err
	}
	if err := l.saveRunState(ctx, state); err != nil {
		return nil, err
	}
	return advanceFromState(state, []LoopAction{action}, nil, false), nil
}
func (l *NativeLoop) executeAction(ctx context.Context, action LoopAction) (LoopEvent, error) {
	switch action.Kind {
	case LoopActionCallModel:
		if action.ModelRequest == nil {
			return nil, fmt.Errorf("native loop: model request action is missing request")
		}
		if l.logger != nil {
			l.logger.Debugf("native loop run %s step %d calling model", action.RunID, action.Step)
		}
		startedAt := time.Now().UTC()
		response, err := l.llm.Complete(ctx, *action.ModelRequest)
		completedAt := time.Now().UTC()
		if err != nil {
			return ModelResponseFailedEvent{
				RunIDValue:  action.RunID,
				Error:       err.Error(),
				StartedAt:   startedAt,
				CompletedAt: completedAt,
			}, nil
		}
		return ModelResponseReceivedEvent{
			RunIDValue:  action.RunID,
			Response:    response,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
		}, nil
	case LoopActionDispatchTool:
		if action.ToolCall == nil {
			return nil, fmt.Errorf("native loop: dispatch tool action is missing tool call")
		}
		if l.tools == nil {
			return nil, fmt.Errorf("native loop: tool registry is required for tool calls")
		}
		startedAt := time.Now().UTC()
		toolCtx := l.toolExecutionContext(ctx, action.Task, action.TurnID, action.Step)
		executor, ok := l.tools.Lookup(action.ToolCall.ToolName)
		if !ok {
			return nil, fmt.Errorf("native loop: unknown tool %q", action.ToolCall.ToolName)
		}
		toolResult, err := executor.Execute(toolCtx, action.ToolCall.Arguments)
		completedAt := time.Now().UTC()
		recorded := ToolResult{
			Name:        action.ToolCall.ToolName,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
		}
		if err != nil {
			recorded.Error = err.Error()
		} else {
			recorded.Content = toolResult.Content
			recorded.Metadata = toolResult.Metadata
		}
		return ToolCallCompletedEvent{
			RunIDValue:  action.RunID,
			ToolCallID:  action.ToolCall.ToolCallID,
			ToolName:    action.ToolCall.ToolName,
			Result:      recorded,
			Error:       recorded.Error,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
		}, nil
	case LoopActionEmitFinal:
		return nil, l.writeOutput(action.FinalAnswer)
	default:
		return nil, fmt.Errorf("native loop: unsupported action kind %s", action.Kind)
	}
}

func (l *NativeLoop) planModelCall(ctx context.Context, state *RunState) (LoopAction, error) {
	state.Status = RunStatusCallingModel
	state.UpdatedAt = time.Now().UTC()
	request := modelRequestFromState(*state)
	if err := l.saveEvent(ctx, state.Task, state.TurnID, state.CurrentStep, session.EventTypeLLMRequest, llmRequestEventPayload{
		Request: request,
	}); err != nil {
		return LoopAction{}, err
	}
	return LoopAction{
		ID:           actionID(state.RunID, state.CurrentStep, LoopActionCallModel, 1),
		Kind:         LoopActionCallModel,
		RunID:        state.RunID,
		TurnID:       state.TurnID,
		Step:         state.CurrentStep,
		Task:         state.Task,
		ModelRequest: &request,
	}, nil
}

func modelRequestFromState(state RunState) llmClient.Request {
	return llmClient.Request{
		Model:       state.Model,
		Messages:    cloneMessages(state.Messages),
		Tools:       cloneToolDefinitions(state.ToolDefinitions),
		Temperature: state.Temperature,
		Metadata: map[string]string{
			"loop":   "native",
			"run_id": state.RunID,
			"step":   fmt.Sprintf("%d", state.CurrentStep),
		},
	}
}

func (l *NativeLoop) saveRunState(ctx context.Context, state RunState) error {
	if state.RunID == "" {
		return fmt.Errorf("save run state: run id is required")
	}
	state.UpdatedAt = time.Now().UTC()
	cloned := cloneRunState(state)
	if l.states == nil {
		l.states = make(map[string]RunState)
	}
	l.states[state.RunID] = cloned
	if l.session == nil {
		return nil
	}
	raw, err := json.Marshal(cloned)
	if err != nil {
		return fmt.Errorf("marshal run state: %w", err)
	}
	if err := l.session.Save(ctx, session.Record{
		AgentName: cloned.Task.AgentName,
		Task:      cloned.Task.Input,
		WorkDir:   cloned.Task.WorkDir,
		TurnID:    cloned.TurnID,
		StepIndex: cloned.CurrentStep,
		Kind:      session.RecordKindRunState,
		Timestamp: time.Now().UTC(),
		RunState:  raw,
	}); err != nil {
		return fmt.Errorf("save run state: %w", err)
	}
	return nil
}

func (l *NativeLoop) loadRunState(ctx context.Context, runID string) (RunState, error) {
	if strings.TrimSpace(runID) == "" {
		return RunState{}, fmt.Errorf("load run state: run id is required")
	}
	if l.states != nil {
		if state, ok := l.states[runID]; ok {
			return cloneRunState(state), nil
		}
	}
	if l.session == nil {
		return RunState{}, fmt.Errorf("load run state: run %s not found", runID)
	}
	loader, ok := l.session.(session.Loader)
	if !ok {
		return RunState{}, fmt.Errorf("load run state: session loader is unavailable")
	}
	records, err := loader.Load(ctx)
	if err != nil {
		return RunState{}, fmt.Errorf("load run state: %w", err)
	}
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Kind != session.RecordKindRunState || len(record.RunState) == 0 {
			continue
		}
		var state RunState
		if err := json.Unmarshal(record.RunState, &state); err != nil {
			return RunState{}, fmt.Errorf("parse run state: %w", err)
		}
		if state.RunID == runID {
			if l.states == nil {
				l.states = make(map[string]RunState)
			}
			l.states[runID] = cloneRunState(state)
			return cloneRunState(state), nil
		}
	}
	return RunState{}, fmt.Errorf("load run state: run %s not found", runID)
}
func idOrNew(id string) (string, error) {
	if strings.TrimSpace(id) != "" {
		return id, nil
	}
	return session.NewID()
}

func actionID(runID string, step int, kind LoopActionKind, sequence int) string {
	return fmt.Sprintf("%s:%03d:%s:%03d", runID, step, kind, sequence)
}

func dispatchActionFromPending(state RunState, pending PendingToolCall, sequence int) LoopAction {
	call := pending
	return LoopAction{
		ID:       actionID(state.RunID, pending.StepIndex, LoopActionDispatchTool, sequence),
		Kind:     LoopActionDispatchTool,
		RunID:    state.RunID,
		TurnID:   state.TurnID,
		Step:     pending.StepIndex,
		Task:     state.Task,
		ToolCall: &call,
	}
}

func dispatchActionsFromPending(state RunState) []LoopAction {
	if len(state.PendingTools) == 0 {
		return nil
	}
	actions := make([]LoopAction, 0, len(state.PendingTools))
	for i, pending := range state.PendingTools {
		if pending.Status == ToolCallStatusCompleted || pending.Status == ToolCallStatusFailed {
			continue
		}
		actions = append(actions, dispatchActionFromPending(state, pending, i+1))
	}
	return actions
}

func hasOpenPendingTool(pending []PendingToolCall) bool {
	for _, call := range pending {
		if call.Status != ToolCallStatusCompleted && call.Status != ToolCallStatusFailed {
			return true
		}
	}
	return false
}

func appendToolResultToStep(steps []Step, stepIndex int, result ToolResult) []Step {
	for i := range steps {
		if steps[i].Index == stepIndex {
			steps[i].ToolResults = append(steps[i].ToolResults, result)
			if result.CompletedAt.After(steps[i].CompletedAt) {
				steps[i].CompletedAt = result.CompletedAt
			}
			return steps
		}
	}
	return append(steps, Step{Index: stepIndex, ToolResults: []ToolResult{result}})
}

func advanceFromState(state RunState, actions []LoopAction, result *Result, suspended bool) *LoopAdvanceResult {
	return &LoopAdvanceResult{
		RunID:     state.RunID,
		Status:    state.Status,
		State:     cloneRunState(state),
		Actions:   cloneLoopActions(actions),
		Result:    cloneResultPtr(result),
		Suspended: suspended,
	}
}

func isSuspendedStatus(status RunStatus) bool {
	switch status {
	case RunStatusWaitingForToolResult,
		RunStatusWaitingForUserApproval,
		RunStatusWaitingForExternalCallback,
		RunStatusWaitingForScheduledResume,
		RunStatusStepLimitReached,
		RunStatusNeedsAlternativeStrategy:
		return true
	default:
		return false
	}
}

func resultFromState(state RunState) Result {
	return Result{Content: state.FinalAnswer, Steps: cloneSteps(state.Steps)}
}

func cloneRunState(state RunState) RunState {
	cloned := state
	cloned.Messages = cloneMessages(state.Messages)
	cloned.ToolDefinitions = cloneToolDefinitions(state.ToolDefinitions)
	cloned.PendingTools = clonePendingToolCalls(state.PendingTools)
	cloned.Steps = cloneSteps(state.Steps)
	return cloned
}

func clonePendingToolCalls(calls []PendingToolCall) []PendingToolCall {
	if len(calls) == 0 {
		return nil
	}
	cloned := make([]PendingToolCall, 0, len(calls))
	for _, call := range calls {
		call.Arguments = append(json.RawMessage(nil), call.Arguments...)
		cloned = append(cloned, call)
	}
	return cloned
}

func cloneSteps(steps []Step) []Step {
	if len(steps) == 0 {
		return nil
	}
	cloned := make([]Step, 0, len(steps))
	for _, step := range steps {
		cloned = append(cloned, cloneStep(step))
	}
	return cloned
}

func cloneStep(step Step) Step {
	cloned := step
	if len(step.ToolCalls) > 0 {
		cloned.ToolCalls = make([]ToolCall, 0, len(step.ToolCalls))
		for _, call := range step.ToolCalls {
			cloned.ToolCalls = append(cloned.ToolCalls, ToolCall{
				ID:    call.ID,
				Name:  call.Name,
				Input: append(json.RawMessage(nil), call.Input...),
			})
		}
	}
	if len(step.ToolResults) > 0 {
		cloned.ToolResults = make([]ToolResult, 0, len(step.ToolResults))
		for _, result := range step.ToolResults {
			cloned.ToolResults = append(cloned.ToolResults, cloneToolResult(result))
		}
	}
	return cloned
}

func cloneToolResult(result ToolResult) ToolResult {
	cloned := result
	if len(result.Metadata) > 0 {
		cloned.Metadata = make(map[string]any, len(result.Metadata))
		for key, value := range result.Metadata {
			cloned.Metadata[key] = value
		}
	}
	return cloned
}

func cloneLoopActions(actions []LoopAction) []LoopAction {
	if len(actions) == 0 {
		return nil
	}
	cloned := make([]LoopAction, 0, len(actions))
	for _, action := range actions {
		cloned = append(cloned, cloneLoopAction(action))
	}
	return cloned
}

func cloneLoopAction(action LoopAction) LoopAction {
	cloned := action
	if action.ModelRequest != nil {
		req := *action.ModelRequest
		req.Messages = cloneMessages(action.ModelRequest.Messages)
		req.Tools = cloneToolDefinitions(action.ModelRequest.Tools)
		if len(action.ModelRequest.Metadata) > 0 {
			req.Metadata = make(map[string]string, len(action.ModelRequest.Metadata))
			for key, value := range action.ModelRequest.Metadata {
				req.Metadata[key] = value
			}
		}
		cloned.ModelRequest = &req
	}
	if action.ToolCall != nil {
		call := *action.ToolCall
		call.Arguments = append(json.RawMessage(nil), action.ToolCall.Arguments...)
		cloned.ToolCall = &call
	}
	cloned.Result = cloneResultPtr(action.Result)
	return cloned
}

func cloneResultPtr(result *Result) *Result {
	if result == nil {
		return nil
	}
	cloned := Result{Content: result.Content, Steps: cloneSteps(result.Steps)}
	return &cloned
}

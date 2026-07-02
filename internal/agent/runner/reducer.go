package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeevent "agent/internal/event"
	"agent/internal/foundation/llmClient"
	"agent/internal/llm"
	"agent/internal/state"
)

type ReActReducer struct{}

func (r ReActReducer) Match(ctx context.Context, runState state.RunState, event runtimeevent.Event) bool {
	_ = ctx
	_ = runState
	switch event.Type {
	case runtimeevent.EventModelRequestCreated,
		runtimeevent.EventModelResponseReceived,
		runtimeevent.EventModelResponseFailed,
		runtimeevent.EventToolCallRequested,
		runtimeevent.EventToolCallDispatched,
		runtimeevent.EventToolCallCompleted,
		runtimeevent.EventToolCallFailed,
		runtimeevent.EventUserApprovalRequired,
		runtimeevent.EventUserApprovalReceived,
		runtimeevent.EventUserInputRequested,
		runtimeevent.EventUserInputReceived:
		return true
	default:
		return false
	}
}

func (r ReActReducer) Reduce(ctx context.Context, runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	_ = ctx
	if runState.IsTerminal() {
		return runState, nil, nil
	}

	switch event.Type {
	case runtimeevent.EventModelRequestCreated:
		return reduceModelRequestCreated(runState, event)
	case runtimeevent.EventModelResponseReceived:
		return reduceModelResponseReceived(runState, event)
	case runtimeevent.EventModelResponseFailed:
		return reduceModelResponseFailed(runState, event)
	case runtimeevent.EventToolCallRequested:
		return reduceToolCallRequested(runState, event)
	case runtimeevent.EventToolCallDispatched:
		return reduceToolCallDispatched(runState, event)
	case runtimeevent.EventToolCallCompleted:
		return reduceToolCallFinished(runState, event, toolCallCompleted)
	case runtimeevent.EventToolCallFailed:
		return reduceToolCallFinished(runState, event, toolCallFailed)
	case runtimeevent.EventUserApprovalRequired:
		return reduceUserApprovalRequired(runState, event)
	case runtimeevent.EventUserApprovalReceived:
		return reduceUserApprovalReceived(runState, event)
	case runtimeevent.EventUserInputRequested:
		return reduceUserInputRequested(runState, event)
	case runtimeevent.EventUserInputReceived:
		return reduceUserInputReceived(runState, event)
	default:
		return runState, nil, nil
	}
}

func reduceModelRequestCreated(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	var payload ModelRequestCreatedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode model request event: %w", err)
	}
	if strings.TrimSpace(payload.Request.Model) == "" {
		return runState, nil, fmt.Errorf("runner: model request missing model")
	}
	if payload.Step <= 0 {
		payload.Step = runState.Step + 1
	}

	runState.Phase = state.PhaseWaiting
	runState.Waiting = &state.WaitingState{Reason: "model_result"}
	effect, err := newEffectWithPayload(runState.RunID, state.EffectExecuteModel, payload)
	if err != nil {
		return runState, nil, err
	}
	return runState, []state.Effect{effect}, nil
}

func reduceModelResponseReceived(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	data, err := loadRunData(runState)
	if err != nil {
		return runState, nil, err
	}

	var payload ModelResponseReceivedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode model response event: %w", err)
	}

	runState.Phase = state.PhaseRunning
	runState.Waiting = nil

	if payload.AssistantMessage != nil {
		data.Messages = append(data.Messages, cloneMessage(*payload.AssistantMessage))
	}
	if payload.Usage != nil {
		data.Usage = data.Usage.Add(*payload.Usage)
	}
	runState.Step++

	if len(payload.ToolCalls) == 0 {
		data.FinalAnswer = finalAnswerFromPayload(payload)
		if err := storeRunData(&runState, data); err != nil {
			return runState, nil, err
		}
		effect, err := newEffectWithPayload(runState.RunID, state.EffectCompleteRun, CompleteRunPayload{
			FinalAnswer: data.FinalAnswer,
			StepsUsed:   runState.Step,
		})
		if err != nil {
			return runState, nil, err
		}
		return runState, []state.Effect{effect}, nil
	}

	now := time.Now().UTC()
	effects := make([]state.Effect, 0, len(payload.ToolCalls))
	for _, call := range payload.ToolCalls {
		call = cloneToolCall(call)
		data.PendingTools = append(data.PendingTools, pendingToolCall{
			ToolCallID: call.ID,
			ToolName:   call.Name,
			Arguments:  append(json.RawMessage(nil), call.Input...),
			Status:     toolCallPending,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
		effect, err := newEffectWithPayload(runState.RunID, state.EffectDispatchTool, DispatchToolPayload{ToolCall: call})
		if err != nil {
			return runState, nil, err
		}
		effects = append(effects, effect)
	}

	runState.Phase = state.PhaseWaiting
	runState.Waiting = &state.WaitingState{Reason: "tool_result"}
	if err := storeRunData(&runState, data); err != nil {
		return runState, nil, err
	}
	return runState, effects, nil
}

func reduceModelResponseFailed(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	runState.Phase = state.PhaseRunning
	runState.Waiting = nil
	failure := state.ErrorState{Code: "model_error", Message: "model call failed"}
	if len(event.Payload) > 0 {
		var payload ModelResponseFailedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return runState, nil, fmt.Errorf("runner: decode model failure event: %w", err)
		}
		if strings.TrimSpace(payload.Code) != "" {
			failure.Code = payload.Code
		}
		if strings.TrimSpace(payload.Message) != "" {
			failure.Message = payload.Message
		}
	}
	effect, err := newEffectWithPayload(runState.RunID, state.EffectFailRun, failure)
	if err != nil {
		return runState, nil, err
	}
	return runState, []state.Effect{effect}, nil
}

func reduceToolCallRequested(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	data, err := loadRunData(runState)
	if err != nil {
		return runState, nil, err
	}

	var payload ToolCallEventPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode tool request event: %w", err)
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return runState, nil, fmt.Errorf("runner: tool request missing tool_call_id")
	}

	index := findPendingTool(data.PendingTools, payload.ToolCallID)
	if index < 0 {
		return runState, nil, fmt.Errorf("runner: pending tool call %q not found", payload.ToolCallID)
	}
	switch data.PendingTools[index].Status {
	case toolCallRequested:
		return runState, nil, nil
	case toolCallPending:
	default:
		return runState, nil, nil
	}

	now := time.Now().UTC()
	data.PendingTools[index].Status = toolCallRequested
	data.PendingTools[index].UpdatedAt = now
	runState.Phase = state.PhaseWaiting
	runState.Waiting = &state.WaitingState{Reason: "tool_dispatch", Target: payload.ToolCallID}
	if err := storeRunData(&runState, data); err != nil {
		return runState, nil, err
	}

	call := llmClient.ToolCall{
		ID:    data.PendingTools[index].ToolCallID,
		Name:  data.PendingTools[index].ToolName,
		Input: append(json.RawMessage(nil), data.PendingTools[index].Arguments...),
	}
	effect, err := newEffectWithPayload(runState.RunID, state.EffectConfirmTool, DispatchToolPayload{ToolCall: call})
	if err != nil {
		return runState, nil, err
	}
	return runState, []state.Effect{effect}, nil
}

func reduceToolCallDispatched(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	data, err := loadRunData(runState)
	if err != nil {
		return runState, nil, err
	}

	var payload ToolCallEventPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode tool dispatch event: %w", err)
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return runState, nil, fmt.Errorf("runner: tool dispatch missing tool_call_id")
	}

	index := findPendingTool(data.PendingTools, payload.ToolCallID)
	if index < 0 {
		return runState, nil, fmt.Errorf("runner: pending tool call %q not found", payload.ToolCallID)
	}
	if data.PendingTools[index].Status == toolCallDispatched {
		return runState, nil, nil
	}
	if data.PendingTools[index].Status != toolCallPending && data.PendingTools[index].Status != toolCallRequested {
		return runState, nil, nil
	}

	now := time.Now().UTC()
	data.PendingTools[index].Status = toolCallDispatched
	data.PendingTools[index].UpdatedAt = now
	runState.Phase = state.PhaseWaiting
	runState.Waiting = &state.WaitingState{Reason: "tool_result"}
	if err := storeRunData(&runState, data); err != nil {
		return runState, nil, err
	}

	call := llmClient.ToolCall{
		ID:    data.PendingTools[index].ToolCallID,
		Name:  data.PendingTools[index].ToolName,
		Input: append(json.RawMessage(nil), data.PendingTools[index].Arguments...),
	}
	effect, err := newEffectWithPayload(runState.RunID, state.EffectExecuteTool, DispatchToolPayload{ToolCall: call})
	if err != nil {
		return runState, nil, err
	}
	return runState, []state.Effect{effect}, nil
}

func reduceUserApprovalRequired(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	data, err := loadRunData(runState)
	if err != nil {
		return runState, nil, err
	}

	var payload UserApprovalRequiredPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode user approval request event: %w", err)
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return runState, nil, fmt.Errorf("runner: user approval request missing tool_call_id")
	}

	index := findPendingTool(data.PendingTools, payload.ToolCallID)
	if index < 0 {
		return runState, nil, fmt.Errorf("runner: pending tool call %q not found", payload.ToolCallID)
	}
	switch data.PendingTools[index].Status {
	case toolCallWaitingApproval:
		return runState, nil, nil
	case toolCallPending, toolCallDispatched:
	default:
		return runState, nil, nil
	}

	now := time.Now().UTC()
	data.PendingTools[index].Status = toolCallWaitingApproval
	data.PendingTools[index].UpdatedAt = now
	runState.Phase = state.PhaseWaiting
	runState.Waiting = &state.WaitingState{Reason: "user_approval", Target: payload.ToolCallID}
	if err := storeRunData(&runState, data); err != nil {
		return runState, nil, err
	}
	return runState, nil, nil
}

func reduceUserApprovalReceived(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	data, err := loadRunData(runState)
	if err != nil {
		return runState, nil, err
	}

	var payload UserApprovalReceivedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode user approval received event: %w", err)
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return runState, nil, fmt.Errorf("runner: user approval received missing tool_call_id")
	}

	index := findPendingTool(data.PendingTools, payload.ToolCallID)
	if index < 0 {
		return runState, nil, fmt.Errorf("runner: pending tool call %q not found", payload.ToolCallID)
	}
	if data.PendingTools[index].Status != toolCallWaitingApproval {
		return runState, nil, nil
	}

	if !payload.Approved {
		reason := strings.TrimSpace(payload.Reason)
		if reason == "" {
			reason = "user declined approval"
		}
		return reduceToolResult(runState, payload.ToolCallID, llm.ToolResult{
			Name:  payload.ToolName,
			Error: reason,
		}, reason, toolCallFailed)
	}

	now := time.Now().UTC()
	data.PendingTools[index].Status = toolCallDispatched
	data.PendingTools[index].UpdatedAt = now
	runState.Phase = state.PhaseWaiting
	runState.Waiting = &state.WaitingState{Reason: "tool_result"}

	call := llmClient.ToolCall{
		ID:    data.PendingTools[index].ToolCallID,
		Name:  data.PendingTools[index].ToolName,
		Input: append(json.RawMessage(nil), data.PendingTools[index].Arguments...),
	}
	if err := storeRunData(&runState, data); err != nil {
		return runState, nil, err
	}
	effect, err := newEffectWithPayload(runState.RunID, state.EffectExecuteTool, DispatchToolPayload{
		ToolCall: call,
		Approved: true,
	})
	if err != nil {
		return runState, nil, err
	}
	return runState, []state.Effect{effect}, nil
}

func reduceUserInputRequested(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	data, err := loadRunData(runState)
	if err != nil {
		return runState, nil, err
	}

	var payload UserInputRequestedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode user input request event: %w", err)
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return runState, nil, fmt.Errorf("runner: user input request missing tool_call_id")
	}

	index := findPendingTool(data.PendingTools, payload.ToolCallID)
	if index < 0 {
		return runState, nil, fmt.Errorf("runner: pending tool call %q not found", payload.ToolCallID)
	}
	switch data.PendingTools[index].Status {
	case toolCallWaitingInput:
		return runState, nil, nil
	case toolCallPending, toolCallDispatched:
	default:
		return runState, nil, nil
	}

	now := time.Now().UTC()
	data.PendingTools[index].Status = toolCallWaitingInput
	data.PendingTools[index].UpdatedAt = now
	runState.Phase = state.PhaseWaiting
	runState.Waiting = &state.WaitingState{Reason: "user_input", Target: payload.ToolCallID}
	if err := storeRunData(&runState, data); err != nil {
		return runState, nil, err
	}
	return runState, nil, nil
}

func reduceUserInputReceived(runState state.RunState, event runtimeevent.Event) (state.RunState, []state.Effect, error) {
	var payload UserInputReceivedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode user input received event: %w", err)
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return runState, nil, fmt.Errorf("runner: user input received missing tool_call_id")
	}
	result := llm.ToolResult{
		Name:    payload.ToolName,
		Content: payload.Answer,
		Metadata: map[string]any{
			"used_default": payload.UsedDefault,
		},
	}
	return reduceToolResult(runState, payload.ToolCallID, result, "", toolCallCompleted)
}

func reduceToolCallFinished(runState state.RunState, event runtimeevent.Event, status toolCallStatus) (state.RunState, []state.Effect, error) {
	var payload ToolCallEventPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return runState, nil, fmt.Errorf("runner: decode tool result event: %w", err)
	}
	if strings.TrimSpace(payload.ToolCallID) == "" {
		return runState, nil, fmt.Errorf("runner: tool result missing tool_call_id")
	}
	return reduceToolResult(runState, payload.ToolCallID, payload.Result, payload.Error, status)
}

func reduceToolResult(runState state.RunState, toolCallID string, result llm.ToolResult, eventError string, status toolCallStatus) (state.RunState, []state.Effect, error) {
	data, err := loadRunData(runState)
	if err != nil {
		return runState, nil, err
	}
	index := findPendingTool(data.PendingTools, toolCallID)
	if index < 0 {
		return runState, nil, fmt.Errorf("runner: pending tool call %q not found", toolCallID)
	}
	if data.PendingTools[index].Status != toolCallPending &&
		data.PendingTools[index].Status != toolCallRequested &&
		data.PendingTools[index].Status != toolCallDispatched &&
		data.PendingTools[index].Status != toolCallWaitingApproval &&
		data.PendingTools[index].Status != toolCallWaitingInput {
		return runState, nil, nil
	}

	now := time.Now().UTC()
	data.PendingTools[index].Status = status
	data.PendingTools[index].Error = eventError
	data.PendingTools[index].UpdatedAt = now

	call := llmClient.ToolCall{
		ID:    data.PendingTools[index].ToolCallID,
		Name:  data.PendingTools[index].ToolName,
		Input: append(json.RawMessage(nil), data.PendingTools[index].Arguments...),
	}
	result.Name = call.Name
	if status == toolCallFailed && result.Error == "" {
		result.Error = eventError
	}
	data.Messages = append(data.Messages, llm.NewToolResultMessage(call, result))

	if hasPendingTool(data.PendingTools) {
		runState.Phase = state.PhaseWaiting
		runState.Waiting = &state.WaitingState{Reason: "tool_result"}
		if err := storeRunData(&runState, data); err != nil {
			return runState, nil, err
		}
		return runState, nil, nil
	}

	runState.Phase = state.PhaseWaiting
	runState.Waiting = &state.WaitingState{Reason: "model_result"}
	if err := storeRunData(&runState, data); err != nil {
		return runState, nil, err
	}
	return runState, []state.Effect{state.NewEffect(runState.RunID, state.EffectCallModel)}, nil
}

func finalAnswerFromPayload(payload ModelResponseReceivedPayload) string {
	if payload.AssistantMessage != nil && strings.TrimSpace(payload.AssistantMessage.Content) != "" {
		return payload.AssistantMessage.Content
	}
	return payload.Response.Content
}

func findPendingTool(calls []pendingToolCall, toolCallID string) int {
	fallback := -1
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].ToolCallID != toolCallID {
			continue
		}
		if calls[i].Status == toolCallPending || calls[i].Status == toolCallRequested || calls[i].Status == toolCallDispatched || calls[i].Status == toolCallWaitingApproval || calls[i].Status == toolCallWaitingInput {
			return i
		}
		if fallback < 0 {
			fallback = i
		}
	}
	return fallback
}

func hasPendingTool(calls []pendingToolCall) bool {
	for _, call := range calls {
		if call.Status == toolCallPending || call.Status == toolCallRequested || call.Status == toolCallDispatched || call.Status == toolCallWaitingApproval || call.Status == toolCallWaitingInput {
			return true
		}
	}
	return false
}

func newEffectWithPayload(runID string, effectType state.EffectType, payload any) (state.Effect, error) {
	effect := state.NewEffect(runID, effectType)
	raw, err := json.Marshal(payload)
	if err != nil {
		return state.Effect{}, fmt.Errorf("runner: encode %s effect payload: %w", effectType, err)
	}
	effect.Payload = raw
	return effect, nil
}

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"agent/internal/capability/tool"
	runtimeevent "agent/internal/event"
	"agent/internal/foundation/llmClient"
	"agent/internal/llm"
	"agent/internal/state"
)

type ToolRegistry interface {
	Execute(ctx context.Context, name string, input json.RawMessage) (tool.Result, error)
}

type ApprovedToolRegistry interface {
	ExecuteApproved(ctx context.Context, name string, input json.RawMessage) (tool.Result, error)
}

type EffectDispatcher interface {
	Dispatch(ctx context.Context, effect state.Effect) error
}

type EffectDispatcherFunc func(ctx context.Context, effect state.Effect) error

func (f EffectDispatcherFunc) Dispatch(ctx context.Context, effect state.Effect) error {
	return f(ctx, effect)
}

type StoreEffectDispatcher struct {
	effects state.EffectStore
}

func NewStoreEffectDispatcher(effects state.EffectStore) (*StoreEffectDispatcher, error) {
	if effects == nil {
		return nil, fmt.Errorf("runner effect dispatcher: effect store is required")
	}
	return &StoreEffectDispatcher{effects: effects}, nil
}

func (d *StoreEffectDispatcher) Dispatch(ctx context.Context, effect state.Effect) error {
	if d == nil {
		return fmt.Errorf("runner effect dispatcher is nil")
	}
	return d.effects.MarkDispatched(ctx, effect.ID)
}

type EffectWorker interface {
	Execute(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error)
}

type EffectWorkerOptions struct {
	StateStore             state.StateStore
	LLM                    *llm.Runtime
	Tools                  ToolRegistry
	SuspendUserInteraction bool
}

type RuntimeEffectWorker struct {
	states                 state.StateStore
	llm                    *llm.Runtime
	tools                  ToolRegistry
	suspendUserInteraction bool
}

func NewRuntimeEffectWorker(opts EffectWorkerOptions) (*RuntimeEffectWorker, error) {
	if opts.StateStore == nil {
		return nil, fmt.Errorf("runner effect worker: state store is required")
	}
	if opts.LLM == nil {
		return nil, fmt.Errorf("runner effect worker: llm runtime is required")
	}
	return &RuntimeEffectWorker{
		states:                 opts.StateStore,
		llm:                    opts.LLM,
		tools:                  opts.Tools,
		suspendUserInteraction: opts.SuspendUserInteraction,
	}, nil
}

func (w *RuntimeEffectWorker) Execute(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error) {
	if w == nil {
		return nil, fmt.Errorf("runner effect worker is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	switch effect.Type {
	case state.EffectNoop, state.EffectFinalize:
		return nil, nil
	case state.EffectCallModel:
		return w.executeCallModel(ctx, effect)
	case state.EffectExecuteModel:
		return w.executeExecuteModel(ctx, effect)
	case state.EffectDispatchTool:
		return w.executeDispatchTool(ctx, effect)
	case state.EffectConfirmTool:
		return w.executeConfirmTool(ctx, effect)
	case state.EffectExecuteTool:
		return w.executeExecuteTool(ctx, effect)
	case state.EffectCompleteRun:
		return w.executeCompleteRun(effect)
	case state.EffectFailRun:
		return w.executeFailRun(effect)
	default:
		return nil, fmt.Errorf("runner effect worker: unsupported effect type %q", effect.Type)
	}
}

func (w *RuntimeEffectWorker) executeCallModel(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error) {
	runState, err := w.states.Load(ctx, effect.RunID)
	if err != nil {
		return nil, err
	}
	if runState.MaxSteps > 0 && runState.Step >= runState.MaxSteps {
		stepEvent, err := newRuntimeEvent(runtimeevent.EventStepLimitReached, effect.RunID, nil, effect.ID)
		if err != nil {
			return nil, err
		}
		return []runtimeevent.Event{stepEvent}, nil
	}
	data, err := loadRunData(runState)
	if err != nil {
		return nil, err
	}

	input := llm.ModelCallInput{
		RunID:    effect.RunID,
		Step:     runState.Step + 1,
		Agent:    data.Agent,
		Messages: cloneMessages(data.Messages),
	}
	request, err := w.llm.BuildRequest(ctx, input)
	if err != nil {
		return w.modelFailedEvent(effect, err)
	}

	requestEvent, err := newRuntimeEvent(runtimeevent.EventModelRequestCreated, effect.RunID, ModelRequestCreatedPayload{
		Step:    input.Step,
		Request: request,
	}, effect.ID)
	if err != nil {
		return nil, err
	}

	return []runtimeevent.Event{requestEvent}, nil
}

func (w *RuntimeEffectWorker) executeExecuteModel(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error) {
	runState, err := w.states.Load(ctx, effect.RunID)
	if err != nil {
		return nil, err
	}
	data, err := loadRunData(runState)
	if err != nil {
		return nil, err
	}

	var payload ModelRequestCreatedPayload
	if err := json.Unmarshal(effect.Payload, &payload); err != nil {
		return nil, fmt.Errorf("runner: decode execute model effect: %w", err)
	}
	if payload.Step <= 0 {
		payload.Step = runState.Step + 1
	}
	input := llm.ModelCallInput{
		RunID:    effect.RunID,
		Step:     payload.Step,
		Agent:    data.Agent,
		Messages: cloneMessages(data.Messages),
	}

	result, err := w.llm.CompleteRequest(ctx, input, payload.Request)
	if err != nil {
		failedEvents, failedErr := w.modelFailedEvent(effect, err)
		if failedErr != nil {
			return nil, failedErr
		}
		return failedEvents, nil
	}

	responseEvent, err := newRuntimeEvent(runtimeevent.EventModelResponseReceived, effect.RunID, ModelResponseReceivedPayload{
		Response:         result.Response,
		AssistantMessage: result.AssistantMessage,
		ToolCalls:        result.ToolCalls,
		Usage:            result.Usage,
		StartedAt:        result.StartedAt,
		CompletedAt:      result.CompletedAt,
	}, effect.ID)
	if err != nil {
		return nil, err
	}
	return []runtimeevent.Event{responseEvent}, nil
}

func (w *RuntimeEffectWorker) executeDispatchTool(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error) {
	var payload DispatchToolPayload
	if err := json.Unmarshal(effect.Payload, &payload); err != nil {
		return nil, fmt.Errorf("runner: decode dispatch tool effect: %w", err)
	}
	call := cloneToolCall(payload.ToolCall)
	if strings.TrimSpace(call.ID) == "" {
		return nil, fmt.Errorf("runner: tool call id is required")
	}
	if strings.TrimSpace(call.Name) == "" {
		return nil, fmt.Errorf("runner: tool name is required")
	}

	requested, err := newRuntimeEvent(runtimeevent.EventToolCallRequested, effect.RunID, ToolCallEventPayload{
		ToolCallID: call.ID,
		ToolName:   call.Name,
		Arguments:  append(json.RawMessage(nil), call.Input...),
	}, effect.ID)
	if err != nil {
		return nil, err
	}

	return []runtimeevent.Event{requested}, nil
}

func (w *RuntimeEffectWorker) executeConfirmTool(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error) {
	var payload DispatchToolPayload
	if err := json.Unmarshal(effect.Payload, &payload); err != nil {
		return nil, fmt.Errorf("runner: decode confirm tool effect: %w", err)
	}
	call := cloneToolCall(payload.ToolCall)
	if strings.TrimSpace(call.ID) == "" {
		return nil, fmt.Errorf("runner: tool call id is required")
	}
	if strings.TrimSpace(call.Name) == "" {
		return nil, fmt.Errorf("runner: tool name is required")
	}

	dispatched, err := newRuntimeEvent(runtimeevent.EventToolCallDispatched, effect.RunID, ToolCallEventPayload{
		ToolCallID: call.ID,
		ToolName:   call.Name,
		Arguments:  append(json.RawMessage(nil), call.Input...),
	}, effect.ID)
	if err != nil {
		return nil, err
	}
	return []runtimeevent.Event{dispatched}, nil
}

func (w *RuntimeEffectWorker) executeExecuteTool(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error) {
	var payload DispatchToolPayload
	if err := json.Unmarshal(effect.Payload, &payload); err != nil {
		return nil, fmt.Errorf("runner: decode execute tool effect: %w", err)
	}
	call := cloneToolCall(payload.ToolCall)
	if strings.TrimSpace(call.ID) == "" {
		return nil, fmt.Errorf("runner: tool call id is required")
	}
	if strings.TrimSpace(call.Name) == "" {
		return nil, fmt.Errorf("runner: tool name is required")
	}

	if w.suspendUserInteraction && call.Name == "ask_user" {
		requested, err := w.userInputRequestedEvent(effect, call)
		if err != nil {
			return nil, err
		}
		return []runtimeevent.Event{requested}, nil
	}

	if w.tools == nil {
		failed, err := w.toolFailedEvent(effect, call.ID, call.Name, call.Input, fmt.Errorf("tool registry is required"))
		if err != nil {
			return nil, err
		}
		return []runtimeevent.Event{failed}, nil
	}

	result, err := w.executeToolCall(ctx, payload, call)
	if err != nil {
		var approvalRequired *tool.ApprovalRequiredError
		if errors.As(err, &approvalRequired) {
			required, requiredErr := w.userApprovalRequiredEvent(effect, call, approvalRequired)
			if requiredErr != nil {
				return nil, requiredErr
			}
			return []runtimeevent.Event{required}, nil
		}
		failed, failedErr := w.toolFailedEvent(effect, call.ID, call.Name, call.Input, err)
		if failedErr != nil {
			return nil, failedErr
		}
		return []runtimeevent.Event{failed}, nil
	}

	completed, err := newRuntimeEvent(runtimeevent.EventToolCallCompleted, effect.RunID, ToolCallEventPayload{
		ToolCallID: call.ID,
		ToolName:   call.Name,
		Arguments:  append(json.RawMessage(nil), call.Input...),
		Result: llm.ToolResult{
			Name:     call.Name,
			Content:  result.Content,
			Metadata: result.Metadata,
		},
	}, effect.ID)
	if err != nil {
		return nil, err
	}
	return []runtimeevent.Event{completed}, nil
}

func (w *RuntimeEffectWorker) executeToolCall(ctx context.Context, payload DispatchToolPayload, call llmClient.ToolCall) (tool.Result, error) {
	if payload.Approved {
		if approved, ok := w.tools.(ApprovedToolRegistry); ok {
			return approved.ExecuteApproved(ctx, call.Name, call.Input)
		}
	}
	return w.tools.Execute(ctx, call.Name, call.Input)
}

func (w *RuntimeEffectWorker) userInputRequestedEvent(effect state.Effect, call llmClient.ToolCall) (runtimeevent.Event, error) {
	var request struct {
		Question string `json:"question"`
		Prompt   string `json:"prompt"`
		Default  string `json:"default"`
	}
	if err := json.Unmarshal(call.Input, &request); err != nil {
		return runtimeevent.Event{}, fmt.Errorf("runner: decode ask_user input: %w", err)
	}
	question := strings.TrimSpace(request.Question)
	if question == "" {
		question = strings.TrimSpace(request.Prompt)
	}
	if question == "" {
		return runtimeevent.Event{}, fmt.Errorf("runner: ask_user question is required")
	}
	return newRuntimeEvent(runtimeevent.EventUserInputRequested, effect.RunID, UserInputRequestedPayload{
		ToolCallID: call.ID,
		ToolName:   call.Name,
		Arguments:  append(json.RawMessage(nil), call.Input...),
		Question:   question,
		Default:    strings.TrimSpace(request.Default),
	}, effect.ID)
}

func (w *RuntimeEffectWorker) userApprovalRequiredEvent(effect state.Effect, call llmClient.ToolCall, approval *tool.ApprovalRequiredError) (runtimeevent.Event, error) {
	if approval == nil {
		return runtimeevent.Event{}, fmt.Errorf("runner: approval error is nil")
	}
	return newRuntimeEvent(runtimeevent.EventUserApprovalRequired, effect.RunID, UserApprovalRequiredPayload{
		ToolCallID: call.ID,
		ToolName:   call.Name,
		Arguments:  append(json.RawMessage(nil), call.Input...),
		Request:    approval.Request,
		Decision:   approval.Result,
	}, effect.ID)
}

func (w *RuntimeEffectWorker) executeCompleteRun(effect state.Effect) ([]runtimeevent.Event, error) {
	var payload CompleteRunPayload
	if len(effect.Payload) > 0 {
		if err := json.Unmarshal(effect.Payload, &payload); err != nil {
			return nil, fmt.Errorf("runner: decode complete run effect: %w", err)
		}
	}
	event, err := newRuntimeEvent(runtimeevent.EventRunCompleted, effect.RunID, payload, effect.ID)
	if err != nil {
		return nil, err
	}
	return []runtimeevent.Event{event}, nil
}

func (w *RuntimeEffectWorker) executeFailRun(effect state.Effect) ([]runtimeevent.Event, error) {
	failure := state.ErrorState{Code: "run_failed", Message: "run failed"}
	if len(effect.Payload) > 0 {
		if err := json.Unmarshal(effect.Payload, &failure); err != nil {
			return nil, fmt.Errorf("runner: decode fail run effect: %w", err)
		}
	}
	event, err := newRuntimeEvent(runtimeevent.EventRunFailed, effect.RunID, failure, effect.ID)
	if err != nil {
		return nil, err
	}
	return []runtimeevent.Event{event}, nil
}

func (w *RuntimeEffectWorker) modelFailedEvent(effect state.Effect, callErr error) ([]runtimeevent.Event, error) {
	event, err := newRuntimeEvent(runtimeevent.EventModelResponseFailed, effect.RunID, ModelResponseFailedPayload{
		Code:    "model_error",
		Message: callErr.Error(),
	}, effect.ID)
	if err != nil {
		return nil, err
	}
	return []runtimeevent.Event{event}, nil
}

func (w *RuntimeEffectWorker) toolFailedEvent(effect state.Effect, toolCallID string, toolName string, args json.RawMessage, callErr error) (runtimeevent.Event, error) {
	return newRuntimeEvent(runtimeevent.EventToolCallFailed, effect.RunID, ToolCallEventPayload{
		ToolCallID: toolCallID,
		ToolName:   toolName,
		Arguments:  append(json.RawMessage(nil), args...),
		Result: llm.ToolResult{
			Name:  toolName,
			Error: callErr.Error(),
		},
		Error: callErr.Error(),
	}, effect.ID)
}

func newRuntimeEvent(eventType runtimeevent.Type, runID string, payload any, causationID string) (runtimeevent.Event, error) {
	return runtimeevent.New(eventType, payload,
		runtimeevent.WithRunID(runID),
		runtimeevent.WithSource("agent.runner"),
		runtimeevent.WithCausationID(causationID),
	)
}

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"agent/internal/capability/tool"
	runtimeevent "agent/internal/event"
	"agent/internal/foundation/llmClient"
	"agent/internal/foundation/policy"
	"agent/internal/llm"
	"agent/internal/state"
)

func TestAgentRunnerStartEnqueuesRunStartedWithoutAdvancing(t *testing.T) {
	model := &scriptedLLM{responses: []llmClient.Response{{Provider: "mock", Model: "mock-native", Content: "done"}}}
	tools := &recordingTools{result: tool.Result{Content: "lookup result"}}
	runner := mustRunner(t, mustRuntime(t, model), tools)
	ctx := context.Background()

	runID, err := runner.Start(ctx, Task{
		Input: "build",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if len(model.requests) != 0 {
		t.Fatalf("model requests = %d, want 0 after Start", len(model.requests))
	}
	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %d, want 0 after Start", len(tools.calls))
	}

	result, err := runner.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Status != state.PhaseIdle {
		t.Fatalf("state = %#v, want idle before event processing", result.State)
	}
	if len(result.Events) != 0 {
		t.Fatalf("processed events = %#v, want none before event processing", result.Events)
	}
	assertNoPendingEffects(t, runner, runID)
	pendingEvents := listPendingEvents(t, runner, runID)
	if len(pendingEvents) != 1 || pendingEvents[0].Event.Type != runtimeevent.EventRunStarted {
		t.Fatalf("pending events = %#v, want RunStarted", pendingEvents)
	}

	processNextEvent(t, runner, runID)
	result, err = runner.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result after processing returned error: %v", err)
	}
	if result.Status != state.PhaseWaiting || result.State.Waiting == nil || result.State.Waiting.Reason != "model_result" {
		t.Fatalf("state = %#v, want waiting model_result", result.State)
	}
	effect := findPendingEffect(t, runner, runID, state.EffectCallModel)
	if effect.Status != state.EffectStatusDispatched {
		t.Fatalf("effect status = %q, want dispatched", effect.Status)
	}
}

func TestAgentRunnerHandleModelResponseOnlyEnqueuesUntilProcessed(t *testing.T) {
	model := &scriptedLLM{}
	tools := &recordingTools{result: tool.Result{Content: "lookup result"}}
	runner := mustRunner(t, mustRuntime(t, model), tools)
	ctx := context.Background()

	runID := startAndProcessRunStarted(t, runner, Task{
		Input: "lookup x",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	modelEffect := findPendingEffect(t, runner, runID, state.EffectCallModel)
	completeEffect(t, runner, runID, modelEffect.Effect.ID)

	response := llmClient.Response{
		Provider: "mock",
		Model:    "mock-native",
		Content:  "need lookup",
		ToolCalls: []llmClient.ToolCall{{
			ID:    "call_lookup_1",
			Name:  "lookup",
			Input: json.RawMessage(`{"query":"x"}`),
		}},
	}
	if err := runner.HandleEvent(ctx, modelResponseEvent(t, string(runID), modelEffect.Effect.ID, response)); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if len(model.requests) != 0 {
		t.Fatalf("model requests = %d, want 0", len(model.requests))
	}
	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %d, want 0", len(tools.calls))
	}

	result, err := runner.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Status != state.PhaseWaiting || result.State.Waiting == nil || result.State.Waiting.Reason != "model_result" {
		t.Fatalf("state = %#v, want still waiting model_result before processing event", result.State)
	}
	if len(pendingEffectsOfType(t, runner, runID, state.EffectDispatchTool)) != 0 {
		t.Fatal("tool dispatch effect exists before processing model response event")
	}
	pendingEvents := listPendingEvents(t, runner, runID)
	if len(pendingEvents) != 1 || pendingEvents[0].Event.Type != runtimeevent.EventModelResponseReceived {
		t.Fatalf("pending events = %#v, want ModelResponseReceived", pendingEvents)
	}

	processNextEvent(t, runner, runID)
	result, err = runner.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result after processing returned error: %v", err)
	}
	if result.Status != state.PhaseWaiting || result.State.Waiting == nil || result.State.Waiting.Reason != "tool_result" {
		t.Fatalf("state = %#v, want waiting tool_result", result.State)
	}
	toolEffect := findPendingEffect(t, runner, runID, state.EffectDispatchTool)
	if toolEffect.Status != state.EffectStatusDispatched {
		t.Fatalf("tool effect status = %q, want dispatched", toolEffect.Status)
	}
}

func TestAgentRunnerHandleToolResultOnlyAdvancesWhenProcessed(t *testing.T) {
	model := &scriptedLLM{}
	tools := &recordingTools{result: tool.Result{Content: "lookup result"}}
	runner := mustRunner(t, mustRuntime(t, model), tools)
	ctx := context.Background()

	runID := startAndProcessRunStarted(t, runner, Task{
		Input: "lookup x",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	modelEffect := findPendingEffect(t, runner, runID, state.EffectCallModel)
	completeEffect(t, runner, runID, modelEffect.Effect.ID)

	response := llmClient.Response{
		Provider: "mock",
		Model:    "mock-native",
		Content:  "need lookup",
		ToolCalls: []llmClient.ToolCall{{
			ID:    "call_lookup_1",
			Name:  "lookup",
			Input: json.RawMessage(`{"query":"x"}`),
		}},
	}
	if err := runner.HandleEvent(ctx, modelResponseEvent(t, string(runID), modelEffect.Effect.ID, response)); err != nil {
		t.Fatalf("HandleEvent model response returned error: %v", err)
	}
	processNextEvent(t, runner, runID)
	toolEffect := findPendingEffect(t, runner, runID, state.EffectDispatchTool)
	completeEffect(t, runner, runID, toolEffect.Effect.ID)

	toolDone, err := newRuntimeEvent(runtimeevent.EventToolCallCompleted, string(runID), ToolCallEventPayload{
		ToolCallID: "call_lookup_1",
		ToolName:   "lookup",
		Arguments:  json.RawMessage(`{"query":"x"}`),
		Result:     llm.ToolResult{Name: "lookup", Content: "lookup result"},
	}, toolEffect.Effect.ID)
	if err != nil {
		t.Fatalf("newRuntimeEvent returned error: %v", err)
	}
	if err := runner.HandleEvent(ctx, toolDone); err != nil {
		t.Fatalf("HandleEvent tool result returned error: %v", err)
	}
	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %d, want 0", len(tools.calls))
	}

	result, err := runner.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Status != state.PhaseWaiting || result.State.Waiting == nil || result.State.Waiting.Reason != "tool_result" {
		t.Fatalf("state = %#v, want still waiting tool_result before processing event", result.State)
	}

	processNextEvent(t, runner, runID)
	result, err = runner.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result after processing returned error: %v", err)
	}
	if result.Status != state.PhaseWaiting || result.State.Waiting == nil || result.State.Waiting.Reason != "model_result" {
		t.Fatalf("state = %#v, want waiting model_result", result.State)
	}
	findPendingEffect(t, runner, runID, state.EffectCallModel)

	runState, err := runner.states.Load(ctx, string(runID))
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	data, err := loadRunData(runState)
	if err != nil {
		t.Fatalf("loadRunData returned error: %v", err)
	}
	if !hasToolResultMessage(data.Messages, "call_lookup_1", "lookup result") {
		t.Fatalf("messages missing tool result: %#v", data.Messages)
	}
}

func TestAgentRunnerDispatchNextEffectCanResumeFromEnqueuedEvents(t *testing.T) {
	ctx := context.Background()
	states := state.NewMemoryStateStore()
	events := state.NewMemoryEventStore()
	effects := state.NewMemoryEffectStore()
	inbox := state.NewMemoryEventInbox()
	model := &scriptedLLM{responses: []llmClient.Response{{Provider: "mock", Model: "mock-native", Content: "done"}}}

	first := mustRunnerWithStores(t, mustRuntime(t, model), &recordingTools{}, states, events, effects, inbox)
	runID, err := first.Start(ctx, Task{
		Input: "build",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	advanced, err := first.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance returned error: %v", err)
	}
	if advanced.Status != AdvanceStatusEventProcessed || advanced.Event == nil || advanced.Event.Type != runtimeevent.EventRunStarted {
		t.Fatalf("advance = %#v, want processed RunStarted", advanced)
	}

	dispatched, err := first.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("DispatchNextEffect returned error: %v", err)
	}
	if dispatched.Status != AdvanceStatusEffectDispatched || dispatched.Effect == nil || dispatched.Effect.Type != state.EffectCallModel {
		t.Fatalf("dispatch = %#v, want dispatched model effect", dispatched)
	}
	if len(model.requests) != 0 {
		t.Fatalf("model requests = %d, want 0 before ModelRequestCreated is processed", len(model.requests))
	}
	if len(dispatched.Events) != 1 || dispatched.Events[0].Type != runtimeevent.EventModelRequestCreated {
		t.Fatalf("dispatched events = %#v, want model request event", dispatched.Events)
	}

	result, err := first.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Status != state.PhaseWaiting || result.State.Waiting == nil || result.State.Waiting.Reason != "model_result" {
		t.Fatalf("result state = %#v, want still waiting for model_result before processing queued events", result.State)
	}
	assertEventTypes(t, result.Events, runtimeevent.EventRunStarted)

	second := mustRunnerWithStores(t, mustRuntime(t, model), &recordingTools{}, states, events, effects, inbox)
	advanced, err = second.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("second Advance request event returned error: %v", err)
	}
	if advanced.Status != AdvanceStatusEventProcessed || advanced.Event == nil || advanced.Event.Type != runtimeevent.EventModelRequestCreated {
		t.Fatalf("second advance = %#v, want processed ModelRequestCreated", advanced)
	}

	modelExecuteEffect := findPendingEffect(t, second, runID, state.EffectExecuteModel)
	dispatched, err = second.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("second DispatchNextEffect model execute returned error: %v", err)
	}
	if dispatched.Status != AdvanceStatusEffectDispatched || dispatched.Effect == nil || dispatched.Effect.ID != modelExecuteEffect.Effect.ID {
		t.Fatalf("model execute dispatch = %#v, want model execute effect", dispatched)
	}
	if len(model.requests) != 1 {
		t.Fatalf("model requests = %d, want 1 after model execute effect", len(model.requests))
	}
	if len(dispatched.Events) != 1 || dispatched.Events[0].Type != runtimeevent.EventModelResponseReceived {
		t.Fatalf("model execute events = %#v, want ModelResponseReceived", dispatched.Events)
	}

	advanced, err = second.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("second Advance response event returned error: %v", err)
	}
	if advanced.Status != AdvanceStatusEventProcessed || advanced.Event == nil || advanced.Event.Type != runtimeevent.EventModelResponseReceived {
		t.Fatalf("second advance = %#v, want processed ModelResponseReceived", advanced)
	}

	dispatched, err = second.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("second DispatchNextEffect complete returned error: %v", err)
	}
	if dispatched.Status != AdvanceStatusEffectDispatched || dispatched.Effect == nil || dispatched.Effect.Type != state.EffectCompleteRun {
		t.Fatalf("complete dispatch = %#v, want complete run effect", dispatched)
	}
	advanced, err = second.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("second Advance RunCompleted returned error: %v", err)
	}
	if advanced.Status != AdvanceStatusEventProcessed || advanced.Event == nil || advanced.Event.Type != runtimeevent.EventRunCompleted {
		t.Fatalf("completion advance = %#v, want processed RunCompleted", advanced)
	}

	result, err = second.Result(ctx, runID)
	if err != nil {
		t.Fatalf("final Result returned error: %v", err)
	}
	if result.Status != state.PhaseCompleted || result.FinalAnswer != "done" {
		t.Fatalf("final result = %#v, want completed done", result)
	}
}

func TestAgentRunnerToolDispatchSuspendsBeforeToolExecution(t *testing.T) {
	ctx := context.Background()
	model := &scriptedLLM{}
	tools := &recordingTools{result: tool.Result{Content: "lookup result"}}
	runner := mustRunner(t, mustRuntime(t, model), tools)
	runID := startAndProcessRunStarted(t, runner, Task{
		Input: "lookup x",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	modelEffect := findPendingEffect(t, runner, runID, state.EffectCallModel)
	completeEffect(t, runner, runID, modelEffect.Effect.ID)

	response := llmClient.Response{
		Provider: "mock",
		Model:    "mock-native",
		Content:  "need lookup",
		ToolCalls: []llmClient.ToolCall{{
			ID:    "call_lookup_1",
			Name:  "lookup",
			Input: json.RawMessage(`{"query":"x"}`),
		}},
	}
	if err := runner.HandleEvent(ctx, modelResponseEvent(t, string(runID), modelEffect.Effect.ID, response)); err != nil {
		t.Fatalf("HandleEvent model response returned error: %v", err)
	}
	processNextEvent(t, runner, runID)

	toolDispatchEffect := findPendingEffect(t, runner, runID, state.EffectDispatchTool)
	dispatched, err := runner.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("DispatchNextEffect tool dispatch returned error: %v", err)
	}
	if dispatched.Status != AdvanceStatusEffectDispatched || dispatched.Effect == nil || dispatched.Effect.ID != toolDispatchEffect.Effect.ID {
		t.Fatalf("dispatch result = %#v, want tool dispatch effect", dispatched)
	}
	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %d, want 0 before ToolCallDispatched is processed", len(tools.calls))
	}
	if len(dispatched.Events) != 2 ||
		dispatched.Events[0].Type != runtimeevent.EventToolCallRequested ||
		dispatched.Events[1].Type != runtimeevent.EventToolCallDispatched {
		t.Fatalf("tool dispatch events = %#v, want requested and dispatched", dispatched.Events)
	}

	advanced, err := runner.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance ToolCallRequested returned error: %v", err)
	}
	if advanced.Status != AdvanceStatusEventProcessed || advanced.Event == nil || advanced.Event.Type != runtimeevent.EventToolCallRequested {
		t.Fatalf("advance = %#v, want ToolCallRequested", advanced)
	}
	if len(pendingEffectsOfType(t, runner, runID, state.EffectExecuteTool)) != 0 {
		t.Fatal("execute tool effect exists before ToolCallDispatched is processed")
	}

	advanced, err = runner.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance ToolCallDispatched returned error: %v", err)
	}
	if advanced.Status != AdvanceStatusEventProcessed || advanced.Event == nil || advanced.Event.Type != runtimeevent.EventToolCallDispatched {
		t.Fatalf("advance = %#v, want ToolCallDispatched", advanced)
	}
	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %d, want 0 before execute tool effect is dispatched", len(tools.calls))
	}
	toolExecuteEffect := findPendingEffect(t, runner, runID, state.EffectExecuteTool)

	executed, err := runner.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("DispatchNextEffect tool execute returned error: %v", err)
	}
	if executed.Status != AdvanceStatusEffectDispatched || executed.Effect == nil || executed.Effect.ID != toolExecuteEffect.Effect.ID {
		t.Fatalf("execute result = %#v, want execute tool effect", executed)
	}
	if len(tools.calls) != 1 || tools.calls[0].name != "lookup" {
		t.Fatalf("tool calls = %#v, want one lookup call", tools.calls)
	}
	if len(executed.Events) != 1 || executed.Events[0].Type != runtimeevent.EventToolCallCompleted {
		t.Fatalf("tool execute events = %#v, want ToolCallCompleted", executed.Events)
	}
}

func TestAgentRunnerAskUserSuspendsUntilUserInputReceived(t *testing.T) {
	ctx := context.Background()
	model := &scriptedLLM{responses: []llmClient.Response{
		{
			Provider: "mock",
			Model:    "mock-native",
			ToolCalls: []llmClient.ToolCall{{
				ID:    "call_ask_1",
				Name:  "ask_user",
				Input: json.RawMessage(`{"question":"Which target?","default":"backend"}`),
			}},
		},
		{Provider: "mock", Model: "mock-native", Content: "target recorded"},
	}}
	tools := &recordingTools{result: tool.Result{Content: "should not be called"}}
	runner := mustRunnerWithOptions(t, Options{
		LLM:                    mustRuntime(t, model),
		ToolRegistry:           tools,
		MaxSteps:               5,
		SuspendUserInteraction: true,
	})

	runID, err := runner.Start(ctx, Task{
		Input: "ask for target",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	processNextEvent(t, runner, runID)
	if _, err := runner.DispatchNextEffect(ctx, runID); err != nil {
		t.Fatalf("Dispatch model.call returned error: %v", err)
	}
	processNextEvent(t, runner, runID)
	if _, err := runner.DispatchNextEffect(ctx, runID); err != nil {
		t.Fatalf("Dispatch model.execute returned error: %v", err)
	}
	processNextEvent(t, runner, runID)
	if _, err := runner.DispatchNextEffect(ctx, runID); err != nil {
		t.Fatalf("Dispatch tool.dispatch returned error: %v", err)
	}
	processNextEvent(t, runner, runID)
	processNextEvent(t, runner, runID)
	if _, err := runner.DispatchNextEffect(ctx, runID); err != nil {
		t.Fatalf("Dispatch tool.execute returned error: %v", err)
	}
	if len(tools.calls) != 0 {
		t.Fatalf("tools calls = %#v, want ask_user to suspend before registry execution", tools.calls)
	}

	advanced, err := runner.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance UserInputRequested returned error: %v", err)
	}
	if advanced.Event == nil || advanced.Event.Type != runtimeevent.EventUserInputRequested {
		t.Fatalf("advance = %#v, want UserInputRequested", advanced)
	}
	result, err := runner.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Status != state.PhaseWaiting || result.State.Waiting == nil || result.State.Waiting.Reason != "user_input" || result.State.Waiting.Target != "call_ask_1" {
		t.Fatalf("state = %#v, want waiting user_input for call_ask_1", result.State)
	}
	if pending := pendingEffectsOfType(t, runner, runID, state.EffectCallModel); len(pending) != 0 {
		t.Fatalf("model effects = %#v, want none before user input arrives", pending)
	}

	inputReceived, err := newRuntimeEvent(runtimeevent.EventUserInputReceived, string(runID), UserInputReceivedPayload{
		ToolCallID:  "call_ask_1",
		ToolName:    "ask_user",
		Answer:      "frontend",
		UsedDefault: false,
	}, "external_user")
	if err != nil {
		t.Fatalf("newRuntimeEvent returned error: %v", err)
	}
	if err := runner.HandleEvent(ctx, inputReceived); err != nil {
		t.Fatalf("HandleEvent UserInputReceived returned error: %v", err)
	}
	processNextEvent(t, runner, runID)

	runState, err := runner.states.Load(ctx, string(runID))
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	data, err := loadRunData(runState)
	if err != nil {
		t.Fatalf("loadRunData returned error: %v", err)
	}
	if !hasToolResultMessage(data.Messages, "call_ask_1", "frontend") {
		t.Fatalf("messages missing user answer tool result: %#v", data.Messages)
	}
	findPendingEffect(t, runner, runID, state.EffectCallModel)
}

func TestAgentRunnerPolicyApprovalSuspendsAndExecutesAfterApproval(t *testing.T) {
	ctx := context.Background()
	model := &scriptedLLM{}
	tools := &approvalTools{result: tool.Result{Content: "network sent"}}
	runner := mustRunner(t, mustRuntime(t, model), tools)
	runID := startAndProcessRunStarted(t, runner, Task{
		Input: "send network request",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	modelEffect := findPendingEffect(t, runner, runID, state.EffectCallModel)
	completeEffect(t, runner, runID, modelEffect.Effect.ID)

	response := llmClient.Response{
		Provider: "mock",
		Model:    "mock-native",
		ToolCalls: []llmClient.ToolCall{{
			ID:    "call_network_1",
			Name:  "network",
			Input: json.RawMessage(`{"url":"https://example.test"}`),
		}},
	}
	if err := runner.HandleEvent(ctx, modelResponseEvent(t, string(runID), modelEffect.Effect.ID, response)); err != nil {
		t.Fatalf("HandleEvent model response returned error: %v", err)
	}
	processNextEvent(t, runner, runID)
	if _, err := runner.DispatchNextEffect(ctx, runID); err != nil {
		t.Fatalf("Dispatch tool.dispatch returned error: %v", err)
	}
	processNextEvent(t, runner, runID)
	processNextEvent(t, runner, runID)
	if _, err := runner.DispatchNextEffect(ctx, runID); err != nil {
		t.Fatalf("Dispatch tool.execute returned error: %v", err)
	}
	if tools.executeCalls != 1 || tools.approvedCalls != 0 {
		t.Fatalf("tool calls execute=%d approved=%d, want one unapproved attempt", tools.executeCalls, tools.approvedCalls)
	}

	advanced, err := runner.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance UserApprovalRequired returned error: %v", err)
	}
	if advanced.Event == nil || advanced.Event.Type != runtimeevent.EventUserApprovalRequired {
		t.Fatalf("advance = %#v, want UserApprovalRequired", advanced)
	}
	result, err := runner.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Status != state.PhaseWaiting || result.State.Waiting == nil || result.State.Waiting.Reason != "user_approval" || result.State.Waiting.Target != "call_network_1" {
		t.Fatalf("state = %#v, want waiting user_approval for call_network_1", result.State)
	}

	approvalReceived, err := newRuntimeEvent(runtimeevent.EventUserApprovalReceived, string(runID), UserApprovalReceivedPayload{
		ToolCallID: "call_network_1",
		ToolName:   "network",
		Approved:   true,
	}, "external_user")
	if err != nil {
		t.Fatalf("newRuntimeEvent returned error: %v", err)
	}
	if err := runner.HandleEvent(ctx, approvalReceived); err != nil {
		t.Fatalf("HandleEvent UserApprovalReceived returned error: %v", err)
	}
	processNextEvent(t, runner, runID)

	executeEffect := findPendingEffect(t, runner, runID, state.EffectExecuteTool)
	dispatched, err := runner.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("Dispatch approved tool.execute returned error: %v", err)
	}
	if dispatched.Effect == nil || dispatched.Effect.ID != executeEffect.Effect.ID {
		t.Fatalf("approved dispatch = %#v, want execute effect %s", dispatched, executeEffect.Effect.ID)
	}
	if tools.executeCalls != 1 || tools.approvedCalls != 1 {
		t.Fatalf("tool calls execute=%d approved=%d, want approved execution", tools.executeCalls, tools.approvedCalls)
	}
	if len(dispatched.Events) != 1 || dispatched.Events[0].Type != runtimeevent.EventToolCallCompleted {
		t.Fatalf("approved events = %#v, want ToolCallCompleted", dispatched.Events)
	}
}

func TestAgentRunnerRecoverDiscoversNonTerminalRunsAndContinues(t *testing.T) {
	ctx := context.Background()
	states := state.NewMemoryStateStore()
	events := state.NewMemoryEventStore()
	effects := state.NewMemoryEffectStore()
	inbox := state.NewMemoryEventInbox()
	model := &scriptedLLM{responses: []llmClient.Response{{Provider: "mock", Model: "mock-native", Content: "done"}}}

	first := mustRunnerWithStores(t, mustRuntime(t, model), &recordingTools{}, states, events, effects, inbox)
	runID, err := first.Start(ctx, Task{
		Input: "recover me",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	processNextEvent(t, first, runID)
	if len(model.requests) != 0 {
		t.Fatalf("model requests = %d, want 0 before recovery", len(model.requests))
	}

	second := mustRunnerWithStores(t, mustRuntime(t, model), &recordingTools{}, states, events, effects, inbox)
	recovered, err := second.Recover(ctx)
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(recovered.Runs) != 1 {
		t.Fatalf("recovered = %#v, want one non-terminal run", recovered)
	}
	if recovered.Runs[0].RunID != string(runID) || recovered.Runs[0].PendingEvents != 1 || recovered.Runs[0].PendingEffects != 1 {
		t.Fatalf("recovered run = %#v, want run with one resume event and one pending effect", recovered.Runs[0])
	}

	recoveredAgain, err := second.Recover(ctx)
	if err != nil {
		t.Fatalf("second Recover returned error: %v", err)
	}
	if len(recoveredAgain.Runs) != 1 || recoveredAgain.Runs[0].PendingEvents != 1 {
		t.Fatalf("second recovered = %#v, want idempotent resume event", recoveredAgain)
	}

	advanced, err := second.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance RunResumed returned error: %v", err)
	}
	if advanced.Status != AdvanceStatusEventProcessed || advanced.Event == nil || advanced.Event.Type != runtimeevent.EventRunResumed {
		t.Fatalf("advance = %#v, want processed RunResumed", advanced)
	}

	dispatched, err := second.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("Dispatch model call returned error: %v", err)
	}
	if dispatched.Status != AdvanceStatusEffectDispatched || dispatched.Effect == nil || dispatched.Effect.Type != state.EffectCallModel {
		t.Fatalf("model call dispatch = %#v, want model.call effect", dispatched)
	}
	advanced, err = second.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance ModelRequestCreated returned error: %v", err)
	}
	if advanced.Event == nil || advanced.Event.Type != runtimeevent.EventModelRequestCreated {
		t.Fatalf("advance = %#v, want ModelRequestCreated", advanced)
	}
	dispatched, err = second.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("Dispatch model execute returned error: %v", err)
	}
	if dispatched.Effect == nil || dispatched.Effect.Type != state.EffectExecuteModel {
		t.Fatalf("model execute dispatch = %#v, want model.execute effect", dispatched)
	}
	if len(model.requests) != 1 {
		t.Fatalf("model requests = %d, want 1", len(model.requests))
	}
	advanced, err = second.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance ModelResponseReceived returned error: %v", err)
	}
	if advanced.Event == nil || advanced.Event.Type != runtimeevent.EventModelResponseReceived {
		t.Fatalf("advance = %#v, want ModelResponseReceived", advanced)
	}
	dispatched, err = second.DispatchNextEffect(ctx, runID)
	if err != nil {
		t.Fatalf("Dispatch complete run returned error: %v", err)
	}
	if dispatched.Effect == nil || dispatched.Effect.Type != state.EffectCompleteRun {
		t.Fatalf("complete dispatch = %#v, want run.complete effect", dispatched)
	}
	advanced, err = second.Advance(ctx, runID)
	if err != nil {
		t.Fatalf("Advance RunCompleted returned error: %v", err)
	}
	if advanced.Event == nil || advanced.Event.Type != runtimeevent.EventRunCompleted {
		t.Fatalf("advance = %#v, want RunCompleted", advanced)
	}

	result, err := second.Result(ctx, runID)
	if err != nil {
		t.Fatalf("Result returned error: %v", err)
	}
	if result.Status != state.PhaseCompleted || result.FinalAnswer != "done" {
		t.Fatalf("result = %#v, want completed done", result)
	}
	assertEventTypes(t, result.Events,
		runtimeevent.EventRunStarted,
		runtimeevent.EventRunResumed,
		runtimeevent.EventModelRequestCreated,
		runtimeevent.EventModelResponseReceived,
		runtimeevent.EventRunCompleted,
	)
}

func TestAgentRunnerCompletesWithoutTools(t *testing.T) {
	model := &scriptedLLM{responses: []llmClient.Response{{Provider: "mock", Model: "mock-native", Content: "done"}}}
	runner := mustRunner(t, mustRuntime(t, model), &recordingTools{})

	result, err := runner.Run(context.Background(), Task{
		Input: "build",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Status != state.PhaseCompleted || result.FinalAnswer != "done" || result.StepsUsed != 1 {
		t.Fatalf("result = %#v, want completed done with one step", result)
	}
	if len(model.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(model.requests))
	}
	assertEventTypes(t, result.Events,
		runtimeevent.EventRunStarted,
		runtimeevent.EventModelRequestCreated,
		runtimeevent.EventModelResponseReceived,
		runtimeevent.EventRunCompleted,
	)
}

func TestAgentRunnerDispatchesToolAndContinuesModel(t *testing.T) {
	model := &scriptedLLM{responses: []llmClient.Response{
		{
			Provider: "mock",
			Model:    "mock-native",
			Content:  "need lookup",
			ToolCalls: []llmClient.ToolCall{{
				ID:    "call_lookup_1",
				Name:  "lookup",
				Input: json.RawMessage(`{"query":"x"}`),
			}},
		},
		{Provider: "mock", Model: "mock-native", Content: "final answer"},
	}}
	tools := &recordingTools{result: tool.Result{Content: "lookup result"}}
	runner := mustRunner(t, mustRuntime(t, model), tools)

	result, err := runner.Run(context.Background(), Task{
		Input: "lookup x",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Status != state.PhaseCompleted || result.FinalAnswer != "final answer" || result.StepsUsed != 2 {
		t.Fatalf("result = %#v, want final answer after two steps", result)
	}
	if len(tools.calls) != 1 || tools.calls[0].name != "lookup" || string(tools.calls[0].input) != `{"query":"x"}` {
		t.Fatalf("tool calls = %#v, want lookup call", tools.calls)
	}
	if len(model.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(model.requests))
	}
	if !hasToolResultMessage(model.requests[1].Messages, "call_lookup_1", "lookup result") {
		t.Fatalf("second request messages missing tool result: %#v", model.requests[1].Messages)
	}
	assertEventTypes(t, result.Events,
		runtimeevent.EventRunStarted,
		runtimeevent.EventModelRequestCreated,
		runtimeevent.EventModelResponseReceived,
		runtimeevent.EventToolCallRequested,
		runtimeevent.EventToolCallDispatched,
		runtimeevent.EventToolCallCompleted,
		runtimeevent.EventModelRequestCreated,
		runtimeevent.EventModelResponseReceived,
		runtimeevent.EventRunCompleted,
	)
}

func TestAgentRunnerHandlesRepeatedToolCallIDs(t *testing.T) {
	model := &scriptedLLM{responses: []llmClient.Response{
		{Provider: "mock", Model: "mock-native", ToolCalls: []llmClient.ToolCall{{ID: "call_same", Name: "lookup", Input: json.RawMessage(`{"step":1}`)}}},
		{Provider: "mock", Model: "mock-native", ToolCalls: []llmClient.ToolCall{{ID: "call_same", Name: "lookup", Input: json.RawMessage(`{"step":2}`)}}},
		{Provider: "mock", Model: "mock-native", Content: "done"},
	}}
	tools := &recordingTools{result: tool.Result{Content: "ok"}}
	runner := mustRunner(t, mustRuntime(t, model), tools)

	result, err := runner.Run(context.Background(), Task{
		Input:    "repeat ids",
		Agent:    llm.AgentProfile{Name: "default", Model: "mock-native"},
		MaxSteps: 5,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Status != state.PhaseCompleted || result.FinalAnswer != "done" || result.StepsUsed != 3 {
		t.Fatalf("result = %#v, want completed after repeated ids", result)
	}
	if len(tools.calls) != 2 {
		t.Fatalf("tool calls = %d, want 2", len(tools.calls))
	}
}

func TestAgentRunnerDoesNotPublishEffectEventsAfterLeaseExpiry(t *testing.T) {
	worker := EffectWorkerFunc(func(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error) {
		time.Sleep(10 * time.Millisecond)
		event, err := newRuntimeEvent(runtimeevent.EventRunCompleted, effect.RunID, CompleteRunPayload{FinalAnswer: "stale"}, effect.ID)
		if err != nil {
			return nil, err
		}
		return []runtimeevent.Event{event}, nil
	})
	runner, err := NewAgentRunner(Options{
		EffectWorker:  worker,
		WorkerOwner:   "worker_a",
		LeaseDuration: time.Millisecond,
		MaxSteps:      5,
	})
	if err != nil {
		t.Fatalf("NewAgentRunner returned error: %v", err)
	}

	result, err := runner.Run(context.Background(), Task{
		Input: "build",
		Agent: llm.AgentProfile{Name: "default", Model: "mock-native"},
	})
	if !errors.Is(err, state.ErrLeaseExpired) {
		t.Fatalf("Run error = %v, want ErrLeaseExpired", err)
	}

	latest, resultErr := runner.Result(context.Background(), RunID(result.RunID))
	if resultErr != nil {
		t.Fatalf("Result returned error: %v", resultErr)
	}
	assertEventTypes(t, latest.Events, runtimeevent.EventRunStarted)
	if latest.Status != state.PhaseWaiting || latest.FinalAnswer != "" {
		t.Fatalf("latest result = %#v, want waiting without stale final answer", latest)
	}

	claimed, ok, err := runner.effectStore.Claim(context.Background(), result.RunID, "worker_b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("worker_b Claim ok=%v err=%v, want reclaimable expired effect", ok, err)
	}
	if claimed.Owner != "worker_b" || claimed.ClaimCount != 2 {
		t.Fatalf("claimed = %#v, want worker_b second claim", claimed)
	}
}

type scriptedLLM struct {
	requests  []llmClient.Request
	responses []llmClient.Response
}

func (c *scriptedLLM) Complete(_ context.Context, req llmClient.Request) (llmClient.Response, error) {
	c.requests = append(c.requests, cloneRequest(req))
	if len(c.responses) == 0 {
		return llmClient.Response{}, nil
	}
	response := c.responses[0]
	c.responses = c.responses[1:]
	return response, nil
}

type recordingTools struct {
	calls  []recordedToolCall
	result tool.Result
}

type approvalTools struct {
	executeCalls  int
	approvedCalls int
	result        tool.Result
}

type EffectWorkerFunc func(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error)

func (f EffectWorkerFunc) Execute(ctx context.Context, effect state.Effect) ([]runtimeevent.Event, error) {
	return f(ctx, effect)
}

type recordedToolCall struct {
	name  string
	input json.RawMessage
}

func (t *recordingTools) Execute(_ context.Context, name string, input json.RawMessage) (tool.Result, error) {
	t.calls = append(t.calls, recordedToolCall{name: name, input: append(json.RawMessage(nil), input...)})
	return t.result, nil
}

func (t *approvalTools) Execute(_ context.Context, name string, input json.RawMessage) (tool.Result, error) {
	t.executeCalls++
	return tool.Result{}, &tool.ApprovalRequiredError{
		Request: policy.Request{ToolName: name, Risk: policy.RiskNet, Operation: name},
		Result:  policy.Result{Decision: policy.Ask, Reason: "approval required"},
	}
}

func (t *approvalTools) ExecuteApproved(_ context.Context, name string, input json.RawMessage) (tool.Result, error) {
	t.approvedCalls++
	return t.result, nil
}

func mustRuntime(t *testing.T, model *scriptedLLM) *llm.Runtime {
	t.Helper()
	rt, err := llm.NewRuntime(llm.Options{LLM: model})
	if err != nil {
		t.Fatalf("NewRuntime returned error: %v", err)
	}
	return rt
}

func mustRunner(t *testing.T, rt *llm.Runtime, tools ToolRegistry) *AgentRunner {
	t.Helper()
	return mustRunnerWithOptions(t, Options{LLM: rt, ToolRegistry: tools, MaxSteps: 5})
}

func mustRunnerWithOptions(t *testing.T, opts Options) *AgentRunner {
	t.Helper()
	runner, err := NewAgentRunner(opts)
	if err != nil {
		t.Fatalf("NewAgentRunner returned error: %v", err)
	}
	return runner
}

func mustRunnerWithStores(t *testing.T, rt *llm.Runtime, tools ToolRegistry, states state.StateStore, events state.EventStore, effects state.EffectStore, inbox state.EventInboxStore) *AgentRunner {
	t.Helper()
	runner, err := NewAgentRunner(Options{
		LLM:          rt,
		ToolRegistry: tools,
		StateStore:   states,
		EventStore:   events,
		EffectStore:  effects,
		EventInbox:   inbox,
		MaxSteps:     5,
	})
	if err != nil {
		t.Fatalf("NewAgentRunner returned error: %v", err)
	}
	return runner
}

func startAndProcessRunStarted(t *testing.T, runner *AgentRunner, task Task) RunID {
	t.Helper()
	runID, err := runner.Start(context.Background(), task)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	processNextEvent(t, runner, runID)
	return runID
}

func processNextEvent(t *testing.T, runner *AgentRunner, runID RunID) {
	t.Helper()
	processed, err := runner.ProcessNextEvent(context.Background(), runID)
	if err != nil {
		t.Fatalf("ProcessNextEvent returned error: %v", err)
	}
	if !processed {
		t.Fatalf("ProcessNextEvent processed false, want true")
	}
}

func completeEffect(t *testing.T, runner *AgentRunner, runID RunID, effectID string) {
	t.Helper()
	ctx := context.Background()
	claimed, ok, err := runner.effectStore.Claim(ctx, string(runID), runner.workerOwner, runner.leaseDuration)
	if err != nil {
		t.Fatalf("Claim effect returned error: %v", err)
	}
	if !ok {
		t.Fatalf("Claim effect returned ok=false, want %s", effectID)
	}
	if claimed.Effect.ID != effectID {
		t.Fatalf("claimed effect id = %q, want %q", claimed.Effect.ID, effectID)
	}
	if err := runner.effectStore.MarkCompleted(ctx, effectID, runner.workerOwner); err != nil {
		t.Fatalf("MarkCompleted effect returned error: %v", err)
	}
}

func modelResponseEvent(t *testing.T, runID string, effectID string, response llmClient.Response) runtimeevent.Event {
	t.Helper()
	toolCalls := cloneToolCallsForTest(response.ToolCalls)
	assistant, ok := llm.NewAssistantMessage(response, toolCalls)
	payload := ModelResponseReceivedPayload{
		Response:  response,
		ToolCalls: toolCalls,
	}
	if ok {
		payload.AssistantMessage = &assistant
	}
	event, err := newRuntimeEvent(runtimeevent.EventModelResponseReceived, runID, payload, effectID)
	if err != nil {
		t.Fatalf("newRuntimeEvent returned error: %v", err)
	}
	return event
}

func cloneToolCallsForTest(calls []llmClient.ToolCall) []llmClient.ToolCall {
	if len(calls) == 0 {
		return nil
	}
	cloned := make([]llmClient.ToolCall, 0, len(calls))
	for _, call := range calls {
		cloned = append(cloned, cloneToolCall(call))
	}
	return cloned
}

func listPendingEvents(t *testing.T, runner *AgentRunner, runID RunID) []state.StoredEvent {
	t.Helper()
	events, err := runner.eventInbox.ListPending(context.Background(), string(runID))
	if err != nil {
		t.Fatalf("ListPending events returned error: %v", err)
	}
	return events
}

func assertNoPendingEffects(t *testing.T, runner *AgentRunner, runID RunID) {
	t.Helper()
	effects, err := runner.effectStore.ListPending(context.Background(), string(runID))
	if err != nil {
		t.Fatalf("ListPending effects returned error: %v", err)
	}
	if len(effects) != 0 {
		t.Fatalf("pending effects = %#v, want none", effects)
	}
}

func pendingEffectsOfType(t *testing.T, runner *AgentRunner, runID RunID, effectType state.EffectType) []state.StoredEffect {
	t.Helper()
	effects, err := runner.effectStore.ListPending(context.Background(), string(runID))
	if err != nil {
		t.Fatalf("ListPending returned error: %v", err)
	}
	matched := make([]state.StoredEffect, 0, len(effects))
	for _, effect := range effects {
		if effect.Effect.Type == effectType {
			matched = append(matched, effect)
		}
	}
	return matched
}

func findPendingEffect(t *testing.T, runner *AgentRunner, runID RunID, effectType state.EffectType) state.StoredEffect {
	t.Helper()
	matched := pendingEffectsOfType(t, runner, runID, effectType)
	if len(matched) > 0 {
		return matched[0]
	}
	effects, _ := runner.effectStore.ListPending(context.Background(), string(runID))
	t.Fatalf("pending effects = %#v, want %s", effects, effectType)
	return state.StoredEffect{}
}

func hasToolResultMessage(messages []llmClient.Message, toolCallID string, content string) bool {
	for _, message := range messages {
		if message.Role == llmClient.RoleTool && message.ToolCallID == toolCallID && strings.Contains(message.Content, content) {
			return true
		}
	}
	return false
}

func assertEventTypes(t *testing.T, events []runtimeevent.Event, want ...runtimeevent.Type) {
	t.Helper()
	if len(events) != len(want) {
		t.Fatalf("events = %d, want %d: %#v", len(events), len(want), events)
	}
	for i, event := range events {
		if event.Type != want[i] {
			t.Fatalf("event[%d] = %q, want %q", i, event.Type, want[i])
		}
	}
}

func cloneRequest(request llmClient.Request) llmClient.Request {
	cloned := request
	if len(request.Messages) > 0 {
		cloned.Messages = append([]llmClient.Message(nil), request.Messages...)
	}
	if len(request.Tools) > 0 {
		cloned.Tools = append([]llmClient.ToolDefinition(nil), request.Tools...)
	}
	if len(request.Metadata) > 0 {
		cloned.Metadata = make(map[string]string, len(request.Metadata))
		for key, value := range request.Metadata {
			cloned.Metadata[key] = value
		}
	}
	return cloned
}

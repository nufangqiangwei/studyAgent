package runner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"agent/internal/capability/tool"
	runtimeevent "agent/internal/event"
	"agent/internal/foundation/llmClient"
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
	if err := runner.effectStore.MarkCompleted(ctx, modelEffect.Effect.ID); err != nil {
		t.Fatalf("MarkCompleted returned error: %v", err)
	}

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
	if err := runner.effectStore.MarkCompleted(ctx, modelEffect.Effect.ID); err != nil {
		t.Fatalf("MarkCompleted model effect returned error: %v", err)
	}

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
	if err := runner.effectStore.MarkCompleted(ctx, toolEffect.Effect.ID); err != nil {
		t.Fatalf("MarkCompleted tool effect returned error: %v", err)
	}

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

type recordedToolCall struct {
	name  string
	input json.RawMessage
}

func (t *recordingTools) Execute(_ context.Context, name string, input json.RawMessage) (tool.Result, error) {
	t.calls = append(t.calls, recordedToolCall{name: name, input: append(json.RawMessage(nil), input...)})
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
	runner, err := NewAgentRunner(Options{LLM: rt, ToolRegistry: tools, MaxSteps: 5})
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

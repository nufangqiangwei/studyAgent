package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAnalyzeAgentStartRequestsModelThenProducesToolIntent(t *testing.T) {
	store := agents2.NewMemorySnapshotStore()
	agent := mustAnalyzeAgent(t, nil, WithSnapshotStore(store), WithTools([]agents2.ToolSpec{{Name: "workspace.list"}}))

	startResult, err := agent.Start(context.Background(), agents2.AgentStartInput{
		TaskID: "task_1",
		Input:  "analyze this repo",
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	modelPayload, request := mustModelRequestEvent(t, startResult.Events[0])
	if request.Trigger != "start" || request.Input != "analyze this repo" || request.ModelCallID != modelPayload.ModelCallID {
		t.Fatalf("model request = %#v payload=%#v, want start request", request, modelPayload)
	}
	snapshot := mustSnapshot(t, agent, "task_1")
	if snapshot.Phase != agents2.BusinessPhaseCallingModel || snapshot.PendingModelCallID != modelPayload.ModelCallID {
		t.Fatalf("snapshot = %#v, want CallingModel with pending model call", snapshot)
	}

	result := resumeWithModelResponse(t, agent, "task_1", modelPayload.ModelCallID, agents2.ModelResponse{Decision: &agents2.Decision{
		Action: agents2.ActionUseTool,
		Plan: []agents2.PlanStep{
			{ID: "1", Goal: "inspect project", Status: "active"},
		},
		Tool: &agents2.ToolIntent{
			ToolCallID: "call_1",
			ToolName:   "workspace.list",
			Arguments:  json.RawMessage(`{"path":"."}`),
		},
	}})
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventAgentToolRequested {
		t.Fatalf("events = %#v, want agent.tool_requested", result.Events)
	}
	var payload statemachine.ToolCallPayload
	mustUnmarshal(t, result.Events[0].Payload, &payload)
	if payload.ToolCallID != "call_1" || payload.ToolName != "workspace.list" || string(payload.Arguments) != `{"path":"."}` {
		t.Fatalf("payload = %#v, want workspace.list call_1", payload)
	}
	snapshot = mustSnapshot(t, agent, "task_1")
	if snapshot.Phase != agents2.BusinessPhaseCallingTool || snapshot.PendingModelCallID != "" || snapshot.PendingToolCallID != "call_1" || len(snapshot.Plan) != 1 {
		t.Fatalf("snapshot = %#v, want CallingTool with pending call_1 and plan", snapshot)
	}
}

func TestToolsTesterAgentStartsWithToolTesterPromptAndName(t *testing.T) {
	store := agents2.NewMemorySnapshotStore()
	agent, err := NewToolsTesterAgent(
		WithSnapshotStore(store),
		WithModelName("mock-native"),
		WithTools([]agents2.ToolSpec{{Name: "read_file"}}),
		WithAgentClock(func() time.Time {
			return time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC)
		}),
	)
	if err != nil {
		t.Fatalf("NewToolsTesterAgent returned error: %v", err)
	}
	if agent.Name() != ToolsTesterAgentName {
		t.Fatalf("Name = %q, want %q", agent.Name(), ToolsTesterAgentName)
	}

	startResult, err := agent.Start(context.Background(), agents2.AgentStartInput{
		TaskID: "task_tools",
		Input:  "test read_file tool",
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	payload, request := mustModelRequestEvent(t, startResult.Events[0])
	if request.Agent != ToolsTesterAgentName || payload.Agent != ToolsTesterAgentName {
		t.Fatalf("request agent=%q payload agent=%q, want %q", request.Agent, payload.Agent, ToolsTesterAgentName)
	}
	if request.Model != "mock-native" {
		t.Fatalf("request model = %q, want mock-native", request.Model)
	}
	if len(request.Tools) != 1 || request.Tools[0].Name != "read_file" {
		t.Fatalf("request tools = %#v, want read_file", request.Tools)
	}
	if len(request.Messages) == 0 || !strings.Contains(request.Messages[0].Content, "Tool Testing Agent") {
		t.Fatalf("system prompt missing tool tester instructions: %#v", request.Messages)
	}
	if startResult.Events[0].Metadata["agent"] != ToolsTesterAgentName {
		t.Fatalf("event metadata = %#v, want tool-tester agent", startResult.Events[0].Metadata)
	}
	snapshot, ok, err := store.Load(context.Background(), ToolsTesterAgentName, "task_tools")
	if err != nil || !ok {
		t.Fatalf("store.Load ok=%v err=%v, want snapshot", ok, err)
	}
	if snapshot.Agent != ToolsTesterAgentName || snapshot.Phase != agents2.BusinessPhaseCallingModel {
		t.Fatalf("snapshot = %#v, want tool tester calling model", snapshot)
	}
}

func TestAnalyzeAgentBuildSystemPromptHookReceivesStartInput(t *testing.T) {
	agent := mustAnalyzeAgent(t, nil, WithAgentRuntimeHooks(AgentRuntimeHooks{
		BuildSystemPrompt: func(ctx context.Context, input agents2.AgentStartInput) ([]agents2.Message, error) {
			if ctx == nil {
				t.Fatalf("ctx is nil")
			}
			if input.TaskID != "task_hook" || input.Input != "inspect runtime hooks" || input.Metadata["profile"] != "local" {
				t.Fatalf("input = %#v, want start input", input)
			}
			return []agents2.Message{
				{Role: "system", Content: "hooked system prompt for " + input.Input},
				{Role: "user", Content: input.Input},
			}, nil
		},
	}))

	startResult, err := agent.Start(context.Background(), agents2.AgentStartInput{
		TaskID: "task_hook",
		Input:  "inspect runtime hooks",
		Metadata: map[string]string{
			"profile": "local",
		},
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	_, request := mustModelRequestEvent(t, startResult.Events[0])
	if len(request.Messages) < 2 {
		t.Fatalf("messages = %#v, want system and user messages", request.Messages)
	}
	if request.Messages[0].Role != "system" || request.Messages[0].Content != "hooked system prompt for inspect runtime hooks" {
		t.Fatalf("system message = %#v, want hooked prompt", request.Messages[0])
	}
	if request.Messages[1].Role != "user" || request.Messages[1].Content != "inspect runtime hooks" {
		t.Fatalf("user message = %#v, want start input", request.Messages[1])
	}
}

func TestAnalyzeAgentResumeToolResultRequestsModelThenCompletesTask(t *testing.T) {
	agent := mustAnalyzeAgent(t, nil)
	startResult, err := agent.Start(context.Background(), agents2.AgentStartInput{TaskID: "task_1", Input: "inspect module"})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	startModelPayload, _ := mustModelRequestEvent(t, startResult.Events[0])
	result := resumeWithModelResponse(t, agent, "task_1", startModelPayload.ModelCallID, agents2.ModelResponse{Decision: &agents2.Decision{
		Action: agents2.ActionUseTool,
		Tool:   &agents2.ToolIntent{ToolCallID: "call_1", ToolName: "workspace.read", Arguments: json.RawMessage(`{"path":"go.mod"}`)},
	}})
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventAgentToolRequested {
		t.Fatalf("events = %#v, want tool request", result.Events)
	}

	result, err = agent.Resume(context.Background(), agents2.AgentResumeInput{
		TaskID: "task_1",
		Payload: mustMarshal(t, statemachine.ToolCallPayload{
			ToolCallID: "call_1",
			ToolName:   "workspace.read",
			Result:     json.RawMessage(`{"content":"module agent"}`),
		}),
	})
	if err != nil {
		t.Fatalf("Resume returned error: %v", err)
	}
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventAgentModelRequested {
		t.Fatalf("events = %#v, want model request after tool result", result.Events)
	}
	toolModelPayload, request := mustModelRequestEvent(t, result.Events[0])
	if request.Trigger != "tool_result" {
		t.Fatalf("model request trigger = %q, want tool_result", request.Trigger)
	}

	result = resumeWithModelResponse(t, agent, "task_1", toolModelPayload.ModelCallID, agents2.ModelResponse{Decision: &agents2.Decision{
		Action:      agents2.ActionComplete,
		FinalAnswer: "go module inspected",
		Result:      json.RawMessage(`{"summary":"go module inspected"}`),
	}})
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventAgentCompleted {
		t.Fatalf("events = %#v, want agent.completed", result.Events)
	}
	var payload statemachine.AgentCompletedPayload
	mustUnmarshal(t, result.Events[0].Payload, &payload)
	if string(payload.Result) != `{"summary":"go module inspected"}` {
		t.Fatalf("completion payload = %s, want summary", payload.Result)
	}
	snapshot := mustSnapshot(t, agent, "task_1")
	if snapshot.Phase != agents2.BusinessPhaseCompleted || snapshot.PendingModelCallID != "" || snapshot.PendingToolCallID != "" || snapshot.LastToolResult == nil {
		t.Fatalf("snapshot = %#v, want completed with cleared pending calls and last result", snapshot)
	}
}

func TestAnalyzeAgentCanAskUserAndCreateSubAgentAfterModelResponse(t *testing.T) {
	userAgent := mustAnalyzeAgent(t, nil)
	userStart, err := userAgent.Start(context.Background(), agents2.AgentStartInput{TaskID: "task_user", Input: "inspect"})
	if err != nil {
		t.Fatalf("user Start returned error: %v", err)
	}
	userPayload, _ := mustModelRequestEvent(t, userStart.Events[0])
	userResult := resumeWithModelResponse(t, userAgent, "task_user", userPayload.ModelCallID, agents2.ModelResponse{Decision: &agents2.Decision{
		Action:    agents2.ActionAskUser,
		UserInput: &agents2.UserInputIntent{RequestID: "input_1", Prompt: "Which package should I inspect?"},
	}})
	if len(userResult.Events) != 1 || userResult.Events[0].Type != statemachine.EventAgentUserInputRequested {
		t.Fatalf("user events = %#v, want user input request", userResult.Events)
	}
	userSnapshot := mustSnapshot(t, userAgent, "task_user")
	if userSnapshot.Phase != agents2.BusinessPhaseWaitingUser || userSnapshot.PendingUserInputID != "input_1" {
		t.Fatalf("user snapshot = %#v, want waiting input_1", userSnapshot)
	}

	subAgent := mustAnalyzeAgent(t, nil)
	subStart, err := subAgent.Start(context.Background(), agents2.AgentStartInput{TaskID: "task_sub", Input: "delegate review"})
	if err != nil {
		t.Fatalf("sub Start returned error: %v", err)
	}
	subPayload, _ := mustModelRequestEvent(t, subStart.Events[0])
	subResult := resumeWithModelResponse(t, subAgent, "task_sub", subPayload.ModelCallID, agents2.ModelResponse{Decision: &agents2.Decision{
		Action:   agents2.ActionCreateSubAgent,
		SubAgent: &agents2.SubAgentIntent{SubTaskID: "sub_1", Agent: "review", Input: "review docs"},
	}})
	if len(subResult.Events) != 1 || subResult.Events[0].Type != statemachine.EventAgentSubAgentRequested {
		t.Fatalf("sub events = %#v, want sub-agent request", subResult.Events)
	}
	subSnapshot := mustSnapshot(t, subAgent, "task_sub")
	if subSnapshot.Phase != agents2.BusinessPhaseWaitingSubAgent || len(subSnapshot.SubTasks) != 1 || subSnapshot.SubTasks[0].SubTaskID != "sub_1" {
		t.Fatalf("sub snapshot = %#v, want waiting sub_1", subSnapshot)
	}
}

func TestAnalyzeAgentModelFailureEventBecomesAgentFailedEvent(t *testing.T) {
	agent := mustAnalyzeAgent(t, nil)
	startResult, err := agent.Start(context.Background(), agents2.AgentStartInput{TaskID: "task_1", Input: "inspect"})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	modelPayload, _ := mustModelRequestEvent(t, startResult.Events[0])

	result, err := agent.Resume(context.Background(), agents2.AgentResumeInput{
		TaskID: "task_1",
		Payload: mustMarshal(t, statemachine.ModelCallPayload{
			ModelCallID: modelPayload.ModelCallID,
			Error:       "model unavailable",
		}),
	})
	if err != nil {
		t.Fatalf("Resume returned error: %v", err)
	}

	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventAgentFailed {
		t.Fatalf("events = %#v, want agent.failed", result.Events)
	}
	var payload statemachine.AgentFailedPayload
	mustUnmarshal(t, result.Events[0].Payload, &payload)
	if payload.Code != "model_error" || payload.Message != "model unavailable" {
		t.Fatalf("payload = %#v, want model_error", payload)
	}
	snapshot := mustSnapshot(t, agent, "task_1")
	if snapshot.Phase != agents2.BusinessPhaseFailed || snapshot.FailureCount != 1 || snapshot.PendingModelCallID != "" {
		t.Fatalf("snapshot = %#v, want failed with one failure", snapshot)
	}
}

func TestAnalyzeAgentParsesJSONDecisionContentFromModelResponse(t *testing.T) {
	agent := mustAnalyzeAgent(t, nil)
	startResult, err := agent.Start(context.Background(), agents2.AgentStartInput{TaskID: "task_1", Input: "inspect"})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	modelPayload, _ := mustModelRequestEvent(t, startResult.Events[0])

	result := resumeWithModelResponse(t, agent, "task_1", modelPayload.ModelCallID, agents2.ModelResponse{
		Content: `{"action":"complete","final_answer":"done"}`,
	})
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventAgentCompleted {
		t.Fatalf("events = %#v, want agent.completed", result.Events)
	}
}

func TestAgentExecutorRunsAnalyzeAgentForReactorEffects(t *testing.T) {
	agent := mustAnalyzeAgent(t, nil)
	registry, err := agents2.NewRegistry(agent)
	if err != nil {
		t.Fatalf("agents.NewRegistry returned error: %v", err)
	}
	executor, err := agents2.NewAgentExecutor(registry)
	if err != nil {
		t.Fatalf("agents.NewAgentExecutor returned error: %v", err)
	}
	effect, err := reactor.NewEffect("task_1", reactor.EffectAgentStart, statemachine.TaskStartPayload{
		Agent: AnalyzeAgentName,
		Input: "analyze",
	})
	if err != nil {
		t.Fatalf("NewEffect returned error: %v", err)
	}

	result, err := executor.ExecuteEffect(context.Background(), reactor.TaskRuntime{TaskID: "task_1", Agent: AnalyzeAgentName}, effect)
	if err != nil {
		t.Fatalf("ExecuteEffect returned error: %v", err)
	}
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventAgentModelRequested {
		t.Fatalf("events = %#v, want agent.model_requested", result.Events)
	}
}

func TestModelExecutorCallsModelClientAndEmitsModelResponseEvent(t *testing.T) {
	model := &scriptedModel{responses: []agents2.ModelResponse{{Decision: &agents2.Decision{
		Action:      agents2.ActionComplete,
		FinalAnswer: "done",
	}}}}
	executor, err := agents2.NewModelExecutor(model)
	if err != nil {
		t.Fatalf("agents.NewModelExecutor returned error: %v", err)
	}
	rawRequest, err := agents2.MarshalModelRequest(agents2.ModelRequest{
		ModelCallID: "model_1",
		TaskID:      "task_1",
		Agent:       AnalyzeAgentName,
		Trigger:     "start",
		Messages:    []agents2.Message{{Role: "user", Content: "inspect"}},
	})
	if err != nil {
		t.Fatalf("agents.MarshalModelRequest returned error: %v", err)
	}
	effect, err := reactor.NewEffect("task_1", reactor.EffectModelCall, statemachine.ModelCallPayload{
		ModelCallID: "model_1",
		Agent:       AnalyzeAgentName,
		Request:     rawRequest,
	})
	if err != nil {
		t.Fatalf("NewEffect returned error: %v", err)
	}

	result, err := executor.ExecuteEffect(context.Background(), reactor.TaskRuntime{TaskID: "task_1"}, effect)
	if err != nil {
		t.Fatalf("ExecuteEffect returned error: %v", err)
	}
	if len(model.requests) != 1 || model.requests[0].Trigger != "start" {
		t.Fatalf("model requests = %#v, want one start request", model.requests)
	}
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventModelResponseReceived {
		t.Fatalf("events = %#v, want model.response_received", result.Events)
	}
	var payload statemachine.ModelCallPayload
	mustUnmarshal(t, result.Events[0].Payload, &payload)
	response, err := agents2.UnmarshalModelResponse(payload.Response)
	if err != nil {
		t.Fatalf("agents.UnmarshalModelResponse returned error: %v", err)
	}
	decision, err := response.ResolveDecision()
	if err != nil {
		t.Fatalf("ResolveDecision returned error: %v", err)
	}
	if decision.Action != agents2.ActionComplete {
		t.Fatalf("decision = %#v, want complete", decision)
	}
}

func TestModelExecutorMapsModelClientErrorToModelFailedEvent(t *testing.T) {
	model := &scriptedModel{err: errors.New("model unavailable")}
	executor, err := agents2.NewModelExecutor(model)
	if err != nil {
		t.Fatalf("agents.NewModelExecutor returned error: %v", err)
	}
	rawRequest, err := agents2.MarshalModelRequest(agents2.ModelRequest{
		ModelCallID: "model_1",
		TaskID:      "task_1",
		Agent:       AnalyzeAgentName,
		Trigger:     "start",
		Messages:    []agents2.Message{{Role: "user", Content: "inspect"}},
	})
	if err != nil {
		t.Fatalf("agents.MarshalModelRequest returned error: %v", err)
	}
	effect, err := reactor.NewEffect("task_1", reactor.EffectModelCall, statemachine.ModelCallPayload{
		ModelCallID: "model_1",
		Agent:       AnalyzeAgentName,
		Request:     rawRequest,
	})
	if err != nil {
		t.Fatalf("NewEffect returned error: %v", err)
	}

	result, err := executor.ExecuteEffect(context.Background(), reactor.TaskRuntime{TaskID: "task_1"}, effect)
	if err != nil {
		t.Fatalf("ExecuteEffect returned error: %v", err)
	}
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventModelResponseFailed {
		t.Fatalf("events = %#v, want model.response_failed", result.Events)
	}
	var payload statemachine.ModelCallPayload
	mustUnmarshal(t, result.Events[0].Payload, &payload)
	if !strings.Contains(payload.Error, "model unavailable") {
		t.Fatalf("payload error = %q, want model unavailable", payload.Error)
	}
}

func mustAnalyzeAgent(t *testing.T, model agents2.ModelClient, options ...AnalyzeAgentOption) *AnalyzeAgent {
	t.Helper()
	base := []AnalyzeAgentOption{
		WithModel(model),
		WithAgentClock(func() time.Time {
			return time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC)
		}),
	}
	base = append(base, options...)
	agent, err := NewAnalyzeAgent(base...)
	if err != nil {
		t.Fatalf("NewAnalyzeAgent returned error: %v", err)
	}
	return agent
}

func mustModelRequestEvent(t *testing.T, event eventbus.Event) (statemachine.ModelCallPayload, agents2.ModelRequest) {
	t.Helper()
	if event.Type != statemachine.EventAgentModelRequested {
		t.Fatalf("event = %#v, want agent.model_requested", event)
	}
	var payload statemachine.ModelCallPayload
	mustUnmarshal(t, event.Payload, &payload)
	request, err := agents2.UnmarshalModelRequest(payload.Request)
	if err != nil {
		t.Fatalf("agents.UnmarshalModelRequest returned error: %v", err)
	}
	return payload, request
}

func resumeWithModelResponse(t *testing.T, agent *AnalyzeAgent, taskID string, modelCallID string, response agents2.ModelResponse) agents2.AgentResult {
	t.Helper()
	rawResponse, err := agents2.MarshalModelResponse(response)
	if err != nil {
		t.Fatalf("agents.MarshalModelResponse returned error: %v", err)
	}
	result, err := agent.Resume(context.Background(), agents2.AgentResumeInput{
		TaskID: taskID,
		Payload: mustMarshal(t, statemachine.ModelCallPayload{
			ModelCallID: modelCallID,
			Response:    rawResponse,
		}),
	})
	if err != nil {
		t.Fatalf("Resume returned error: %v", err)
	}
	return result
}

func mustSnapshot(t *testing.T, agent agents2.Agent, taskID string) agents2.AgentSnapshot {
	t.Helper()
	snapshot, ok, err := agent.Snapshot(context.Background(), taskID)
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	if !ok {
		t.Fatalf("snapshot %s not found", taskID)
	}
	return snapshot
}

func mustMarshal(t *testing.T, payload any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	return json.RawMessage(raw)
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
}

type scriptedModel struct {
	requests  []agents2.ModelRequest
	responses []agents2.ModelResponse
	err       error
}

func (m *scriptedModel) Complete(_ context.Context, request agents2.ModelRequest) (agents2.ModelResponse, error) {
	m.requests = append(m.requests, request.Clone())
	if m.err != nil {
		return agents2.ModelResponse{}, m.err
	}
	if len(m.responses) == 0 {
		return agents2.ModelResponse{}, nil
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return response.Clone(), nil
}

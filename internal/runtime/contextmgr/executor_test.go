package contextmgr

import (
	"agent/internal/foundation/llmClient"
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/agents/builtinagents"
	reactor2 "agent/internal/runtime/reactor"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"testing"
)

func TestModelExecutorCallsManagerAndEmitsModelResponse(t *testing.T) {
	model := &recordingLLM{response: llmClient.Response{
		Provider: "mock",
		Model:    "mock-native",
		Content:  `{"action":"complete","final_answer":"done"}`,
	}}
	manager, err := NewManager(Options{LLM: model})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	executor, err := NewModelExecutor(manager)
	if err != nil {
		t.Fatalf("NewModelExecutor returned error: %v", err)
	}
	rawRequest, err := agents2.MarshalModelRequest(agents2.ModelRequest{
		ModelCallID: "model_1",
		TaskID:      "task_1",
		Agent:       builtinagents.AnalyzeAgentName,
		Model:       "mock-native",
		Trigger:     "start",
		Messages:    []agents2.Message{{Role: "user", Content: "inspect"}},
	})
	if err != nil {
		t.Fatalf("MarshalModelRequest returned error: %v", err)
	}
	effect, err := reactor2.NewEffect("task_1", reactor2.EffectModelCall, statemachine.ModelCallPayload{
		ModelCallID: "model_1",
		Agent:       builtinagents.AnalyzeAgentName,
		Request:     rawRequest,
	})
	if err != nil {
		t.Fatalf("NewEffect returned error: %v", err)
	}

	result, err := executor.ExecuteEffect(context.Background(), reactor2.TaskRuntime{TaskID: "task_1"}, effect)
	if err != nil {
		t.Fatalf("ExecuteEffect returned error: %v", err)
	}
	if len(model.requests) != 1 || model.requests[0].Model != "mock-native" || len(model.requests[0].Messages) != 1 {
		t.Fatalf("llm requests = %#v, want one mock-native request", model.requests)
	}
	if len(result.Events) != 1 || result.Events[0].Type != statemachine.EventModelResponseReceived {
		t.Fatalf("events = %#v, want model.response_received", result.Events)
	}
	var payload statemachine.ModelCallPayload
	if err := json.Unmarshal(result.Events[0].Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	response, err := agents2.UnmarshalModelResponse(payload.Response)
	if err != nil {
		t.Fatalf("UnmarshalModelResponse returned error: %v", err)
	}
	if response.Content != `{"action":"complete","final_answer":"done"}` {
		t.Fatalf("response content = %q, want model content", response.Content)
	}
}

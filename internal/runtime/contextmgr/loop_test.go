package contextmgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"agent/internal/foundation/llmClient"
)

func TestManagerBuildRequestMergesAgentProfileMessagesToolsAndMetadata(t *testing.T) {
	rt, err := NewManager(Options{LLM: &recordingLLM{}})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}

	request, err := rt.BuildRequest(context.Background(), ModelCallInput{
		RunID: "run_1",
		Step:  3,
		Agent: AgentProfile{
			Name:        "default",
			Model:       "mock-native",
			Temperature: 0.4,
			Tools: []llmClient.ToolDefinition{{
				Name:        "ask_user",
				Description: "ask user",
				InputSchema: json.RawMessage(`{"type":"object"}`),
			}},
			Metadata: map[string]string{
				"purpose": "test",
				"run_id":  "overridden",
			},
		},
		Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("BuildRequest returned error: %v", err)
	}

	if request.Model != "mock-native" || request.Temperature != 0.4 {
		t.Fatalf("request model/temperature = %q/%v", request.Model, request.Temperature)
	}
	if len(request.Messages) != 1 || request.Messages[0].Content != "hello" {
		t.Fatalf("request messages = %#v", request.Messages)
	}
	if len(request.Tools) != 1 || request.Tools[0].Name != "ask_user" {
		t.Fatalf("request tools = %#v", request.Tools)
	}
	if request.Metadata["run_id"] != "run_1" || request.Metadata["step"] != "3" || request.Metadata["agent"] != "default" || request.Metadata["purpose"] != "test" {
		t.Fatalf("metadata = %#v", request.Metadata)
	}
}

func TestManagerCallModelReturnsNormalizedResponse(t *testing.T) {
	model := &recordingLLM{response: llmClient.Response{
		Provider: "mock",
		Model:    "mock-native",
		Content:  "need target",
		ToolCalls: []llmClient.ToolCall{{
			Name:  "ask_user",
			Input: json.RawMessage(`   `),
		}},
		Usage: &llmClient.Usage{InputTokens: 3, OutputTokens: 4, TotalTokens: 7},
	}}
	rt, err := NewManager(Options{LLM: model})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}

	result, err := rt.CallModel(context.Background(), ModelCallInput{
		RunID:    "run_1",
		Step:     2,
		Agent:    AgentProfile{Name: "default", Model: "mock-native"},
		Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: "build"}},
	})
	if err != nil {
		t.Fatalf("CallModel returned error: %v", err)
	}

	if len(model.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(model.requests))
	}
	if result.StartedAt.IsZero() || result.CompletedAt.IsZero() || result.CompletedAt.Before(result.StartedAt) {
		t.Fatalf("invalid timings: %#v", result)
	}
	if len(result.ToolCalls) != 1 || result.ToolCalls[0].ID != "call_2_1" || string(result.ToolCalls[0].Input) != "{}" {
		t.Fatalf("tool calls = %#v, want normalized call", result.ToolCalls)
	}
	if result.Response.ToolCalls[0].ID != "call_2_1" {
		t.Fatalf("response tool calls were not normalized: %#v", result.Response.ToolCalls)
	}
	if result.AssistantMessage == nil || result.AssistantMessage.Content != "need target" || len(result.AssistantMessage.ToolCalls) != 1 {
		t.Fatalf("assistant message = %#v", result.AssistantMessage)
	}
	if result.Usage == nil || result.Usage.TotalTokens != 7 {
		t.Fatalf("usage = %#v", result.Usage)
	}
}

func TestManagerCallModelReturnsErrorWithoutResponseMutation(t *testing.T) {
	modelErr := errors.New("model failed")
	rt, err := NewManager(Options{LLM: &recordingLLM{err: modelErr}})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}

	result, err := rt.CallModel(context.Background(), ModelCallInput{
		RunID:    "run_1",
		Step:     1,
		Agent:    AgentProfile{Name: "default", Model: "mock-native"},
		Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: "hello"}},
	})
	if err == nil || !strings.Contains(err.Error(), "model failed") {
		t.Fatalf("err = %v, want model failed", err)
	}
	if result.Request.Model != "mock-native" {
		t.Fatalf("partial result request = %#v, want built request", result.Request)
	}
	if result.AssistantMessage != nil || len(result.ToolCalls) != 0 {
		t.Fatalf("result = %#v, want no assistant/tool data on error", result)
	}
}

func TestManagerConcurrentCallsDoNotShareMutableState(t *testing.T) {
	model := &recordingLLM{respond: func(req llmClient.Request) llmClient.Response {
		return llmClient.Response{Provider: "mock", Model: req.Model, Content: req.Metadata["run_id"]}
	}}
	rt, err := NewManager(Options{LLM: model})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}

	const calls = 20
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for i := 0; i < calls; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			runID := fmt.Sprintf("run_%02d", i)
			result, err := rt.CallModel(context.Background(), ModelCallInput{
				RunID:    runID,
				Step:     i + 1,
				Agent:    AgentProfile{Name: "default", Model: "mock-native"},
				Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: runID}},
			})
			if err != nil {
				errs <- err
				return
			}
			if result.Response.Content != runID || result.Request.Metadata["run_id"] != runID {
				errs <- fmt.Errorf("result for %s was contaminated: %#v", runID, result)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(model.requests) != calls {
		t.Fatalf("requests = %d, want %d", len(model.requests), calls)
	}
}

type recordingLLM struct {
	mu       sync.Mutex
	requests []llmClient.Request
	response llmClient.Response
	respond  func(req llmClient.Request) llmClient.Response
	err      error
}

func (c *recordingLLM) Complete(_ context.Context, req llmClient.Request) (llmClient.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, cloneRequest(req))
	if c.err != nil {
		return llmClient.Response{}, c.err
	}
	if c.respond != nil {
		return c.respond(req), nil
	}
	return c.response, nil
}

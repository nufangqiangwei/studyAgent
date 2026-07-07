package runtimecli

import (
	"agent/internal/capability/command"
	"agent/internal/content"
	"agent/internal/foundation/llmClient"
	"agent/internal/taskpreprocess"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func TestRunStartsRuntimeTaskForPlainInput(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{{
			Provider: "mock",
			Model:    "mock-native",
			Content:  "runtime answer",
		}},
	}
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("hello runtime\n/exit\n"),
			Out: &out,
		},
		Config: content.Config{
			Provider: "mock",
			Model:    "mock-native",
			WorkDir:  "C:\\Code\\GO\\agent",
		},
	}

	err := Run(context.Background(), env, command.Manage, Options{
		LLM:    model,
		TaskID: "task_cli",
		Sync:   true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(model.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(model.requests))
	}
	if model.requests[0].Metadata["purpose"] != "task_preprocess" {
		t.Fatalf("first request metadata = %#v, want preprocessing request", model.requests[0].Metadata)
	}
	if !containsMessage(model.requests[1].Messages, llmClient.RoleUser, "hello runtime") {
		t.Fatalf("agent request messages = %#v, want user input", model.requests[1].Messages)
	}
	if !strings.Contains(out.String(), "runtime answer") {
		t.Fatalf("output missing runtime answer:\n%s", out.String())
	}
}

func TestRunSendsFollowupPlainInputAsUserEvent(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  `{"action":"ask_user","user_input":{"prompt":"Which package?"}}`,
			},
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  `{"action":"complete","final_answer":"Use internal/runtime"}`,
			},
		},
	}
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("inspect project\ninternal/runtime\n/exit\n"),
			Out: &out,
		},
		Config: content.Config{
			Provider: "mock",
			Model:    "mock-native",
			WorkDir:  "C:\\Code\\GO\\agent",
		},
	}

	err := Run(context.Background(), env, command.Manage, Options{
		LLM:    model,
		TaskID: "task_cli",
		Sync:   true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(model.requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(model.requests))
	}
	if model.requests[0].Metadata["purpose"] != "task_preprocess" {
		t.Fatalf("first request metadata = %#v, want preprocessing request", model.requests[0].Metadata)
	}
	if !containsMessage(model.requests[2].Messages, llmClient.RoleUser, "internal/runtime") {
		t.Fatalf("follow-up request messages = %#v, want follow-up user input", model.requests[2].Messages)
	}
	got := out.String()
	for _, want := range []string{"? Which package?", "Use internal/runtime"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunPreprocessesPlainInputAndAsksBeforeStartingTask(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{{
			Provider: "mock",
			Model:    "mock-native",
			Content:  "runtime answer",
		}},
	}
	preprocessor := &scriptedPreprocessor{
		results: []taskpreprocess.Result{
			{
				Action: taskpreprocess.ActionAskUser,
				Questions: []taskpreprocess.Question{{
					ID:     "q1",
					Prompt: "Which package should be changed?",
				}},
				MissingInformation: []string{"target package"},
			},
			{
				Action:         taskpreprocess.ActionProceed,
				OriginalInput:  "fix the parser",
				NormalizedTask: "Fix the parser in internal/runtime.",
			},
		},
	}
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("fix the parser\ninternal/runtime\n/exit\n"),
			Out: &out,
		},
		Config: content.Config{
			Provider: "mock",
			Model:    "mock-native",
			WorkDir:  "C:\\Code\\GO\\agent",
		},
	}

	err := Run(context.Background(), env, command.Manage, Options{
		LLM:          model,
		TaskID:       "task_cli",
		Sync:         true,
		Preprocessor: preprocessor,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(preprocessor.requests) != 2 {
		t.Fatalf("preprocess requests = %d, want 2", len(preprocessor.requests))
	}
	if len(preprocessor.requests[0].Clarifications) != 0 {
		t.Fatalf("first preprocess clarifications = %#v, want none", preprocessor.requests[0].Clarifications)
	}
	if got := preprocessor.requests[1].Clarifications; len(got) != 1 || got[0].Answer != "internal/runtime" {
		t.Fatalf("second preprocess clarifications = %#v, want answer", got)
	}
	if len(model.requests) != 1 {
		t.Fatalf("model requests = %d, want task to start only after clarification", len(model.requests))
	}
	if !containsMessage(model.requests[0].Messages, llmClient.RoleUser, "Fix the parser in internal/runtime.") {
		t.Fatalf("agent request messages = %#v, want normalized task", model.requests[0].Messages)
	}
	got := out.String()
	for _, want := range []string{"? Which package should be changed?", "runtime answer"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func containsMessage(messages []llmClient.Message, role llmClient.Role, content string) bool {
	for _, message := range messages {
		if message.Role == role && strings.Contains(message.Content, content) {
			return true
		}
	}
	return false
}

type scriptedLLM struct {
	mu        sync.Mutex
	requests  []llmClient.Request
	responses []llmClient.Response
}

func (m *scriptedLLM) Complete(_ context.Context, req llmClient.Request) (llmClient.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, cloneRequest(req))
	if req.Metadata["purpose"] == "task_preprocess" {
		raw, err := json.Marshal(map[string]string{
			"action":          "proceed",
			"normalized_task": preprocessOriginalInput(req),
		})
		if err != nil {
			return llmClient.Response{}, err
		}
		return llmClient.Response{
			Provider: "mock",
			Model:    req.Model,
			Content:  string(raw),
		}, nil
	}
	if len(m.responses) == 0 {
		return llmClient.Response{Provider: "mock", Model: req.Model}, nil
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return response, nil
}

type scriptedPreprocessor struct {
	requests []taskpreprocess.Request
	results  []taskpreprocess.Result
}

func (p *scriptedPreprocessor) Preprocess(_ context.Context, request taskpreprocess.Request) (taskpreprocess.Result, error) {
	p.requests = append(p.requests, request)
	if len(p.results) == 0 {
		return taskpreprocess.Result{
			Action:         taskpreprocess.ActionProceed,
			OriginalInput:  request.Input,
			NormalizedTask: request.Input,
		}, nil
	}
	result := p.results[0]
	p.results = p.results[1:]
	return result, nil
}

func cloneRequest(request llmClient.Request) llmClient.Request {
	cloned := request
	if len(request.Messages) > 0 {
		cloned.Messages = make([]llmClient.Message, 0, len(request.Messages))
		for _, message := range request.Messages {
			cloned.Messages = append(cloned.Messages, message)
		}
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

func preprocessOriginalInput(request llmClient.Request) string {
	for i := len(request.Messages) - 1; i >= 0; i-- {
		if request.Messages[i].Role != llmClient.RoleUser {
			continue
		}
		var payload struct {
			OriginalInput string `json:"original_input"`
		}
		if err := json.Unmarshal([]byte(request.Messages[i].Content), &payload); err == nil && payload.OriginalInput != "" {
			return payload.OriginalInput
		}
		return request.Messages[i].Content
	}
	return ""
}

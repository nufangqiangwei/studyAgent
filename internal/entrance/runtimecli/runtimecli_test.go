package runtimecli

import (
	"agent/internal/capability/command"
	"agent/internal/content"
	"agent/internal/foundation/llmClient"
	"context"
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
	if len(model.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(model.requests))
	}
	if !containsMessage(model.requests[0].Messages, llmClient.RoleUser, "hello runtime") {
		t.Fatalf("agent request messages = %#v, want user input", model.requests[0].Messages)
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
	if len(model.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(model.requests))
	}
	if !containsMessage(model.requests[1].Messages, llmClient.RoleUser, "internal/runtime") {
		t.Fatalf("follow-up request messages = %#v, want follow-up user input", model.requests[1].Messages)
	}
	got := out.String()
	for _, want := range []string{"? Which package?", "Use internal/runtime"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunStartsPlainInputWithoutPreprocessing(t *testing.T) {
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
			In:  strings.NewReader("fix the parser\n/exit\n"),
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
	if len(model.requests) != 1 {
		t.Fatalf("model requests = %d, want direct task start", len(model.requests))
	}
	if !containsMessage(model.requests[0].Messages, llmClient.RoleUser, "fix the parser") {
		t.Fatalf("agent request messages = %#v, want original input", model.requests[0].Messages)
	}
	if !strings.Contains(out.String(), "runtime answer") {
		t.Fatalf("output missing runtime answer:\n%s", out.String())
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
	if len(m.responses) == 0 {
		return llmClient.Response{Provider: "mock", Model: req.Model}, nil
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return response, nil
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

package provider

import (
	"agent/internal/foundation/llmClient"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewSupportsDeepSeekOpenAICompatibleRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("path = %q, want /chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("Authorization = %q", got)
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["model"] != "deepseek-chat" {
			t.Fatalf("model = %v, want deepseek-chat", body["model"])
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "model": "deepseek-chat",
  "usage": {"prompt_tokens": 9, "completion_tokens": 4, "total_tokens": 13},
  "choices": [
    {
      "finish_reason": "stop",
      "message": {"content": "deepseek chat response"}
    }
  ]
}`))
	}))
	defer server.Close()

	client, err := New(Options{
		Model:    "deepseek-chat",
		ModelURL: server.URL,
		APIKey:   "secret-token",
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	resp, err := client.Complete(context.Background(), llmClient.Request{
		Messages: []llmClient.Message{
			{Role: llmClient.RoleSystem, Content: "system prompt"},
			{Role: llmClient.RoleUser, Content: "hello"},
		},
	})
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if resp.Provider != "deepseek" {
		t.Fatalf("Provider = %q, want deepseek", resp.Provider)
	}
	if resp.Model != "deepseek-chat" {
		t.Fatalf("Model = %q, want deepseek-chat", resp.Model)
	}
	if resp.Content != "deepseek chat response" {
		t.Fatalf("Content = %q", resp.Content)
	}
	if resp.Usage == nil || resp.Usage.InputTokens != 9 || resp.Usage.OutputTokens != 4 || resp.Usage.TotalTokens != 13 {
		t.Fatalf("Usage = %#v", resp.Usage)
	}
	if strings.Contains(string(resp.Raw), "secret-token") {
		t.Fatalf("response leaked api key:\n%s", string(resp.Raw))
	}
}

func TestCompleteWritesDebugJSONL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "model": "gpt-test",
  "choices": [{"finish_reason": "stop", "message": {"content": "debug response"}}]
}`))
	}))
	defer server.Close()

	debugPath := filepath.Join(t.TempDir(), "llm.jsonl")
	recorder, err := NewJSONLDebugRecorder(debugPath)
	if err != nil {
		t.Fatalf("NewJSONLDebugRecorder returned error: %v", err)
	}
	client, err := New(Options{
		Model:         "gpt-test",
		ModelURL:      server.URL,
		APIKey:        "secret-token",
		DebugRecorder: recorder,
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	_, err = client.Complete(context.Background(), llmClient.Request{
		Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: "hello debug"}},
	})
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	data, err := os.ReadFile(debugPath)
	if err != nil {
		t.Fatalf("read debug file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("debug lines = %d, want 1:\n%s", len(lines), string(data))
	}

	var entry struct {
		Kind        string `json:"kind"`
		Provider    string `json:"provider"`
		Model       string `json:"model"`
		StatusCode  int    `json:"status_code"`
		RequestBody struct {
			Model    string `json:"model"`
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		} `json:"request_body"`
		ResponseBody struct {
			Model   string `json:"model"`
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		} `json:"response_body"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("parse debug jsonl: %v\n%s", err, lines[0])
	}
	if entry.Kind != "llm_http" || entry.Provider != "openai" || entry.Model != "gpt-test" || entry.StatusCode != http.StatusOK {
		t.Fatalf("entry metadata = %#v", entry)
	}
	if entry.RequestBody.Model != "gpt-test" || entry.RequestBody.Messages[0].Content != "hello debug" {
		t.Fatalf("request body = %#v", entry.RequestBody)
	}
	if entry.ResponseBody.Model != "gpt-test" || entry.ResponseBody.Choices[0].Message.Content != "debug response" {
		t.Fatalf("response body = %#v", entry.ResponseBody)
	}
}

func TestNewSupportsDeepSeekAnthropicCompatibleRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/anthropic/v1/messages" {
			t.Fatalf("path = %q, want /anthropic/v1/messages", r.URL.Path)
		}
		if got := r.Header.Get("x-api-key"); got != "secret-token" {
			t.Fatalf("x-api-key = %q", got)
		}
		if got := r.Header.Get("anthropic-version"); got != anthropicVersion {
			t.Fatalf("anthropic-version = %q", got)
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["system"] != "system prompt" {
			t.Fatalf("system = %v", body["system"])
		}
		if body["max_tokens"] == nil {
			t.Fatal("body missing max_tokens")
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "model": "deepseek-v4-pro",
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 7, "output_tokens": 5, "cache_read_input_tokens": 2},
  "content": [{"type": "text", "text": "deepseek anthropic response"}]
}`))
	}))
	defer server.Close()

	client, err := New(Options{
		Model:    "deepseek-v4-pro",
		ModelURL: server.URL,
		APIKey:   "secret-token",
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	resp, err := client.Complete(context.Background(), llmClient.Request{
		Messages: []llmClient.Message{
			{Role: llmClient.RoleSystem, Content: "system prompt"},
			{Role: llmClient.RoleUser, Content: "hello"},
		},
	})
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if resp.Provider != "deepseek" {
		t.Fatalf("Provider = %q, want deepseek", resp.Provider)
	}
	if resp.Content != "deepseek anthropic response" {
		t.Fatalf("Content = %q", resp.Content)
	}
	if resp.Usage == nil || resp.Usage.InputTokens != 7 || resp.Usage.OutputTokens != 5 || resp.Usage.CacheReadInputTokens != 2 || resp.Usage.TotalTokens != 14 {
		t.Fatalf("Usage = %#v", resp.Usage)
	}
}

func TestNewSupportsGeminiRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1beta/models/gemini-test:generateContent" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("x-goog-api-key"); got != "secret-token" {
			t.Fatalf("x-goog-api-key = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "candidates": [
    {
      "finishReason": "STOP",
      "content": {"parts": [{"text": "gemini response"}]}
    }
  ],
  "usageMetadata": {"promptTokenCount": 6, "candidatesTokenCount": 3, "totalTokenCount": 9}
}`))
	}))
	defer server.Close()

	client, err := New(Options{
		Model:    "gemini-test",
		ModelURL: server.URL + "/v1beta/models/{model}:generateContent",
		APIKey:   "secret-token",
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	resp, err := client.Complete(context.Background(), llmClient.Request{
		Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if resp.Provider != "gemini" {
		t.Fatalf("Provider = %q, want gemini", resp.Provider)
	}
	if resp.Content != "gemini response" {
		t.Fatalf("Content = %q", resp.Content)
	}
	if resp.Usage == nil || resp.Usage.InputTokens != 6 || resp.Usage.OutputTokens != 3 || resp.Usage.TotalTokens != 9 {
		t.Fatalf("Usage = %#v", resp.Usage)
	}
}

func TestNewSupportsAnthropicRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Fatalf("path = %q, want /v1/messages", r.URL.Path)
		}
		if got := r.Header.Get("x-api-key"); got != "secret-token" {
			t.Fatalf("x-api-key = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "model": "claude-test",
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 8, "output_tokens": 6},
  "content": [{"type": "text", "text": "anthropic response"}]
}`))
	}))
	defer server.Close()

	client, err := New(Options{
		Model:    "claude-test",
		ModelURL: server.URL,
		APIKey:   "secret-token",
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	resp, err := client.Complete(context.Background(), llmClient.Request{
		Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if resp.Provider != "anthropic" {
		t.Fatalf("Provider = %q, want anthropic", resp.Provider)
	}
	if resp.Content != "anthropic response" {
		t.Fatalf("Content = %q", resp.Content)
	}
	if resp.Usage == nil || resp.Usage.InputTokens != 8 || resp.Usage.OutputTokens != 6 || resp.Usage.TotalTokens != 14 {
		t.Fatalf("Usage = %#v", resp.Usage)
	}
}

func TestCompleteRequiresAPIKeyForNonMockProviders(t *testing.T) {
	client, err := New(Options{Model: "deepseek-chat"})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	_, err = client.Complete(context.Background(), llmClient.Request{
		Messages: []llmClient.Message{{Role: llmClient.RoleUser, Content: "hello"}},
	})
	if err == nil {
		t.Fatal("Complete returned nil error")
	}
	if !strings.Contains(err.Error(), "api_key is required") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestNewClientReportsModelName(t *testing.T) {
	tests := []struct {
		name  string
		model string
		want  string
	}{
		{name: "default mock", model: "", want: "mock-native"},
		{name: "mock", model: "mock-test", want: "mock-test"},
		{name: "openai", model: "gpt-4.1", want: "gpt-4.1"},
		{name: "deepseek openai compatible", model: "deepseek-chat", want: "deepseek-chat"},
		{name: "deepseek anthropic compatible", model: "deepseek-v4-pro", want: "deepseek-v4-pro"},
		{name: "gemini", model: "gemini-2.5-pro", want: "gemini-2.5-pro"},
		{name: "anthropic", model: "claude-sonnet-4", want: "claude-sonnet-4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := New(Options{Model: tt.model})
			if err != nil {
				t.Fatalf("New returned error: %v", err)
			}
			if got := client.ModelName(); got != tt.want {
				t.Fatalf("ModelName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNameForModel(t *testing.T) {
	tests := []struct {
		model string
		want  string
	}{
		{model: "", want: "mock"},
		{model: "mock-native", want: "mock"},
		{model: "deepseek-chat", want: "deepseek"},
		{model: "deepseek-v4-pro", want: "deepseek"},
		{model: "gemini-2.5-pro", want: "gemini"},
		{model: "claude-sonnet-4", want: "anthropic"},
		{model: "gpt-4.1", want: "openai"},
		{model: "o4-mini", want: "openai"},
	}

	for _, tt := range tests {
		got, err := NameForModel(tt.model)
		if err != nil {
			t.Fatalf("NameForModel(%q) returned error: %v", tt.model, err)
		}
		if got != tt.want {
			t.Fatalf("NameForModel(%q) = %q, want %q", tt.model, got, tt.want)
		}
	}
}

func TestNameForModelRejectsUnknownModel(t *testing.T) {
	_, err := NameForModel("custom-model")
	if err == nil {
		t.Fatal("NameForModel returned nil error")
	}
	if !strings.Contains(err.Error(), "custom-model") {
		t.Fatalf("error = %q, want model name", err.Error())
	}
}

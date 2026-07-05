package contextmgr

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"agent/internal/foundation/llmClient"
)

func TestContextWindowTokensUsesModelFamiliesAndFallback(t *testing.T) {
	resetContextWindowTokenCacheForTest(t)

	if got := contextWindowTokens("gpt-4o"); got != 128_000 {
		t.Fatalf("gpt window = %d, want 128000", got)
	}
	if got := contextWindowTokens("claude-3-5-sonnet"); got != 200_000 {
		t.Fatalf("claude window = %d, want 200000", got)
	}
	if got := contextWindowTokens("unknown-model"); got != defaultContextWindowTokens {
		t.Fatalf("unknown window = %d, want default %d", got, defaultContextWindowTokens)
	}
}

func TestResolveAndCacheContextWindowTokensUsesGeminiMetadata(t *testing.T) {
	resetContextWindowTokenCacheForTest(t)

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/models/gemini-test" {
			t.Fatalf("path = %q, want /models/gemini-test", r.URL.Path)
		}
		if got := r.Header.Get("x-goog-api-key"); got != "secret-token" {
			t.Fatalf("x-goog-api-key = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"inputTokenLimit":1048576}`))
	}))
	defer server.Close()

	result, err := ResolveAndCacheContextWindowTokens(context.Background(), ContextWindowLookupOptions{
		Provider:    "gemini",
		Model:       "gemini-test",
		APIKey:      "secret-token",
		MetadataURL: server.URL + "/models/{model}",
	})
	if err != nil {
		t.Fatalf("ResolveAndCacheContextWindowTokens returned error: %v", err)
	}
	if result.Tokens != 1_048_576 || result.Source != "gemini" {
		t.Fatalf("result = %#v, want gemini metadata tokens", result)
	}
	if got := contextWindowTokens("gemini-test"); got != 1_048_576 {
		t.Fatalf("cached window = %d, want 1048576", got)
	}

	result, err = ResolveAndCacheContextWindowTokens(context.Background(), ContextWindowLookupOptions{
		Provider:    "gemini",
		Model:       "gemini-test",
		MetadataURL: server.URL + "/models/{model}",
	})
	if err != nil {
		t.Fatalf("second ResolveAndCacheContextWindowTokens returned error: %v", err)
	}
	if result.Source != "cache" {
		t.Fatalf("second result source = %q, want cache", result.Source)
	}
	if requests != 1 {
		t.Fatalf("metadata requests = %d, want 1", requests)
	}
}

func TestResolveAndCacheContextWindowTokensUsesOpenRouterMetadata(t *testing.T) {
	resetContextWindowTokenCacheForTest(t)

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "data": [
    {"id": "anthropic/claude-test", "context_length": 200000},
    {"id": "openai/gpt-test", "canonical_slug": "openai/gpt-test", "context_length": 256000}
  ]
}`))
	}))
	defer server.Close()

	result, err := ResolveAndCacheContextWindowTokens(context.Background(), ContextWindowLookupOptions{
		Provider:    "openai",
		Model:       "gpt-test",
		ModelURL:    "https://openrouter.ai/api/v1",
		APIKey:      "secret-token",
		MetadataURL: server.URL,
	})
	if err != nil {
		t.Fatalf("ResolveAndCacheContextWindowTokens returned error: %v", err)
	}
	if result.Tokens != 256_000 || result.Source != "openrouter" {
		t.Fatalf("result = %#v, want OpenRouter metadata tokens", result)
	}
	if got := contextWindowTokens("gpt-test"); got != 256_000 {
		t.Fatalf("cached window = %d, want 256000", got)
	}
	if requests != 1 {
		t.Fatalf("metadata requests = %d, want 1", requests)
	}
}

func TestResolveAndCacheContextWindowTokensUnsupportedProviderUsesFallbackWithoutRequest(t *testing.T) {
	resetContextWindowTokenCacheForTest(t)

	result, err := ResolveAndCacheContextWindowTokens(context.Background(), ContextWindowLookupOptions{
		Provider:   "openai",
		Model:      "gpt-4o",
		HTTPClient: failingHTTPClient{t: t},
	})
	if err != nil {
		t.Fatalf("ResolveAndCacheContextWindowTokens returned error: %v", err)
	}
	if result.Tokens != 128_000 || result.Source != "fallback" {
		t.Fatalf("result = %#v, want fallback OpenAI tokens", result)
	}
	if got := contextWindowTokens("gpt-4o"); got != 128_000 {
		t.Fatalf("cached window = %d, want 128000", got)
	}
}

func TestResolveAndCacheContextWindowTokensCachesFallbackAfterFailure(t *testing.T) {
	resetContextWindowTokenCacheForTest(t)

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	result, err := ResolveAndCacheContextWindowTokens(context.Background(), ContextWindowLookupOptions{
		Provider:    "gemini",
		Model:       "gemini-test",
		MetadataURL: server.URL,
	})
	if err == nil {
		t.Fatal("ResolveAndCacheContextWindowTokens returned nil error, want metadata failure")
	}
	if result.Tokens != 1_000_000 || result.Source != "fallback" {
		t.Fatalf("result = %#v, want Gemini fallback tokens", result)
	}
	if got := contextWindowTokens("gemini-test"); got != 1_000_000 {
		t.Fatalf("cached fallback window = %d, want 1000000", got)
	}

	result, err = ResolveAndCacheContextWindowTokens(context.Background(), ContextWindowLookupOptions{
		Provider:    "gemini",
		Model:       "gemini-test",
		MetadataURL: server.URL,
	})
	if err != nil {
		t.Fatalf("second ResolveAndCacheContextWindowTokens returned error: %v", err)
	}
	if result.Source != "cache" {
		t.Fatalf("second result source = %q, want cache", result.Source)
	}
	if requests != 1 {
		t.Fatalf("metadata requests = %d, want 1", requests)
	}
}

func TestContextWindowTokensUsesCacheBeforeFallback(t *testing.T) {
	resetContextWindowTokenCacheForTest(t)

	cacheContextWindowTokens("gpt-4o", 4_096)
	if got := contextWindowTokens("gpt-4o"); got != 4_096 {
		t.Fatalf("cached window = %d, want 4096", got)
	}
}

func TestContextCompressionDecisionUsesUsageBeforeEstimate(t *testing.T) {
	resetContextWindowTokenCacheForTest(t)

	decision := contextCompressionDecision(llmClient.Request{
		Model: "mock-native",
		Messages: []llmClient.Message{
			{Role: llmClient.RoleSystem, Content: "system"},
			{Role: llmClient.RoleUser, Content: "small prompt"},
		},
	}, &llmClient.Usage{InputTokens: 16_000}, EstimatedContextTokenCounter{})

	if !decision.ShouldCompress {
		t.Fatalf("ShouldCompress = false, want true: %#v", decision)
	}
	if decision.TriggerTokens != 16_000 || decision.UsageInputTokens != 16_000 {
		t.Fatalf("decision = %#v, want usage-driven trigger", decision)
	}
}

func TestContextCompressionDecisionFallsBackToEstimate(t *testing.T) {
	resetContextWindowTokenCacheForTest(t)

	decision := contextCompressionDecision(llmClient.Request{
		Model: "mock-native",
		Messages: []llmClient.Message{
			{Role: llmClient.RoleSystem, Content: "system"},
			{Role: llmClient.RoleUser, Content: strings.Repeat("a", 64_000)},
		},
	}, nil, EstimatedContextTokenCounter{})

	if !decision.ShouldCompress {
		t.Fatalf("ShouldCompress = false, want true from estimate: %#v", decision)
	}
	if decision.EstimatedTokens < decision.ThresholdTokens {
		t.Fatalf("estimated tokens = %d, threshold = %d", decision.EstimatedTokens, decision.ThresholdTokens)
	}
}

type failingHTTPClient struct {
	t *testing.T
}

func (c failingHTTPClient) Do(*http.Request) (*http.Response, error) {
	c.t.Helper()
	c.t.Fatal("metadata HTTP request was not expected")
	return nil, fmt.Errorf("unexpected request")
}

func resetContextWindowTokenCacheForTest(t *testing.T) {
	t.Helper()
	resetContextWindowTokenCache()
	t.Cleanup(resetContextWindowTokenCache)
}

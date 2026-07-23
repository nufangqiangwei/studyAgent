package main

import (
	"agent/services/interaction"
	"agent/services/llmClient"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeModelClient struct {
	mu       sync.Mutex
	requests []llmClient.ClientRequest
}

func (c *fakeModelClient) Complete(_ context.Context, request llmClient.ClientRequest, _ string) (llmClient.Completion, error) {
	c.mu.Lock()
	c.requests = append(c.requests, request)
	c.mu.Unlock()
	return llmClient.Completion{Content: `{"action":"finish","answer":"hello from the fake model"}`}, nil
}

func (c *fakeModelClient) snapshot() []llmClient.ClientRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]llmClient.ClientRequest(nil), c.requests...)
}

func TestRunCompletesOneInteractiveRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	model := &fakeModelClient{}
	var output bytes.Buffer
	var stderr bytes.Buffer
	err := runWithOptions(ctx, []string{
		"-provider", "openai", "-model", "fake-model", "-base-url", "https://model.example/v1",
		"-data-dir", t.TempDir(),
	}, strings.NewReader("say hello\n/exit\n"), &output, &stderr, runOptions{
		modelClient: model,
		getenv:      func(string) string { return "" },
	})
	if err != nil {
		t.Fatalf("run CLI: %v\nstderr=%s\nstdout=%s", err, stderr.String(), output.String())
	}
	if !strings.Contains(output.String(), "assistant> hello from the fake model") {
		t.Fatalf("stdout=%s", output.String())
	}
	requests := model.snapshot()
	if len(requests) != 1 {
		t.Fatalf("model requests=%d", len(requests))
	}
	prompt := ""
	for _, message := range requests[0].Messages {
		prompt += message.Content
	}
	if !strings.Contains(prompt, "say hello") {
		t.Fatalf("model prompt did not contain CLI input: %s", prompt)
	}
}

func TestHelpDoesNotRequireModelConfiguration(t *testing.T) {
	var output bytes.Buffer
	var stderr bytes.Buffer
	err := runWithOptions(context.Background(), []string{"-help"}, strings.NewReader(""), &output, &stderr, runOptions{
		getenv: func(string) string { return "" },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "Usage: go run ./main/cli") {
		t.Fatalf("help=%s", stderr.String())
	}
}

func TestDeepSeekOpenAICompatibleDefaults(t *testing.T) {
	config, err := parseConfig([]string{"-model", "deepseek-v4-pro"}, &bytes.Buffer{}, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if config.Provider != llmClient.ProviderDeepSeek || config.BaseURL != "https://api.deepseek.com" || config.Model != "deepseek-v4-pro" {
		t.Fatalf("config=%#v", config)
	}
}

func TestPlanRevisionChangesWithPersistedAgentConfiguration(t *testing.T) {
	base := cliConfig{SystemPrompt: "one", MaxTurns: 8, MaxTokens: 100}
	changed := base
	changed.SystemPrompt = "two"
	if planRevision(base) == planRevision(changed) {
		t.Fatal("plan revision did not change with Agent configuration")
	}
	if planRevision(base) != planRevision(base) {
		t.Fatal("plan revision is not deterministic")
	}
}

func TestTerminalPresenterCachesOnlyFivePersistedPresentations(t *testing.T) {
	presenter := newTerminalPresenter(io.Discard)
	for index := 0; index < presenterHistoryLimit+2; index++ {
		requestID := fmt.Sprintf("request-%d", index)
		if err := presenter.Present(context.Background(), interaction.Presentation{
			ID: "presentation-" + requestID, Kind: interaction.PresentationError,
			RequestID: requestID, ErrorCode: "test", ErrorMessage: "test error",
		}); err != nil {
			t.Fatal(err)
		}
	}
	presenter.mu.Lock()
	defer presenter.mu.Unlock()
	if len(presenter.seen) != presenterHistoryLimit || len(presenter.completed) != presenterHistoryLimit {
		t.Fatalf("seen=%d completed=%d", len(presenter.seen), len(presenter.completed))
	}
	if _, found := presenter.seen["presentation-request-0"]; found {
		t.Fatal("oldest presentation remained in the cache")
	}
	if _, found := presenter.completed["request-0"]; found {
		t.Fatal("oldest completion remained in the cache")
	}
}

package provider

import (
	"agent/internal/foundation/llmClient"
	"agent/internal/foundation/llmClient/anthropic"
	"agent/internal/foundation/llmClient/deepseek"
	"agent/internal/foundation/llmClient/gemini"
	"agent/internal/foundation/llmClient/mock"
	"agent/internal/foundation/llmClient/openai"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Options struct {
	Model         string
	ModelURL      string
	APIKey        string
	HTTPClient    httpDoer
	DebugRecorder BodyDebugRecorder
}

type requestBuilder interface {
	Build(req llmClient.Request) ([]byte, error)
}

type responseParser interface {
	Parse(req llmClient.Request, body []byte) (llmClient.Response, error)
}

type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

func New(opts Options) (llmClient.Client, error) {
	name, err := NameForModel(opts.Model)
	if err != nil {
		return nil, err
	}
	switch name {
	case "mock":
		return mock.New(opts.Model), nil
	case "openai":
		return newHTTPClient(httpClientOptions{
			provider: "openai",
			model:    opts.Model,
			endpoint: chatEndpoint(opts.ModelURL, "https://api.openai.com/v1/chat/completions", "v1/chat/completions"),
			apiKey:   opts.APIKey,
			builder:  openai.Builder{},
			parser:   openAIParser{},
			auth:     authBearer,
			client:   opts.HTTPClient,
			debug:    opts.DebugRecorder,
		}), nil
	case "deepseek":
		return newDeepSeekClient(opts), nil
	case "gemini":
		return newHTTPClient(httpClientOptions{
			provider: "gemini",
			model:    opts.Model,
			endpoint: geminiEndpoint(opts.ModelURL, opts.Model),
			apiKey:   opts.APIKey,
			builder:  gemini.Builder{},
			parser:   geminiParser{},
			auth:     authGoogleAPIKey,
			client:   opts.HTTPClient,
			debug:    opts.DebugRecorder,
		}), nil
	case "anthropic":
		return newHTTPClient(httpClientOptions{
			provider: "anthropic",
			model:    opts.Model,
			endpoint: messagesEndpoint(opts.ModelURL, "https://api.anthropic.com/v1/messages"),
			apiKey:   opts.APIKey,
			builder:  anthropic.Builder{},
			parser:   anthropicParser{provider: "anthropic"},
			auth:     authAnthropic,
			client:   opts.HTTPClient,
			debug:    opts.DebugRecorder,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported llm provider %q for model %q", name, opts.Model)
	}
}

func newDeepSeekClient(opts Options) llmClient.Client {
	if isDeepSeekAnthropicCompatibleModel(opts.Model) {
		return newHTTPClient(httpClientOptions{
			provider: "deepseek",
			model:    opts.Model,
			endpoint: deepSeekAnthropicEndpoint(opts.ModelURL),
			apiKey:   opts.APIKey,
			builder:  anthropic.Builder{},
			parser:   anthropicParser{provider: "deepseek"},
			auth:     authAnthropic,
			client:   opts.HTTPClient,
			debug:    opts.DebugRecorder,
		})
	}

	return newHTTPClient(httpClientOptions{
		provider: "deepseek",
		model:    opts.Model,
		endpoint: chatEndpoint(opts.ModelURL, "https://api.deepseek.com/chat/completions", "chat/completions"),
		apiKey:   opts.APIKey,
		builder:  deepseek.Builder{},
		parser:   openAIParser{provider: "deepseek"},
		auth:     authBearer,
		client:   opts.HTTPClient,
		debug:    opts.DebugRecorder,
	})
}

func NameForModel(model string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(model))
	switch {
	case name == "", strings.HasPrefix(name, "mock"):
		return "mock", nil
	case strings.HasPrefix(name, "deepseek"):
		return "deepseek", nil
	case strings.HasPrefix(name, "gemini"):
		return "gemini", nil
	case strings.HasPrefix(name, "claude"):
		return "anthropic", nil
	case strings.HasPrefix(name, "gpt-"),
		strings.HasPrefix(name, "o1"),
		strings.HasPrefix(name, "o3"),
		strings.HasPrefix(name, "o4"),
		strings.HasPrefix(name, "chatgpt-"):
		return "openai", nil
	default:
		return "", fmt.Errorf("unknown llm provider for model %q", model)
	}
}

func isDeepSeekAnthropicCompatibleModel(model string) bool {
	name := strings.ToLower(strings.TrimSpace(model))
	return strings.HasPrefix(name, "deepseek-v4")
}

func defaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Minute}
}

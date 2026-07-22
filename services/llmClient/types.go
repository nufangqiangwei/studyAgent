package llmClient

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	ProviderOpenAI           = "openai"
	ProviderOpenAICompatible = "openai-compatible"
	ProviderDeepSeek         = "deepseek"
	ProviderAnthropic        = "anthropic"
	ProviderGemini           = "gemini"
)

// Config is process-local model configuration. In particular, APIKey is never
// copied into a Runtime manifest, message, journal event, snapshot, or effect.
type Config struct {
	BaseURL    string
	APIKey     string
	Provider   string
	ModelName  string
	Timeout    time.Duration
	HTTPClient *http.Client

	// MaxInputArtifactBytes bounds materialization of an optional prompt
	// Artifact. Zero uses 16 MiB.
	MaxInputArtifactBytes int64
}

func (c Config) validate() error {
	if strings.TrimSpace(c.BaseURL) == "" || strings.TrimSpace(c.Provider) == "" || strings.TrimSpace(c.ModelName) == "" {
		return fmt.Errorf("llm client base URL, provider, and model name are required")
	}
	parsed, err := url.Parse(c.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("llm client base URL must be an absolute URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("llm client base URL must use http or https")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("llm client base URL cannot contain credentials, query parameters, or a fragment")
	}
	return nil
}

func (c Config) withDefaults() Config {
	c.BaseURL = strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	c.Provider = strings.ToLower(strings.TrimSpace(c.Provider))
	c.ModelName = strings.TrimSpace(c.ModelName)
	if c.Timeout <= 0 {
		c.Timeout = 2 * time.Minute
	}
	if c.MaxInputArtifactBytes <= 0 {
		c.MaxInputArtifactBytes = 16 << 20
	}
	return c
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// CompletionRequest is the public model.complete v1 payload. Prompt and
// Messages may be used together; Prompt is appended as the final user message.
// Large prompt content should be supplied through InputArtifact.
type CompletionRequest struct {
	RequestID     string                `json:"request_id,omitempty"`
	System        string                `json:"system,omitempty"`
	Prompt        string                `json:"prompt,omitempty"`
	Messages      []ChatMessage         `json:"messages,omitempty"`
	InputArtifact *contract.ArtifactRef `json:"input_artifact,omitempty"`
	Temperature   *float64              `json:"temperature,omitempty"`
	MaxTokens     int                   `json:"max_tokens,omitempty"`
}

func (r CompletionRequest) validate() error {
	if strings.TrimSpace(r.Prompt) == "" && len(r.Messages) == 0 && r.InputArtifact == nil {
		return fmt.Errorf("completion request requires prompt, messages, or input artifact")
	}
	for index, message := range r.Messages {
		switch strings.ToLower(strings.TrimSpace(message.Role)) {
		case "system", "user", "assistant", "tool":
		default:
			return fmt.Errorf("completion message %d has unsupported role %q", index, message.Role)
		}
		if message.Content == "" {
			return fmt.Errorf("completion message %d content is required", index)
		}
	}
	if r.InputArtifact != nil {
		if err := artifact.ValidateRef(*r.InputArtifact); err != nil {
			return fmt.Errorf("validate completion input artifact: %w", err)
		}
	}
	if r.Temperature != nil && (*r.Temperature < 0 || *r.Temperature > 2) {
		return fmt.Errorf("completion temperature must be between 0 and 2")
	}
	if r.MaxTokens < 0 {
		return fmt.Errorf("completion max_tokens cannot be negative")
	}
	return nil
}

func (r CompletionRequest) clone() CompletionRequest {
	r.Messages = append([]ChatMessage(nil), r.Messages...)
	if r.InputArtifact != nil {
		ref := *r.InputArtifact
		r.InputArtifact = &ref
	}
	if r.Temperature != nil {
		value := *r.Temperature
		r.Temperature = &value
	}
	return r
}

// CompletionReply is delivered to the requesting Service as model.completed.
// ArtifactKey is included explicitly for callers that only need the key; the
// complete immutable reference is also returned for safe reads.
type CompletionReply struct {
	RequestID   string               `json:"request_id"`
	ArtifactKey string               `json:"artifact_key"`
	Artifact    contract.ArtifactRef `json:"artifact"`
	Provider    string               `json:"provider"`
	ModelName   string               `json:"model_name"`
}

// ClientRequest is the normalized request passed to a provider adapter.
type ClientRequest struct {
	Provider    string
	ModelName   string
	System      string
	Messages    []ChatMessage
	Temperature *float64
	MaxTokens   int
}

type Completion struct {
	Content string
}

// Client allows provider adapters and deterministic test doubles to be
// installed without giving the Service direct access to a network client.
type Client interface {
	Complete(ctx context.Context, request ClientRequest, idempotencyKey string) (Completion, error)
}

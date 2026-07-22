package llmClient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maxProviderResponseBytes = 32 << 20

// ProviderHTTPError does not include response bodies or credentials so it is
// safe to persist as an Effect failure.
type ProviderHTTPError struct {
	Provider   string
	StatusCode int
}

func (e *ProviderHTTPError) Error() string {
	return fmt.Sprintf("%s provider returned HTTP status %d", e.Provider, e.StatusCode)
}

func (e *ProviderHTTPError) Retryable() bool {
	return e.StatusCode == http.StatusRequestTimeout || e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

type httpCompletionClient struct {
	config Config
	client *http.Client
}

func newHTTPCompletionClient(config Config) Client {
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: config.Timeout}
	}
	return &httpCompletionClient{config: config, client: client}
}

func (c *httpCompletionClient) Complete(ctx context.Context, request ClientRequest, idempotencyKey string) (Completion, error) {
	provider := providerFamily(request.Provider)
	endpoint, payload, err := c.buildRequest(provider, request)
	if err != nil {
		return Completion{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return Completion{}, fmt.Errorf("create %s provider request: %w", request.Provider, err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("User-Agent", "agent-model-service/1")
	if idempotencyKey != "" {
		httpRequest.Header.Set("Idempotency-Key", idempotencyKey)
	}
	switch provider {
	case ProviderAnthropic:
		if c.config.APIKey != "" {
			httpRequest.Header.Set("x-api-key", c.config.APIKey)
		}
		httpRequest.Header.Set("anthropic-version", "2023-06-01")
	case ProviderGemini:
		if c.config.APIKey != "" {
			httpRequest.Header.Set("x-goog-api-key", c.config.APIKey)
		}
	default:
		if c.config.APIKey != "" {
			httpRequest.Header.Set("Authorization", "Bearer "+c.config.APIKey)
		}
	}

	response, err := c.client.Do(httpRequest)
	if err != nil {
		return Completion{}, fmt.Errorf("call %s provider: %w", request.Provider, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return Completion{}, &ProviderHTTPError{Provider: request.Provider, StatusCode: response.StatusCode}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxProviderResponseBytes+1))
	if err != nil {
		return Completion{}, fmt.Errorf("read %s provider response: %w", request.Provider, err)
	}
	if len(data) > maxProviderResponseBytes {
		return Completion{}, fmt.Errorf("%s provider response exceeds %d bytes", request.Provider, maxProviderResponseBytes)
	}
	return parseProviderResponse(provider, data)
}

func (c *httpCompletionClient) buildRequest(provider string, request ClientRequest) (string, []byte, error) {
	switch provider {
	case ProviderAnthropic:
		messages, system := anthropicMessages(request)
		maxTokens := request.MaxTokens
		if maxTokens == 0 {
			maxTokens = 1024
		}
		payload, err := json.Marshal(struct {
			Model       string        `json:"model"`
			System      string        `json:"system,omitempty"`
			Messages    []ChatMessage `json:"messages"`
			Temperature *float64      `json:"temperature,omitempty"`
			MaxTokens   int           `json:"max_tokens"`
		}{request.ModelName, system, messages, request.Temperature, maxTokens})
		return appendEndpoint(c.config.BaseURL, "/messages", "/messages"), payload, err
	case ProviderGemini:
		type part struct {
			Text string `json:"text"`
		}
		type content struct {
			Role  string `json:"role,omitempty"`
			Parts []part `json:"parts"`
		}
		contents := make([]content, 0, len(request.Messages))
		var systemParts []part
		if request.System != "" {
			systemParts = append(systemParts, part{Text: request.System})
		}
		for _, message := range request.Messages {
			role := strings.ToLower(strings.TrimSpace(message.Role))
			if role == "system" {
				systemParts = append(systemParts, part{Text: message.Content})
				continue
			}
			if role == "assistant" {
				role = "model"
			} else {
				role = "user"
			}
			contents = append(contents, content{Role: role, Parts: []part{{Text: message.Content}}})
		}
		var systemInstruction *content
		if len(systemParts) > 0 {
			systemInstruction = &content{Parts: systemParts}
		}
		generation := struct {
			Temperature     *float64 `json:"temperature,omitempty"`
			MaxOutputTokens int      `json:"maxOutputTokens,omitempty"`
		}{request.Temperature, request.MaxTokens}
		payload, err := json.Marshal(struct {
			Contents          []content `json:"contents"`
			SystemInstruction *content  `json:"systemInstruction,omitempty"`
			GenerationConfig  any       `json:"generationConfig,omitempty"`
		}{contents, systemInstruction, generation})
		endpoint := c.config.BaseURL
		if !strings.Contains(endpoint, ":generateContent") {
			endpoint = strings.TrimRight(endpoint, "/") + "/models/" + url.PathEscape(request.ModelName) + ":generateContent"
		}
		return endpoint, payload, err
	default:
		messages := append([]ChatMessage(nil), request.Messages...)
		if request.System != "" {
			messages = append([]ChatMessage{{Role: "system", Content: request.System}}, messages...)
		}
		payload, err := json.Marshal(struct {
			Model       string        `json:"model"`
			Messages    []ChatMessage `json:"messages"`
			Temperature *float64      `json:"temperature,omitempty"`
			MaxTokens   int           `json:"max_tokens,omitempty"`
		}{request.ModelName, messages, request.Temperature, request.MaxTokens})
		return appendEndpoint(c.config.BaseURL, "/chat/completions", "/chat/completions"), payload, err
	}
}

func providerFamily(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case ProviderAnthropic, "claude", "deepseek-anthropic":
		return ProviderAnthropic
	case ProviderGemini, "google":
		return ProviderGemini
	default:
		return ProviderOpenAICompatible
	}
}

func appendEndpoint(baseURL, suffix, fullSuffix string) string {
	if strings.HasSuffix(strings.TrimRight(baseURL, "/"), fullSuffix) {
		return strings.TrimRight(baseURL, "/")
	}
	return strings.TrimRight(baseURL, "/") + suffix
}

func anthropicMessages(request ClientRequest) ([]ChatMessage, string) {
	messages := make([]ChatMessage, 0, len(request.Messages))
	systems := make([]string, 0, 2)
	if request.System != "" {
		systems = append(systems, request.System)
	}
	for _, message := range request.Messages {
		role := strings.ToLower(strings.TrimSpace(message.Role))
		if role == "system" {
			systems = append(systems, message.Content)
			continue
		}
		if role != "assistant" {
			role = "user"
		}
		messages = append(messages, ChatMessage{Role: role, Content: message.Content})
	}
	return messages, strings.Join(systems, "\n\n")
}

func parseProviderResponse(provider string, data []byte) (Completion, error) {
	var content string
	switch provider {
	case ProviderAnthropic:
		var response struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(data, &response); err != nil {
			return Completion{}, fmt.Errorf("decode anthropic response: %w", err)
		}
		var parts []string
		for _, part := range response.Content {
			if part.Type == "text" && part.Text != "" {
				parts = append(parts, part.Text)
			}
		}
		content = strings.Join(parts, "")
	case ProviderGemini:
		var response struct {
			Candidates []struct {
				Content struct {
					Parts []struct {
						Text string `json:"text"`
					} `json:"parts"`
				} `json:"content"`
			} `json:"candidates"`
		}
		if err := json.Unmarshal(data, &response); err != nil {
			return Completion{}, fmt.Errorf("decode gemini response: %w", err)
		}
		if len(response.Candidates) > 0 {
			var parts []string
			for _, part := range response.Candidates[0].Content.Parts {
				parts = append(parts, part.Text)
			}
			content = strings.Join(parts, "")
		}
	default:
		var response struct {
			Choices []struct {
				Message struct {
					Content json.RawMessage `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(data, &response); err != nil {
			return Completion{}, fmt.Errorf("decode openai-compatible response: %w", err)
		}
		if len(response.Choices) > 0 {
			if err := json.Unmarshal(response.Choices[0].Message.Content, &content); err != nil {
				var parts []struct {
					Text string `json:"text"`
				}
				if arrayErr := json.Unmarshal(response.Choices[0].Message.Content, &parts); arrayErr != nil {
					return Completion{}, fmt.Errorf("decode openai-compatible message content: %w", err)
				}
				for _, part := range parts {
					content += part.Text
				}
			}
		}
	}
	if content == "" {
		return Completion{}, fmt.Errorf("%s provider response contains no text completion", provider)
	}
	return Completion{Content: content}, nil
}

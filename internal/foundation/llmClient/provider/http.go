package provider

import (
	"agent/internal/foundation/llmClient"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type authMode string

const (
	authBearer       authMode = "bearer"
	authAnthropic    authMode = "anthropic"
	authGoogleAPIKey authMode = "google_api_key"
)

const anthropicVersion = "2023-06-01"

type httpClientOptions struct {
	provider string
	model    string
	endpoint string
	apiKey   string
	builder  requestBuilder
	parser   responseParser
	auth     authMode
	client   httpDoer
	debug    BodyDebugRecorder
}

type httpClient struct {
	provider string
	model    string
	endpoint string
	apiKey   string
	builder  requestBuilder
	parser   responseParser
	auth     authMode
	client   httpDoer
	debug    BodyDebugRecorder
}

func newHTTPClient(opts httpClientOptions) httpClient {
	client := opts.client
	if client == nil {
		client = defaultHTTPClient()
	}

	return httpClient{
		provider: opts.provider,
		model:    opts.model,
		endpoint: opts.endpoint,
		apiKey:   strings.TrimSpace(opts.apiKey),
		builder:  opts.builder,
		parser:   opts.parser,
		auth:     opts.auth,
		client:   client,
		debug:    opts.debug,
	}
}

func (c httpClient) ModelName() string {
	return c.model
}

func (c httpClient) Complete(ctx context.Context, req llmClient.Request) (llmClient.Response, error) {
	if c.apiKey == "" {
		return llmClient.Response{}, fmt.Errorf("llm %s: api_key is required for model %q", c.provider, c.model)
	}

	req.Provider = c.provider
	if req.Model == "" {
		req.Model = c.model
	}

	body, err := c.builder.Build(req)
	if err != nil {
		return llmClient.Response{}, fmt.Errorf("build %s request: %w", c.provider, err)
	}

	startedAt := time.Now().UTC()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		if debugErr := c.recordDebug(ctx, startedAt, time.Now().UTC(), req, body, nil, fmt.Sprintf("create %s request: %v", c.provider, err)); debugErr != nil {
			return llmClient.Response{}, debugErr
		}
		return llmClient.Response{}, fmt.Errorf("create %s request: %w", c.provider, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	c.applyAuth(httpReq)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		if debugErr := c.recordDebug(ctx, startedAt, time.Now().UTC(), req, body, nil, fmt.Sprintf("send %s request: %v", c.provider, err)); debugErr != nil {
			return llmClient.Response{}, debugErr
		}
		return llmClient.Response{}, fmt.Errorf("send %s request: %w", c.provider, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		if debugErr := c.recordDebug(ctx, startedAt, time.Now().UTC(), req, body, resp, fmt.Sprintf("read %s response: %v", c.provider, err)); debugErr != nil {
			return llmClient.Response{}, debugErr
		}
		return llmClient.Response{}, fmt.Errorf("read %s response: %w", c.provider, err)
	}
	completedAt := time.Now().UTC()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		errText := fmt.Sprintf("llm %s request failed: %s: %s", c.provider, resp.Status, strings.TrimSpace(string(respBody)))
		if debugErr := c.recordDebug(ctx, startedAt, completedAt, req, body, resp, errText, respBody...); debugErr != nil {
			return llmClient.Response{}, debugErr
		}
		return llmClient.Response{}, fmt.Errorf("%s", errText)
	}

	parsed, err := c.parser.Parse(req, respBody)
	if err != nil {
		if debugErr := c.recordDebug(ctx, startedAt, completedAt, req, body, resp, fmt.Sprintf("parse %s response: %v", c.provider, err), respBody...); debugErr != nil {
			return llmClient.Response{}, debugErr
		}
		return llmClient.Response{}, fmt.Errorf("parse %s response: %w", c.provider, err)
	}
	if debugErr := c.recordDebug(ctx, startedAt, completedAt, req, body, resp, "", respBody...); debugErr != nil {
		return llmClient.Response{}, debugErr
	}
	return parsed, nil
}

func (c httpClient) recordDebug(ctx context.Context, startedAt, completedAt time.Time, req llmClient.Request, requestBody []byte, resp *http.Response, errorText string, responseBody ...byte) error {
	if c.debug == nil {
		return nil
	}

	entry := HTTPExchangeLog{
		Kind:         "llm_http",
		StartedAt:    startedAt,
		CompletedAt:  completedAt,
		Provider:     c.provider,
		Model:        req.Model,
		Endpoint:     c.endpoint,
		RequestBody:  DebugBody(append([]byte(nil), requestBody...)),
		ResponseBody: DebugBody(append([]byte(nil), responseBody...)),
		Error:        errorText,
	}
	if resp != nil {
		entry.StatusCode = resp.StatusCode
		entry.Status = resp.Status
	}
	if err := c.debug.Record(ctx, entry); err != nil {
		return fmt.Errorf("record %s debug body: %w", c.provider, err)
	}
	return nil
}

func (c httpClient) applyAuth(req *http.Request) {
	switch c.auth {
	case authBearer:
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	case authAnthropic:
		req.Header.Set("x-api-key", c.apiKey)
		req.Header.Set("anthropic-version", anthropicVersion)
	case authGoogleAPIKey:
		req.Header.Set("x-goog-api-key", c.apiKey)
	}
}

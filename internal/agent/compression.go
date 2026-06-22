package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"agent/internal/llm"
)

const (
	defaultContextWindowTokens          = 32_000
	defaultContextCompressionRatio      = 0.5
	contextCompressionPromptTemperature = 0.2
)

const contextWindowLookupTimeout = 10 * time.Second

var defaultContextWindowLookupClient = &http.Client{Timeout: contextWindowLookupTimeout}

var contextWindowTokenCache = struct {
	sync.RWMutex
	tokens map[string]int
}{
	tokens: make(map[string]int),
}

type SessionCompressor interface {
	Compress(ctx context.Context, input CompressionInput) (CompressionResult, error)
}

type ContextTokenCounter interface {
	CountRequest(req llm.Request) int
}

type ContextWindowLookupOptions struct {
	Provider    string
	Model       string
	ModelURL    string
	APIKey      string
	MetadataURL string
	HTTPClient  interface {
		Do(*http.Request) (*http.Response, error)
	}
}

type ContextWindowLookupResult struct {
	Model  string
	Tokens int
	Source string
}

type CompressionInput struct {
	Task                Task
	TurnID              string
	StepIndex           int
	Model               string
	Messages            []llm.Message
	TriggerTokens       int
	ContextWindowTokens int
}

type CompressionResult struct {
	Messages []llm.Message
	Summary  string
	Usage    *llm.Usage
}

type EstimatedContextTokenCounter struct{}

type compressionDecision struct {
	ShouldCompress      bool
	EstimatedTokens     int
	UsageInputTokens    int
	TriggerTokens       int
	ContextWindowTokens int
	ThresholdTokens     int
}

type contextCompressionEventPayload struct {
	Status               string     `json:"status"`
	Reason               string     `json:"reason,omitempty"`
	Error                string     `json:"error,omitempty"`
	EstimatedTokens      int        `json:"estimated_tokens,omitempty"`
	UsageInputTokens     int        `json:"usage_input_tokens,omitempty"`
	TriggerTokens        int        `json:"trigger_tokens,omitempty"`
	ThresholdTokens      int        `json:"threshold_tokens,omitempty"`
	ContextWindowTokens  int        `json:"context_window_tokens,omitempty"`
	OriginalMessageCount int        `json:"original_message_count,omitempty"`
	CompressedMessages   int        `json:"compressed_messages,omitempty"`
	Summary              string     `json:"summary,omitempty"`
	Usage                *llm.Usage `json:"usage,omitempty"`
}

type LLMSessionCompressor struct {
	llm LLMClient
}

func NewLLMSessionCompressor(client LLMClient) *LLMSessionCompressor {
	return &LLMSessionCompressor{llm: client}
}

func (c *LLMSessionCompressor) Compress(ctx context.Context, input CompressionInput) (CompressionResult, error) {
	if c == nil || c.llm == nil {
		return CompressionResult{}, fmt.Errorf("context compression: llm client is required")
	}
	if len(input.Messages) == 0 {
		return CompressionResult{}, fmt.Errorf("context compression: messages are required")
	}

	response, err := c.llm.Complete(ctx, llm.Request{
		Model: input.Model,
		Messages: []llm.Message{
			{
				Role:    llm.RoleSystem,
				Content: contextCompressionSystemPrompt(),
			},
			{
				Role:    llm.RoleUser,
				Content: buildContextCompressionPrompt(input),
			},
		},
		Temperature: contextCompressionPromptTemperature,
		Metadata: map[string]string{
			"loop":    "native",
			"purpose": "context_compression",
			"step":    fmt.Sprintf("%d", input.StepIndex),
			"turn_id": input.TurnID,
		},
	})
	if err != nil {
		return CompressionResult{}, fmt.Errorf("context compression llm complete: %w", err)
	}

	summary := strings.TrimSpace(response.Content)
	if summary == "" {
		return CompressionResult{}, fmt.Errorf("context compression: empty summary")
	}

	return CompressionResult{
		Messages: compressedHistoryMessages(input.Messages, summary),
		Summary:  summary,
		Usage:    cloneUsage(response.Usage),
	}, nil
}

func (EstimatedContextTokenCounter) CountRequest(req llm.Request) int {
	tokens := 8
	for _, msg := range req.Messages {
		tokens += 4
		tokens += estimateTextTokens(string(msg.Role))
		tokens += estimateTextTokens(msg.Name)
		tokens += estimateTextTokens(msg.ToolCallID)
		tokens += estimateTextTokens(msg.Content)
		for _, call := range msg.ToolCalls {
			tokens += 8
			tokens += estimateTextTokens(call.ID)
			tokens += estimateTextTokens(call.Name)
			tokens += estimateTextTokens(string(call.Input))
		}
	}
	for _, tool := range req.Tools {
		tokens += 12
		tokens += estimateTextTokens(tool.Name)
		tokens += estimateTextTokens(tool.Description)
		tokens += estimateTextTokens(string(tool.InputSchema))
	}
	for key, value := range req.Metadata {
		tokens += estimateTextTokens(key) + estimateTextTokens(value)
	}
	if tokens < 1 {
		return 1
	}
	return tokens
}

func contextCompressionDecision(req llm.Request, usage *llm.Usage, counter ContextTokenCounter) compressionDecision {
	if counter == nil {
		counter = EstimatedContextTokenCounter{}
	}
	window := contextWindowTokens(req.Model)
	threshold := int(float64(window) * defaultContextCompressionRatio)
	if threshold <= 0 {
		threshold = window / 2
	}
	estimated := counter.CountRequest(req)
	usageInput := 0
	if usage != nil {
		usageInput = usage.InputTokens
	}
	trigger := maxInt(estimated, usageInput)
	return compressionDecision{
		ShouldCompress:      trigger >= threshold,
		EstimatedTokens:     estimated,
		UsageInputTokens:    usageInput,
		TriggerTokens:       trigger,
		ContextWindowTokens: window,
		ThresholdTokens:     threshold,
	}
}

func contextWindowTokens(model string) int {
	name := normalizeContextWindowModel(model)
	if tokens, ok := cachedContextWindowTokens(name); ok {
		return tokens
	}
	return fallbackContextWindowTokens(model)
}

func ResolveAndCacheContextWindowTokens(ctx context.Context, opts ContextWindowLookupOptions) (ContextWindowLookupResult, error) {
	model := strings.TrimSpace(opts.Model)
	name := normalizeContextWindowModel(model)
	if tokens, ok := cachedContextWindowTokens(name); ok {
		return ContextWindowLookupResult{Model: model, Tokens: tokens, Source: "cache"}, nil
	}

	provider := contextWindowMetadataProvider(opts)
	if provider == "" || name == "" {
		tokens := fallbackContextWindowTokens(model)
		cacheContextWindowTokens(name, tokens)
		return ContextWindowLookupResult{Model: model, Tokens: tokens, Source: "fallback"}, nil
	}

	var (
		tokens int
		err    error
	)
	switch provider {
	case "gemini":
		tokens, err = lookupGeminiContextWindowTokens(ctx, opts)
	case "openrouter":
		tokens, err = lookupOpenRouterContextWindowTokens(ctx, opts)
	default:
		tokens = fallbackContextWindowTokens(model)
		cacheContextWindowTokens(name, tokens)
		return ContextWindowLookupResult{Model: model, Tokens: tokens, Source: "fallback"}, nil
	}

	if err != nil {
		tokens = fallbackContextWindowTokens(model)
		cacheContextWindowTokens(name, tokens)
		return ContextWindowLookupResult{Model: model, Tokens: tokens, Source: "fallback"}, fmt.Errorf("%s context window lookup: %w", provider, err)
	}
	cacheContextWindowTokens(name, tokens)
	return ContextWindowLookupResult{Model: model, Tokens: tokens, Source: provider}, nil
}

func fallbackContextWindowTokens(model string) int {
	name := normalizeContextWindowModel(model)
	switch {
	case name == "":
		return defaultContextWindowTokens
	case strings.HasPrefix(name, "gpt-"),
		strings.HasPrefix(name, "o1"),
		strings.HasPrefix(name, "o3"),
		strings.HasPrefix(name, "o4"),
		strings.HasPrefix(name, "chatgpt-"):
		return 128_000
	case strings.HasPrefix(name, "claude"):
		return 200_000
	case strings.HasPrefix(name, "gemini"):
		return 1_000_000
	case strings.HasPrefix(name, "deepseek"):
		return 64_000
	case strings.HasPrefix(name, "mock"):
		return defaultContextWindowTokens
	default:
		return defaultContextWindowTokens
	}
}

func lookupGeminiContextWindowTokens(ctx context.Context, opts ContextWindowLookupOptions) (int, error) {
	endpoint := geminiContextWindowMetadataURL(opts.MetadataURL, opts.ModelURL, opts.Model)
	body, err := getContextWindowMetadata(ctx, opts.HTTPClient, endpoint, func(req *http.Request) {
		if strings.TrimSpace(opts.APIKey) != "" {
			req.Header.Set("x-goog-api-key", strings.TrimSpace(opts.APIKey))
		}
	})
	if err != nil {
		return 0, err
	}
	return parseGeminiContextWindowTokens(body, opts.Model)
}

func lookupOpenRouterContextWindowTokens(ctx context.Context, opts ContextWindowLookupOptions) (int, error) {
	endpoint := strings.TrimSpace(opts.MetadataURL)
	if endpoint == "" {
		endpoint = "https://openrouter.ai/api/v1/models"
	}
	body, err := getContextWindowMetadata(ctx, opts.HTTPClient, endpoint, func(req *http.Request) {
		if strings.TrimSpace(opts.APIKey) != "" {
			req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(opts.APIKey))
		}
	})
	if err != nil {
		return 0, err
	}
	return parseOpenRouterContextWindowTokens(body, opts.Model)
}

func getContextWindowMetadata(ctx context.Context, client interface {
	Do(*http.Request) (*http.Response, error)
}, endpoint string, applyAuth func(*http.Request)) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("metadata URL is empty")
	}
	httpClient := client
	if httpClient == nil {
		httpClient = defaultContextWindowLookupClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create metadata request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if applyAuth != nil {
		applyAuth(req)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send metadata request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read metadata response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("metadata request failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func parseGeminiContextWindowTokens(body []byte, model string) (int, error) {
	var payload struct {
		Name            string `json:"name"`
		BaseModelID     string `json:"baseModelId"`
		InputTokenLimit int    `json:"inputTokenLimit"`
		Models          []struct {
			Name            string `json:"name"`
			BaseModelID     string `json:"baseModelId"`
			InputTokenLimit int    `json:"inputTokenLimit"`
		} `json:"models"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("parse gemini metadata: %w", err)
	}
	if payload.InputTokenLimit > 0 {
		return payload.InputTokenLimit, nil
	}

	target := normalizeContextWindowModel(model)
	for _, candidate := range payload.Models {
		if candidate.InputTokenLimit <= 0 {
			continue
		}
		if contextWindowModelMatches(target, candidate.Name, candidate.BaseModelID) {
			return candidate.InputTokenLimit, nil
		}
	}
	return 0, fmt.Errorf("metadata missing inputTokenLimit for model %q", model)
}

func parseOpenRouterContextWindowTokens(body []byte, model string) (int, error) {
	var payload struct {
		Data []struct {
			ID            string `json:"id"`
			CanonicalSlug string `json:"canonical_slug"`
			ContextLength int    `json:"context_length"`
			TopProvider   struct {
				ContextLength int `json:"context_length"`
			} `json:"top_provider"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("parse openrouter metadata: %w", err)
	}

	target := normalizeContextWindowModel(model)
	for _, candidate := range payload.Data {
		if !contextWindowModelMatches(target, candidate.ID, candidate.CanonicalSlug) {
			continue
		}
		if candidate.ContextLength > 0 {
			return candidate.ContextLength, nil
		}
		if candidate.TopProvider.ContextLength > 0 {
			return candidate.TopProvider.ContextLength, nil
		}
		return 0, fmt.Errorf("metadata missing context_length for model %q", model)
	}
	return 0, fmt.Errorf("metadata missing model %q", model)
}

func contextWindowMetadataProvider(opts ContextWindowLookupOptions) string {
	if isOpenRouterModelURL(opts.ModelURL) {
		return "openrouter"
	}
	return strings.ToLower(strings.TrimSpace(opts.Provider))
}

func isOpenRouterModelURL(rawURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	return host == "openrouter.ai" || strings.HasSuffix(host, ".openrouter.ai")
}

func geminiContextWindowMetadataURL(metadataURL, modelURL, model string) string {
	if strings.TrimSpace(metadataURL) != "" {
		return replaceContextWindowModelPlaceholder(metadataURL, model)
	}

	modelPath := contextWindowModelPath(model)
	rawURL := strings.TrimSpace(modelURL)
	if rawURL == "" {
		return "https://generativelanguage.googleapis.com/v1beta/models/" + modelPath
	}
	if strings.Contains(rawURL, "{model}") {
		return strings.TrimSuffix(replaceContextWindowModelPlaceholder(rawURL, model), ":generateContent")
	}

	trimmed := strings.TrimRight(rawURL, "/")
	lower := strings.ToLower(trimmed)
	if strings.HasSuffix(lower, ":generatecontent") {
		return trimmed[:len(trimmed)-len(":generateContent")]
	}
	if strings.HasSuffix(lower, "/v1beta") {
		return trimmed + "/models/" + modelPath
	}
	return trimmed + "/v1beta/models/" + modelPath
}

func replaceContextWindowModelPlaceholder(rawURL, model string) string {
	return strings.ReplaceAll(strings.TrimSpace(rawURL), "{model}", contextWindowModelPath(model))
}

func contextWindowModelPath(model string) string {
	modelPath := strings.TrimSpace(model)
	if strings.HasPrefix(strings.ToLower(modelPath), "models/") {
		modelPath = modelPath[len("models/"):]
	}
	escaped := url.PathEscape(modelPath)
	return strings.ReplaceAll(escaped, "%3A", ":")
}

func contextWindowModelMatches(target string, candidates ...string) bool {
	if target == "" {
		return false
	}
	for _, candidate := range candidates {
		name := normalizeContextWindowModel(candidate)
		if name == "" {
			continue
		}
		if name == target {
			return true
		}
		if !strings.Contains(target, "/") && strings.HasSuffix(name, "/"+target) {
			return true
		}
	}
	return false
}

func normalizeContextWindowModel(model string) string {
	name := strings.ToLower(strings.TrimSpace(model))
	if strings.HasPrefix(name, "models/") {
		return strings.TrimPrefix(name, "models/")
	}
	return name
}

func cachedContextWindowTokens(model string) (int, bool) {
	contextWindowTokenCache.RLock()
	defer contextWindowTokenCache.RUnlock()
	tokens, ok := contextWindowTokenCache.tokens[model]
	return tokens, ok
}

func cacheContextWindowTokens(model string, tokens int) {
	if tokens <= 0 {
		return
	}
	contextWindowTokenCache.Lock()
	defer contextWindowTokenCache.Unlock()
	if contextWindowTokenCache.tokens == nil {
		contextWindowTokenCache.tokens = make(map[string]int)
	}
	contextWindowTokenCache.tokens[model] = tokens
}

func resetContextWindowTokenCache() {
	contextWindowTokenCache.Lock()
	defer contextWindowTokenCache.Unlock()
	contextWindowTokenCache.tokens = make(map[string]int)
}

func estimateTextTokens(text string) int {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	asciiRunes := 0
	nonASCIIRunes := 0
	for _, r := range text {
		if r <= 127 {
			asciiRunes++
		} else {
			nonASCIIRunes++
		}
	}
	return (asciiRunes+3)/4 + nonASCIIRunes
}

func compressedHistoryMessages(messages []llm.Message, summary string) []llm.Message {
	compressed := make([]llm.Message, 0, 2)
	for _, msg := range messages {
		if msg.Role == llm.RoleSystem {
			compressed = append(compressed, cloneMessage(msg))
		}
	}
	compressed = append(compressed, llm.Message{
		Role:    llm.RoleUser,
		Content: "Conversation summary:\n" + strings.TrimSpace(summary),
	})
	return compressed
}

func contextCompressionSystemPrompt() string {
	return `You compress an agent conversation into a durable handoff summary.
Preserve facts needed to continue the work. Do not invent details.
Include the current task, key decisions, tool results, errors, user constraints, and concrete next steps.`
}

func buildContextCompressionPrompt(input CompressionInput) string {
	transcript := formatMessagesForCompression(input.Messages)
	return fmt.Sprintf(`Current task:
%s

Workspace:
%s

The current context has reached %d tokens out of a %d token window. Compress the transcript below into a concise but complete continuation summary.

Transcript:
%s`, input.Task.Input, input.Task.WorkDir, input.TriggerTokens, input.ContextWindowTokens, transcript)
}

func formatMessagesForCompression(messages []llm.Message) string {
	var builder strings.Builder
	for i, msg := range messages {
		fmt.Fprintf(&builder, "Message %d\nrole: %s\n", i+1, msg.Role)
		if msg.Name != "" {
			fmt.Fprintf(&builder, "name: %s\n", msg.Name)
		}
		if msg.ToolCallID != "" {
			fmt.Fprintf(&builder, "tool_call_id: %s\n", msg.ToolCallID)
		}
		if len(msg.ToolCalls) > 0 {
			raw, err := json.Marshal(msg.ToolCalls)
			if err == nil {
				fmt.Fprintf(&builder, "tool_calls: %s\n", raw)
			}
		}
		if strings.TrimSpace(msg.Content) != "" {
			fmt.Fprintf(&builder, "content:\n%s\n", msg.Content)
		}
		builder.WriteString("\n")
	}
	return strings.TrimSpace(builder.String())
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

package provider

import (
	"net/url"
	"strings"
)

func chatEndpoint(rawURL, fallback, suffix string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	if strings.HasSuffix(strings.ToLower(trimmed), "/v1") {
		return trimmed + "/chat/completions"
	}
	return endpointWithSuffix(rawURL, fallback, suffix, "chat/completions")
}

func messagesEndpoint(rawURL, fallback string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	if strings.HasSuffix(strings.ToLower(trimmed), "/v1") {
		return trimmed + "/messages"
	}
	return endpointWithSuffix(rawURL, fallback, "v1/messages", "v1/messages")
}

func deepSeekAnthropicEndpoint(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "https://api.deepseek.com/anthropic/v1/messages"
	}

	trimmed := strings.TrimRight(rawURL, "/")
	lower := strings.ToLower(trimmed)
	switch {
	case strings.HasSuffix(lower, "/v1/messages"):
		return trimmed
	case strings.HasSuffix(lower, "/anthropic/v1"):
		return trimmed + "/messages"
	case strings.HasSuffix(lower, "/anthropic"):
		return trimmed + "/v1/messages"
	case strings.Contains(lower, "/anthropic/"):
		return messagesEndpoint(trimmed, "https://api.deepseek.com/anthropic/v1/messages")
	default:
		return trimmed + "/anthropic/v1/messages"
	}
}

func geminiEndpoint(rawURL, model string) string {
	modelPath := strings.TrimPrefix(strings.TrimSpace(model), "models/")
	suffix := "v1beta/models/" + pathEscape(modelPath) + ":generateContent"
	if strings.TrimSpace(rawURL) == "" {
		return "https://generativelanguage.googleapis.com/" + suffix
	}
	if strings.Contains(rawURL, "{model}") {
		return strings.ReplaceAll(rawURL, "{model}", pathEscape(modelPath))
	}
	trimmed := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	if strings.HasSuffix(strings.TrimRight(rawURL, "/"), ":generateContent") {
		return strings.TrimSpace(rawURL)
	}
	if strings.HasSuffix(strings.ToLower(trimmed), "/v1beta") {
		return trimmed + "/models/" + pathEscape(modelPath) + ":generateContent"
	}
	return endpointWithSuffix(rawURL, "https://generativelanguage.googleapis.com/"+suffix, suffix, ":generateContent")
}

func endpointWithSuffix(rawURL, fallback, suffix string, completeSuffixes ...string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return fallback
	}
	trimmed := strings.TrimRight(rawURL, "/")
	lower := strings.ToLower(trimmed)
	for _, completeSuffix := range completeSuffixes {
		if strings.HasSuffix(lower, strings.ToLower(completeSuffix)) {
			return trimmed
		}
	}
	return trimmed + "/" + strings.TrimLeft(suffix, "/")
}

func pathEscape(value string) string {
	escaped := url.PathEscape(value)
	return strings.ReplaceAll(escaped, "%3A", ":")
}

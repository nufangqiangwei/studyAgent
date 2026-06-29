package provider

import "testing"

func TestChatEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		rawURL   string
		fallback string
		suffix   string
		want     string
	}{
		{
			name:     "empty uses fallback",
			fallback: "https://api.openai.com/v1/chat/completions",
			suffix:   "v1/chat/completions",
			want:     "https://api.openai.com/v1/chat/completions",
		},
		{
			name:   "openai base",
			rawURL: "https://api.openai.com",
			suffix: "v1/chat/completions",
			want:   "https://api.openai.com/v1/chat/completions",
		},
		{
			name:   "openai v1 base",
			rawURL: "https://api.openai.com/v1",
			suffix: "v1/chat/completions",
			want:   "https://api.openai.com/v1/chat/completions",
		},
		{
			name:   "deepseek base",
			rawURL: "https://api.deepseek.com",
			suffix: "chat/completions",
			want:   "https://api.deepseek.com/chat/completions",
		},
		{
			name:   "full endpoint",
			rawURL: "https://api.deepseek.com/chat/completions",
			suffix: "chat/completions",
			want:   "https://api.deepseek.com/chat/completions",
		},
	}

	for _, tt := range tests {
		got := chatEndpoint(tt.rawURL, tt.fallback, tt.suffix)
		if got != tt.want {
			t.Fatalf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestMessagesEndpoint(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "anthropic base",
			rawURL: "https://api.anthropic.com",
			want:   "https://api.anthropic.com/v1/messages",
		},
		{
			name:   "anthropic v1 base",
			rawURL: "https://api.anthropic.com/v1",
			want:   "https://api.anthropic.com/v1/messages",
		},
		{
			name:   "deepseek anthropic base",
			rawURL: "https://api.deepseek.com/anthropic",
			want:   "https://api.deepseek.com/anthropic/v1/messages",
		},
		{
			name:   "full endpoint",
			rawURL: "https://api.deepseek.com/anthropic/v1/messages",
			want:   "https://api.deepseek.com/anthropic/v1/messages",
		},
	}

	for _, tt := range tests {
		got := messagesEndpoint(tt.rawURL, "")
		if got != tt.want {
			t.Fatalf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestDeepSeekAnthropicEndpoint(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name: "empty uses deepseek anthropic endpoint",
			want: "https://api.deepseek.com/anthropic/v1/messages",
		},
		{
			name:   "deepseek base",
			rawURL: "https://api.deepseek.com",
			want:   "https://api.deepseek.com/anthropic/v1/messages",
		},
		{
			name:   "deepseek anthropic base",
			rawURL: "https://api.deepseek.com/anthropic",
			want:   "https://api.deepseek.com/anthropic/v1/messages",
		},
		{
			name:   "deepseek anthropic v1 base",
			rawURL: "https://api.deepseek.com/anthropic/v1",
			want:   "https://api.deepseek.com/anthropic/v1/messages",
		},
		{
			name:   "full endpoint",
			rawURL: "https://api.deepseek.com/anthropic/v1/messages",
			want:   "https://api.deepseek.com/anthropic/v1/messages",
		},
	}

	for _, tt := range tests {
		got := deepSeekAnthropicEndpoint(tt.rawURL)
		if got != tt.want {
			t.Fatalf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestGeminiEndpoint(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		model  string
		want   string
	}{
		{
			name:  "empty uses default",
			model: "gemini-2.5-pro",
			want:  "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent",
		},
		{
			name:   "template",
			rawURL: "http://localhost/v1beta/models/{model}:generateContent",
			model:  "gemini-2.5-pro",
			want:   "http://localhost/v1beta/models/gemini-2.5-pro:generateContent",
		},
		{
			name:   "v1beta base",
			rawURL: "https://generativelanguage.googleapis.com/v1beta",
			model:  "gemini-2.5-pro",
			want:   "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent",
		},
	}

	for _, tt := range tests {
		got := geminiEndpoint(tt.rawURL, tt.model)
		if got != tt.want {
			t.Fatalf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

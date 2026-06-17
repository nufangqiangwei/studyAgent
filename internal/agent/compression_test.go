package agent

import (
	"strings"
	"testing"

	"agent/internal/llm"
)

func TestContextWindowTokensUsesModelFamiliesAndFallback(t *testing.T) {
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

func TestContextCompressionDecisionUsesUsageBeforeEstimate(t *testing.T) {
	decision := contextCompressionDecision(llm.Request{
		Model: "mock-native",
		Messages: []llm.Message{
			{Role: llm.RoleSystem, Content: "system"},
			{Role: llm.RoleUser, Content: "small prompt"},
		},
	}, &llm.Usage{InputTokens: 16_000}, EstimatedContextTokenCounter{})

	if !decision.ShouldCompress {
		t.Fatalf("ShouldCompress = false, want true: %#v", decision)
	}
	if decision.TriggerTokens != 16_000 || decision.UsageInputTokens != 16_000 {
		t.Fatalf("decision = %#v, want usage-driven trigger", decision)
	}
}

func TestContextCompressionDecisionFallsBackToEstimate(t *testing.T) {
	decision := contextCompressionDecision(llm.Request{
		Model: "mock-native",
		Messages: []llm.Message{
			{Role: llm.RoleSystem, Content: "system"},
			{Role: llm.RoleUser, Content: strings.Repeat("a", 64_000)},
		},
	}, nil, EstimatedContextTokenCounter{})

	if !decision.ShouldCompress {
		t.Fatalf("ShouldCompress = false, want true from estimate: %#v", decision)
	}
	if decision.EstimatedTokens < decision.ThresholdTokens {
		t.Fatalf("estimated tokens = %d, threshold = %d", decision.EstimatedTokens, decision.ThresholdTokens)
	}
}

package building

import "testing"

func TestInlinePayloadPolicyDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	policy := (InlinePayloadPolicy{}).WithDefaults()
	if policy.MaxMessageBytes != DefaultMaxInlinePayloadBytes || policy.MaxEventBytes != DefaultMaxInlinePayloadBytes || policy.MaxReplyBytes != DefaultMaxInlinePayloadBytes || policy.MaxEffectBytes != DefaultMaxInlinePayloadBytes {
		t.Fatalf("defaults = %#v", policy)
	}
	issues := validateInlinePayloadPolicy(InlinePayloadPolicy{MaxMessageBytes: -1})
	if len(issues) != 1 || issues[0].Path != "payloads.max_message_bytes" {
		t.Fatalf("issues = %#v", issues)
	}
}

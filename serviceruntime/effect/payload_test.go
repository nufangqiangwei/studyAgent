package effect

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/fault"
	"testing"
)

func TestValidateEffectResultPayload(t *testing.T) {
	t.Parallel()
	policy := building.InlinePayloadPolicy{MaxEffectBytes: 4}
	if err := validateEffectResultPayload(policy, []byte("1234")); err != nil {
		t.Fatalf("inline result rejected: %v", err)
	}
	err := validateEffectResultPayload(policy, []byte("12345"))
	if fault.KindOf(err) != fault.Permanent {
		t.Fatalf("oversized result error = %v", err)
	}
}

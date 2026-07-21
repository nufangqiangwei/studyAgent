package host

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/service"
	"strings"
	"testing"
)

func TestValidateInlineDecisionPayloads(t *testing.T) {
	t.Parallel()
	policy := building.InlinePayloadPolicy{MaxMessageBytes: 4, MaxEventBytes: 4, MaxReplyBytes: 4, MaxEffectBytes: 4}
	decision := service.Decision{Events: []service.NewEvent{{Key: "large", Payload: []byte("12345")}}}
	if err := validateInlineDecisionPayloads(policy, decision); err == nil || !strings.Contains(err.Error(), "ArtifactRef") {
		t.Fatalf("event error = %v", err)
	}
	decision = service.Decision{Effects: []service.PlannedEffect{{Key: "large", Payload: []byte("12345")}}}
	if err := validateInlineDecisionPayloads(policy, decision); err == nil {
		t.Fatal("oversized effect was accepted")
	}
}

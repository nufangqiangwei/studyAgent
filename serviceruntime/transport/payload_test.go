package transport

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"strings"
	"testing"
)

func TestValidateInlineMessagePayload(t *testing.T) {
	t.Parallel()
	policy := building.InlinePayloadPolicy{MaxMessageBytes: 4, MaxReplyBytes: 2}
	if err := validateInlineMessagePayload(policy, contract.Message{Kind: contract.MessageCommand, Payload: []byte("12345")}); err == nil || !strings.Contains(err.Error(), "ArtifactRef") {
		t.Fatalf("command error = %v", err)
	}
	if err := validateInlineMessagePayload(policy, contract.Message{Kind: contract.MessageReply, Payload: []byte("123")}); err == nil {
		t.Fatal("oversized reply was accepted")
	}
	if err := validateInlineMessagePayload(policy, contract.Message{Kind: contract.MessageCommand, Payload: []byte("1234")}); err != nil {
		t.Fatalf("inline message rejected: %v", err)
	}
}

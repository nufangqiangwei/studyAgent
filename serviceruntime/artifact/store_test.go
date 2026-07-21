package artifact_test

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"testing"
)

func TestValidateRefRejectsUnsafeKey(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"", "../secret", "/absolute", "a\\b", "a//b", "a:b"} {
		err := artifact.ValidateRef(contract.ArtifactRef{Store: "test", Key: key})
		if err == nil {
			t.Fatalf("ValidateRef(%q) succeeded", key)
		}
	}
}

func TestValidateChecksum(t *testing.T) {
	t.Parallel()
	valid := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err := artifact.ValidateChecksum(valid); err != nil {
		t.Fatalf("ValidateChecksum(valid): %v", err)
	}
	if err := artifact.ValidateChecksum("sha256:nope"); err == nil {
		t.Fatal("ValidateChecksum(invalid) succeeded")
	}
}

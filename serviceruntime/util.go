package serviceruntime

import (
	"agent/serviceruntime/contract"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type StableIDs struct{}

func (StableIDs) New(kind string) (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "id"
	}
	return fmt.Sprintf("%s-%s", kind, hex.EncodeToString(raw)), nil
}

func (StableIDs) Derive(kind string, parts ...string) string {
	input := kind + "\x00" + strings.Join(parts, "\x00")
	sum := sha256.Sum256([]byte(input))
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "id"
	}
	return fmt.Sprintf("%s-%s", kind, hex.EncodeToString(sum[:12]))
}

type NoopRecorder struct{}

func (NoopRecorder) RecordRuntimeEvent(context.Context, contract.RuntimeEvent) error { return nil }

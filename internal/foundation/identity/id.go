package identity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

func New(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	encoded := make([]byte, 32)
	hex.Encode(encoded, raw[:])
	id := fmt.Sprintf("%s-%s-%s-%s-%s",
		encoded[0:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:32],
	)
	if strings.TrimSpace(prefix) == "" {
		return id, nil
	}
	return strings.TrimSpace(prefix) + "_" + id, nil
}

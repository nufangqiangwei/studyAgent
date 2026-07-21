package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
)

func NewChecksum() hash.Hash {
	return sha256.New()
}

func FormatChecksum(sum []byte) string {
	return "sha256:" + hex.EncodeToString(sum)
}

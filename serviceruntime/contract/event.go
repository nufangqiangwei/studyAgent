package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

func StateChecksum(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

type StoredEvent struct {
	EventID        string            `json:"event_id"`
	StreamID       StreamID          `json:"stream_id"`
	StreamType     string            `json:"stream_type"`
	Sequence       uint64            `json:"sequence"`
	EventType      EventType         `json:"event_type"`
	EventVersion   int               `json:"event_version"`
	PlanRevision   PlanRevision      `json:"plan_revision"`
	ServiceVersion string            `json:"service_version"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	CausationID    string            `json:"causation_id,omitempty"`
	Payload        json.RawMessage   `json:"payload,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	OccurredAt     time.Time         `json:"occurred_at"`
}

func (e StoredEvent) Clone() StoredEvent {
	e.Payload = CloneRaw(e.Payload)
	e.Metadata = CloneStrings(e.Metadata)
	return e
}

type Snapshot struct {
	StreamID      StreamID        `json:"stream_id"`
	AggregateType string          `json:"aggregate_type"`
	OwnerService  ServiceAddress  `json:"owner_service"`
	PlanRevision  PlanRevision    `json:"plan_revision"`
	SchemaVersion int             `json:"schema_version"`
	LastSequence  uint64          `json:"last_sequence"`
	State         json.RawMessage `json:"state"`
	Checksum      string          `json:"checksum"`
	CreatedAt     time.Time       `json:"created_at"`
}

func (s Snapshot) Clone() Snapshot {
	s.State = CloneRaw(s.State)
	return s
}

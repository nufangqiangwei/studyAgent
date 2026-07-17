package contract

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type MessageKind string

const (
	MessageCommand MessageKind = "command"
	MessageQuery   MessageKind = "query"
	MessageEvent   MessageKind = "event"
	MessageReply   MessageKind = "reply"
)

const MetadataReplyError = "runtime.reply.error"

func (k MessageKind) Valid() bool {
	switch k {
	case MessageCommand, MessageQuery, MessageEvent, MessageReply:
		return true
	default:
		return false
	}
}

type Message struct {
	ID      string      `json:"id"`
	Kind    MessageKind `json:"kind"`
	Type    MessageType `json:"type"`
	Version int         `json:"version"`

	From    ServiceAddress `json:"from,omitempty"`
	To      ServiceAddress `json:"to,omitempty"`
	ReplyTo ServiceAddress `json:"reply_to,omitempty"`

	RuntimeID    RuntimeID    `json:"runtime_id"`
	PlanRevision PlanRevision `json:"plan_revision"`
	UserID       string       `json:"user_id,omitempty"`
	GoalID       string       `json:"goal_id,omitempty"`
	RunID        string       `json:"run_id,omitempty"`

	CorrelationID string `json:"correlation_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`

	StreamID StreamID `json:"stream_id,omitempty"`
	Sequence uint64   `json:"sequence,omitempty"`

	Deadline *time.Time `json:"deadline,omitempty"`
	Attempt  int        `json:"attempt,omitempty"`

	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (m Message) Validate() error {
	if strings.TrimSpace(m.ID) == "" {
		return fmt.Errorf("message id is required")
	}
	if !m.Kind.Valid() {
		return fmt.Errorf("message kind %q is invalid", m.Kind)
	}
	if strings.TrimSpace(string(m.Type)) == "" {
		return fmt.Errorf("message type is required")
	}
	if m.Version <= 0 {
		return fmt.Errorf("message version must be positive")
	}
	if strings.TrimSpace(string(m.RuntimeID)) == "" {
		return fmt.Errorf("message runtime id is required")
	}
	if strings.TrimSpace(string(m.PlanRevision)) == "" {
		return fmt.Errorf("message plan revision is required")
	}
	return nil
}

func (m Message) Clone() Message {
	m.Payload = CloneRaw(m.Payload)
	m.Metadata = CloneStrings(m.Metadata)
	if m.Deadline != nil {
		deadline := *m.Deadline
		m.Deadline = &deadline
	}
	return m
}

type ArtifactRef struct {
	Store       string `json:"store"`
	Key         string `json:"key"`
	ContentType string `json:"content_type,omitempty"`
	Checksum    string `json:"checksum,omitempty"`
	Size        int64  `json:"size,omitempty"`
}

func CloneRaw(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), value...)
}

func CloneStrings(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

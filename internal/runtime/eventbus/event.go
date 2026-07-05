package eventbus

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type EventType string

type Event struct {
	ID         string            `json:"id"`
	Topic      string            `json:"topic,omitempty"`
	Type       EventType         `json:"type"`
	TaskID     string            `json:"task_id,omitempty"`
	OccurredAt time.Time         `json:"occurred_at"`
	Source     string            `json:"source,omitempty"`
	Payload    json.RawMessage   `json:"payload,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

func (e Event) Clone() Event {
	cloned := e
	cloned.Payload = append(json.RawMessage(nil), e.Payload...)
	if len(e.Metadata) > 0 {
		cloned.Metadata = make(map[string]string, len(e.Metadata))
		for key, value := range e.Metadata {
			cloned.Metadata[key] = value
		}
	}
	return cloned
}

type EventOption func(*Event)

func WithEventID(id string) EventOption {
	return func(e *Event) {
		e.ID = id
	}
}

func WithTaskID(taskID string) EventOption {
	return func(e *Event) {
		e.TaskID = taskID
	}
}

func WithOccurredAt(occurredAt time.Time) EventOption {
	return func(e *Event) {
		e.OccurredAt = occurredAt
	}
}

func WithSource(source string) EventOption {
	return func(e *Event) {
		e.Source = source
	}
}

func WithMetadata(metadata map[string]string) EventOption {
	return func(e *Event) {
		if len(metadata) == 0 {
			return
		}
		if e.Metadata == nil {
			e.Metadata = make(map[string]string, len(metadata))
		}
		for key, value := range metadata {
			e.Metadata[key] = value
		}
	}
}

func WithMetadataValue(key, value string) EventOption {
	return func(e *Event) {
		if strings.TrimSpace(key) == "" {
			return
		}
		if e.Metadata == nil {
			e.Metadata = make(map[string]string, 1)
		}
		e.Metadata[key] = value
	}
}

func NewEvent(topic string, eventType EventType, payload any, options ...EventOption) (Event, error) {
	rawPayload, err := marshalPayload(payload)
	if err != nil {
		return Event{}, fmt.Errorf("event %q: marshal payload: %w", eventType, err)
	}
	event := Event{
		Topic:   topic,
		Type:    eventType,
		Payload: rawPayload,
	}
	for _, option := range options {
		if option != nil {
			option(&event)
		}
	}
	return completeEvent(event)
}

func completeEvent(event Event) (Event, error) {
	event = event.Clone()
	if strings.TrimSpace(string(event.Type)) == "" {
		return Event{}, fmt.Errorf("event type is required")
	}
	if strings.TrimSpace(event.ID) == "" {
		id, err := newID()
		if err != nil {
			return Event{}, err
		}
		event.ID = id
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	return event, nil
}

func marshalPayload(payload any) (json.RawMessage, error) {
	if payload == nil {
		return nil, nil
	}
	switch value := payload.(type) {
	case json.RawMessage:
		return append(json.RawMessage(nil), value...), nil
	case []byte:
		return append(json.RawMessage(nil), value...), nil
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return json.RawMessage(data), nil
	}
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("eventbus: generate id: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	encoded := make([]byte, 32)
	hex.Encode(encoded, raw[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		encoded[0:8],
		encoded[8:12],
		encoded[12:16],
		encoded[16:20],
		encoded[20:32],
	), nil
}

package event

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Type string

const AnyType Type = "*"

type DeliveryMode string

const (
	DeliveryCanBeIntercepted      DeliveryMode = "can_be_intercepted"
	DeliveryMustReachStateMachine DeliveryMode = "must_reach_state_machine"
)

type Definition struct {
	Type        Type         `json:"type"`
	Description string       `json:"description,omitempty"`
	Delivery    DeliveryMode `json:"delivery"`
}

func (d Definition) normalized() Definition {
	if d.Delivery == "" {
		d.Delivery = DeliveryCanBeIntercepted
	}
	return d
}

func (d Definition) Validate() error {
	if strings.TrimSpace(string(d.Type)) == "" {
		return fmt.Errorf("event definition: type is required")
	}
	if d.Type == AnyType {
		return fmt.Errorf("event definition: %q is reserved for hook matching", AnyType)
	}
	switch d.normalized().Delivery {
	case DeliveryCanBeIntercepted, DeliveryMustReachStateMachine:
		return nil
	default:
		return fmt.Errorf("event definition %q: unsupported delivery mode %q", d.Type, d.Delivery)
	}
}

func (d Definition) RequiresStateMachine() bool {
	return d.normalized().Delivery == DeliveryMustReachStateMachine
}

func (d Definition) Interceptable() bool {
	return !d.RequiresStateMachine()
}

type Event struct {
	ID            string            `json:"id"`
	Type          Type              `json:"type"`
	OccurredAt    time.Time         `json:"occurred_at"`
	Source        string            `json:"source,omitempty"`
	RunID         string            `json:"run_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	Payload       json.RawMessage   `json:"payload,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
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

func WithID(id string) EventOption {
	return func(e *Event) {
		e.ID = id
	}
}

func WithTime(occurredAt time.Time) EventOption {
	return func(e *Event) {
		e.OccurredAt = occurredAt
	}
}

func WithSource(source string) EventOption {
	return func(e *Event) {
		e.Source = source
	}
}

func WithRunID(runID string) EventOption {
	return func(e *Event) {
		e.RunID = runID
	}
}

func WithCorrelationID(correlationID string) EventOption {
	return func(e *Event) {
		e.CorrelationID = correlationID
	}
}

func WithCausationID(causationID string) EventOption {
	return func(e *Event) {
		e.CausationID = causationID
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
		if key == "" {
			return
		}
		if e.Metadata == nil {
			e.Metadata = make(map[string]string, 1)
		}
		e.Metadata[key] = value
	}
}

func New(eventType Type, payload any, options ...EventOption) (Event, error) {
	return DefaultRegistry().NewEvent(eventType, payload, options...)
}

func newEvent(eventType Type, payload any, options ...EventOption) (Event, error) {
	if strings.TrimSpace(string(eventType)) == "" {
		return Event{}, fmt.Errorf("event: type is required")
	}
	rawPayload, err := marshalPayload(payload)
	if err != nil {
		return Event{}, fmt.Errorf("event %q: marshal payload: %w", eventType, err)
	}
	id, err := newID()
	if err != nil {
		return Event{}, err
	}
	e := Event{
		ID:         id,
		Type:       eventType,
		OccurredAt: time.Now().UTC(),
		Payload:    rawPayload,
	}
	for _, option := range options {
		if option != nil {
			option(&e)
		}
	}
	if strings.TrimSpace(e.ID) == "" {
		return Event{}, fmt.Errorf("event %q: id is required", eventType)
	}
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now().UTC()
	}
	e.Payload = append(json.RawMessage(nil), e.Payload...)
	return e, nil
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
		return "", fmt.Errorf("event: generate id: %w", err)
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

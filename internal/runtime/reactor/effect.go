package reactor

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

type EffectType string

const (
	EffectNoop             EffectType = "noop"
	EffectModelCall        EffectType = "model.call"
	EffectToolDispatch     EffectType = "tool.dispatch"
	EffectAgentStart       EffectType = "agent.start"
	EffectAgentResume      EffectType = "agent.resume"
	EffectUserInputRequest EffectType = "user_input.request"
	EffectSubAgentDispatch EffectType = "sub_agent.dispatch"
)

type Effect struct {
	ID       string            `json:"id"`
	TaskID   string            `json:"task_id,omitempty"`
	Type     EffectType        `json:"type"`
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (e Effect) Clone() Effect {
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

type EffectOption func(*Effect)

func WithEffectID(id string) EffectOption {
	return func(effect *Effect) {
		effect.ID = id
	}
}

func WithEffectMetadata(metadata map[string]string) EffectOption {
	return func(effect *Effect) {
		if len(metadata) == 0 {
			return
		}
		if effect.Metadata == nil {
			effect.Metadata = make(map[string]string, len(metadata))
		}
		for key, value := range metadata {
			effect.Metadata[key] = value
		}
	}
}

func WithEffectMetadataValue(key, value string) EffectOption {
	return func(effect *Effect) {
		if strings.TrimSpace(key) == "" {
			return
		}
		if effect.Metadata == nil {
			effect.Metadata = make(map[string]string, 1)
		}
		effect.Metadata[key] = value
	}
}

func NewEffect(taskID string, effectType EffectType, payload any, options ...EffectOption) (Effect, error) {
	rawPayload, err := marshalPayload(payload)
	if err != nil {
		return Effect{}, fmt.Errorf("effect %q: marshal payload: %w", effectType, err)
	}
	effect := Effect{
		TaskID:  taskID,
		Type:    effectType,
		Payload: rawPayload,
	}
	for _, option := range options {
		if option != nil {
			option(&effect)
		}
	}
	return completeEffect(effect)
}

func completeEffect(effect Effect) (Effect, error) {
	effect = effect.Clone()
	if strings.TrimSpace(string(effect.Type)) == "" {
		return Effect{}, fmt.Errorf("effect type is required")
	}
	if strings.TrimSpace(effect.ID) == "" {
		id, err := newID("eff")
		if err != nil {
			return Effect{}, err
		}
		effect.ID = id
	}
	return effect, nil
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

func newID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("reactor: generate id: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	encoded := make([]byte, 32)
	hex.Encode(encoded, raw[:])
	id := fmt.Sprintf("%s-%s-%s-%s-%s",
		encoded[0:8],
		encoded[8:12],
		encoded[12:16],
		encoded[16:20],
		encoded[20:32],
	)
	if strings.TrimSpace(prefix) == "" {
		return id, nil
	}
	return prefix + "_" + id, nil
}

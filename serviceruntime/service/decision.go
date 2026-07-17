package service

import (
	"agent/serviceruntime/contract"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Decision struct {
	Events   []NewEvent        `json:"events,omitempty"`
	Outgoing []OutgoingMessage `json:"outgoing,omitempty"`
	Effects  []PlannedEffect   `json:"effects,omitempty"`
	Reply    *Reply            `json:"reply,omitempty"`
}

type NewEvent struct {
	Key      string             `json:"key"`
	Type     contract.EventType `json:"type"`
	Version  int                `json:"version"`
	Payload  json.RawMessage    `json:"payload,omitempty"`
	Metadata map[string]string  `json:"metadata,omitempty"`
}

type OutgoingMessage struct {
	Key      string                  `json:"key"`
	Kind     contract.MessageKind    `json:"kind"`
	Type     contract.MessageType    `json:"type"`
	Version  int                     `json:"version"`
	To       contract.ServiceAddress `json:"to,omitempty"`
	ReplyTo  contract.ServiceAddress `json:"reply_to,omitempty"`
	StreamID contract.StreamID       `json:"stream_id,omitempty"`
	Deadline *time.Time              `json:"deadline,omitempty"`
	Payload  json.RawMessage         `json:"payload,omitempty"`
	Metadata map[string]string       `json:"metadata,omitempty"`
}

type Reply struct {
	Key      string               `json:"key"`
	Type     contract.MessageType `json:"type"`
	Version  int                  `json:"version"`
	Payload  json.RawMessage      `json:"payload,omitempty"`
	Error    *ReplyError          `json:"error,omitempty"`
	Metadata map[string]string    `json:"metadata,omitempty"`
}

type ReplyError struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Retryable bool              `json:"retryable,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

type PlannedEffect struct {
	Key            string              `json:"key"`
	Type           contract.EffectType `json:"type"`
	Version        int                 `json:"version"`
	ExecutorRef    string              `json:"executor_ref"`
	IdempotencyKey string              `json:"idempotency_key"`
	Payload        json.RawMessage     `json:"payload,omitempty"`
	Deadline       *time.Time          `json:"deadline,omitempty"`
	Metadata       map[string]string   `json:"metadata,omitempty"`
}

func (d Decision) Validate(input contract.Message, knownEffect func(string) bool) error {
	if input.Kind == contract.MessageQuery && (len(d.Events) > 0 || len(d.Effects) > 0) {
		return fmt.Errorf("query %q cannot produce events or effects", input.Type)
	}
	if d.Reply != nil && strings.TrimSpace(string(input.ReplyTo)) == "" {
		return fmt.Errorf("reply requires input reply_to")
	}
	if err := validateEvents(d.Events); err != nil {
		return err
	}
	if err := validateOutgoing(d.Outgoing); err != nil {
		return err
	}
	if err := validateEffects(d.Effects, knownEffect); err != nil {
		return err
	}
	if d.Reply != nil {
		if strings.TrimSpace(d.Reply.Key) == "" || strings.TrimSpace(string(d.Reply.Type)) == "" || d.Reply.Version <= 0 {
			return fmt.Errorf("reply key, type and positive version are required")
		}
	}
	return nil
}

func validateEvents(events []NewEvent) error {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		key := strings.TrimSpace(event.Key)
		if key == "" || strings.TrimSpace(string(event.Type)) == "" || event.Version <= 0 {
			return fmt.Errorf("event key, type and positive version are required")
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate event key %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateOutgoing(messages []OutgoingMessage) error {
	seen := make(map[string]struct{}, len(messages))
	for _, message := range messages {
		key := strings.TrimSpace(message.Key)
		if key == "" || !message.Kind.Valid() || strings.TrimSpace(string(message.Type)) == "" || message.Version <= 0 {
			return fmt.Errorf("outgoing message key, kind, type and positive version are required")
		}
		if message.Kind != contract.MessageEvent && strings.TrimSpace(string(message.To)) == "" {
			return fmt.Errorf("outgoing %s %q requires a target", message.Kind, message.Type)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate outgoing message key %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateEffects(effects []PlannedEffect, knownEffect func(string) bool) error {
	seen := make(map[string]struct{}, len(effects))
	for _, effect := range effects {
		key := strings.TrimSpace(effect.Key)
		if key == "" || strings.TrimSpace(string(effect.Type)) == "" || effect.Version <= 0 {
			return fmt.Errorf("effect key, type and positive version are required")
		}
		if strings.TrimSpace(effect.ExecutorRef) == "" || strings.TrimSpace(effect.IdempotencyKey) == "" {
			return fmt.Errorf("effect %q requires executor_ref and idempotency_key", key)
		}
		if knownEffect != nil && !knownEffect(effect.ExecutorRef) {
			return fmt.Errorf("effect executor %q is not registered", effect.ExecutorRef)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate effect key %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

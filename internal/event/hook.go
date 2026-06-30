package event

import (
	"context"
	"fmt"
	"strings"
)

type HookLevel int

const (
	HookLevelUnset    HookLevel = 0
	HookLevelCritical HookLevel = 100
	HookLevelHigh     HookLevel = 200
	HookLevelNormal   HookLevel = 500
	HookLevelLow      HookLevel = 900
)

type HookAction string

const (
	HookContinue HookAction = "continue"
	HookStop     HookAction = "stop"
)

type HookResult struct {
	Action HookAction `json:"action"`
	Reason string     `json:"reason,omitempty"`
}

func Continue() HookResult {
	return HookResult{Action: HookContinue}
}

func Stop(reason string) HookResult {
	return HookResult{Action: HookStop, Reason: reason}
}

type HookFunc func(ctx context.Context, event Event) (HookResult, error)

type HookPredicate func(ctx context.Context, event Event) bool

type HookSpec struct {
	Name      string
	EventType Type
	Level     HookLevel
	When      HookPredicate
	Handle    HookFunc
}

func (h HookSpec) validate(registry *Registry) error {
	if strings.TrimSpace(h.Name) == "" {
		return fmt.Errorf("event hook: name is required")
	}
	if h.EventType == "" {
		return fmt.Errorf("event hook %q: event type is required", h.Name)
	}
	if h.Level == HookLevelUnset {
		return fmt.Errorf("event hook %q: level is required", h.Name)
	}
	if h.Handle == nil {
		return fmt.Errorf("event hook %q: handler is required", h.Name)
	}
	if h.EventType == AnyType {
		return nil
	}
	if registry == nil {
		return fmt.Errorf("event hook %q: registry is required", h.Name)
	}
	if _, ok := registry.Lookup(h.EventType); !ok {
		return fmt.Errorf("event hook %q: event definition %q is not registered", h.Name, h.EventType)
	}
	return nil
}

type HookExecution struct {
	Name        string     `json:"name"`
	EventType   Type       `json:"event_type"`
	Level       HookLevel  `json:"level"`
	Action      HookAction `json:"action"`
	Reason      string     `json:"reason,omitempty"`
	StopIgnored bool       `json:"stop_ignored,omitempty"`
}

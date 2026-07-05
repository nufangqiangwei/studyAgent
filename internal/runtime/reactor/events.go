package reactor

import (
	"agent/internal/runtime/eventbus"
)

const (
	DefaultInternalTopic = "reactor"
	InternalMetadataKey  = "reactor.internal"
)

const (
	EventReactorError    eventbus.EventType = "reactor.error"
	EventEffectStarted   eventbus.EventType = "reactor.effect.started"
	EventEffectSucceeded eventbus.EventType = "reactor.effect.succeeded"
	EventEffectFailed    eventbus.EventType = "reactor.effect.failed"
)

type ErrorStage string

const (
	StageEventEntry    ErrorStage = "event_entry"
	StageTaskRoute     ErrorStage = "task_route"
	StageStateMachine  ErrorStage = "state_machine"
	StageEffectRoute   ErrorStage = "effect_route"
	StageEffectExecute ErrorStage = "effect_execute"
	StagePublish       ErrorStage = "publish"
)

type ErrorPayload struct {
	Stage      ErrorStage         `json:"stage"`
	TaskID     string             `json:"task_id,omitempty"`
	EventID    string             `json:"event_id,omitempty"`
	EventType  eventbus.EventType `json:"event_type,omitempty"`
	EffectID   string             `json:"effect_id,omitempty"`
	EffectType EffectType         `json:"effect_type,omitempty"`
	Message    string             `json:"message"`
}

type EffectLifecyclePayload struct {
	TaskID     string     `json:"task_id,omitempty"`
	EffectID   string     `json:"effect_id"`
	EffectType EffectType `json:"effect_type"`
	Status     string     `json:"status"`
	Error      string     `json:"error,omitempty"`
}

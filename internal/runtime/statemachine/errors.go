package statemachine

import (
	"agent/internal/runtime/eventbus"
	"fmt"
)

type IllegalEventError struct {
	TaskID string
	Phase  TaskPhase
	Event  eventbus.EventType
	Reason string
}

func (e *IllegalEventError) Error() string {
	if e == nil {
		return ""
	}
	if e.Reason != "" {
		return fmt.Sprintf("task %s: event %s is illegal in phase %s: %s", e.TaskID, e.Event, e.Phase, e.Reason)
	}
	return fmt.Sprintf("task %s: event %s is illegal in phase %s", e.TaskID, e.Event, e.Phase)
}

func illegalEvent(state TaskState, event eventbus.Event, reason string) error {
	return &IllegalEventError{
		TaskID: state.TaskID,
		Phase:  state.Phase,
		Event:  event.Type,
		Reason: reason,
	}
}

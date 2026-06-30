package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	runtimeevent "agent/internal/event"
)

type EffectExecutor interface {
	Execute(ctx context.Context, effect Effect) ([]runtimeevent.Event, error)
}

type Machine struct {
	states   StateStore
	events   EventStore
	reducers *ReducerRegistry
	executor EffectExecutor
}

func NewMachine(states StateStore, events EventStore, reducers *ReducerRegistry, executor EffectExecutor) *Machine {
	return &Machine{
		states:   states,
		events:   events,
		reducers: reducers,
		executor: executor,
	}
}

func (m *Machine) HandleEvent(ctx context.Context, event runtimeevent.Event) error {
	return m.Dispatch(ctx, event)
}

func (m *Machine) Dispatch(ctx context.Context, event runtimeevent.Event) error {
	if event.RunID == "" {
		return fmt.Errorf("event run_id is required")
	}
	if m == nil {
		return fmt.Errorf("machine is nil")
	}
	if m.states == nil {
		return fmt.Errorf("state store is required")
	}
	if m.events == nil {
		return fmt.Errorf("event store is required")
	}
	if event.ID == "" {
		return fmt.Errorf("event id is required")
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}

	current, err := m.states.Load(ctx, event.RunID)
	if err != nil {
		return err
	}

	if err := m.events.Append(ctx, event); err != nil {
		return err
	}

	next, effects, err := m.reducers.Reduce(ctx, current, event)
	if err != nil {
		return err
	}

	if err := m.states.Save(ctx, next); err != nil {
		return err
	}

	for _, effect := range effects {
		if m.executor == nil {
			continue
		}

		nextEvents, err := m.executor.Execute(ctx, effect)
		if err != nil {
			failed := runtimeevent.Event{
				ID:         NewID("evt"),
				RunID:      effect.RunID,
				Type:       runtimeevent.EventEffectFailed,
				OccurredAt: time.Now().UTC(),
			}
			failed.Payload = marshalEffectError(effect.ID, err)
			nextEvents = []runtimeevent.Event{failed}
		}

		for _, nextEvent := range nextEvents {
			if nextEvent.RunID == "" {
				nextEvent.RunID = event.RunID
			}

			if err := m.Dispatch(ctx, nextEvent); err != nil {
				return err
			}
		}
	}

	return nil
}

func marshalEffectError(effectID string, err error) json.RawMessage {
	payload, marshalErr := json.Marshal(struct {
		EffectID string `json:"effect_id"`
		Error    string `json:"error"`
	}{
		EffectID: effectID,
		Error:    err.Error(),
	})
	if marshalErr != nil {
		return nil
	}
	return payload
}

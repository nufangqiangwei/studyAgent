package state

import (
	"context"
	"fmt"
	"reflect"
	"time"

	runtimeevent "agent/internal/event"
)

type AdvanceResult struct {
	RunID   string   `json:"run_id"`
	State   RunState `json:"state"`
	Effects []Effect `json:"effects,omitempty"`
}

type StateChangedPayload struct {
	PreviousPhase   RunPhase      `json:"previous_phase"`
	NextPhase       RunPhase      `json:"next_phase"`
	PreviousStep    int           `json:"previous_step"`
	NextStep        int           `json:"next_step"`
	PreviousWaiting *WaitingState `json:"previous_waiting,omitempty"`
	NextWaiting     *WaitingState `json:"next_waiting,omitempty"`
	PreviousError   *ErrorState   `json:"previous_error,omitempty"`
	NextError       *ErrorState   `json:"next_error,omitempty"`
}

type ContextPersistedPayload struct {
	LastEventID      string `json:"last_event_id"`
	EffectsPersisted int    `json:"effects_persisted"`
}

type Machine struct {
	states   StateStore
	events   EventStore
	effects  EffectStore
	reducers *ReducerRegistry
}

func NewMachine(states StateStore, events EventStore, effects EffectStore, reducers *ReducerRegistry) *Machine {
	return &Machine{
		states:   states,
		events:   events,
		effects:  effects,
		reducers: reducers,
	}
}

func (m *Machine) HandleEvent(ctx context.Context, event runtimeevent.Event) error {
	_, err := m.Advance(ctx, event)
	return err
}

func (m *Machine) Dispatch(ctx context.Context, event runtimeevent.Event) error {
	_, err := m.Advance(ctx, event)
	return err
}

func effectIDForEvent(eventID string, index int) string {
	if eventID == "" {
		return NewID("eff")
	}
	return fmt.Sprintf("eff_%s_%d", eventID, index+1)
}

func (m *Machine) Advance(ctx context.Context, event runtimeevent.Event) (*AdvanceResult, error) {
	if event.RunID == "" {
		return nil, fmt.Errorf("event run_id is required")
	}
	if m == nil {
		return nil, fmt.Errorf("machine is nil")
	}
	if m.states == nil {
		return nil, fmt.Errorf("state store is required")
	}
	if m.events == nil {
		return nil, fmt.Errorf("event store is required")
	}
	if m.effects == nil {
		return nil, fmt.Errorf("effect store is required")
	}
	if event.ID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}

	appended, err := m.events.Append(ctx, event)
	if err != nil {
		return nil, err
	}

	current, err := m.states.Load(ctx, event.RunID)
	if err != nil {
		return nil, err
	}
	if !appended && current.LastEventID == event.ID {
		return &AdvanceResult{
			RunID: event.RunID,
			State: current,
		}, nil
	}

	next, effects, err := m.reducers.Reduce(ctx, current, event)
	if err != nil {
		return nil, err
	}

	persistedEffects := make([]Effect, 0, len(effects))
	for i, effect := range effects {
		if effect.RunID == "" {
			effect.RunID = event.RunID
		}
		effect.ID = effectIDForEvent(event.ID, i)
		stored, err := m.effects.Append(ctx, effect)
		if err != nil {
			return nil, err
		}
		persistedEffects = append(persistedEffects, stored.Effect.Clone())
	}

	if err := m.states.Save(ctx, next); err != nil {
		return nil, err
	}
	if stateChanged(current, next) {
		if err := m.recordObservableEvent(ctx, runtimeevent.EventStateChanged, event, StateChangedPayload{
			PreviousPhase:   current.Phase,
			NextPhase:       next.Phase,
			PreviousStep:    current.Step,
			NextStep:        next.Step,
			PreviousWaiting: cloneWaitingState(current.Waiting),
			NextWaiting:     cloneWaitingState(next.Waiting),
			PreviousError:   cloneErrorState(current.Error),
			NextError:       cloneErrorState(next.Error),
		}); err != nil {
			return nil, err
		}
	}
	if err := m.recordObservableEvent(ctx, runtimeevent.EventContextPersisted, event, ContextPersistedPayload{
		LastEventID:      next.LastEventID,
		EffectsPersisted: len(persistedEffects),
	}); err != nil {
		return nil, err
	}

	return &AdvanceResult{
		RunID:   event.RunID,
		State:   next,
		Effects: persistedEffects,
	}, nil
}

func (m *Machine) recordObservableEvent(ctx context.Context, eventType runtimeevent.Type, cause runtimeevent.Event, payload any) error {
	event, err := runtimeevent.New(eventType, payload,
		runtimeevent.WithRunID(cause.RunID),
		runtimeevent.WithSource("state.machine"),
		runtimeevent.WithCausationID(cause.ID),
	)
	if err != nil {
		return err
	}
	_, err = m.events.Append(ctx, event)
	return err
}

func stateChanged(previous RunState, next RunState) bool {
	return previous.Phase != next.Phase ||
		previous.Step != next.Step ||
		!reflect.DeepEqual(previous.Waiting, next.Waiting) ||
		!reflect.DeepEqual(previous.Error, next.Error)
}

func cloneWaitingState(waiting *WaitingState) *WaitingState {
	if waiting == nil {
		return nil
	}
	cloned := *waiting
	return &cloned
}

func cloneErrorState(errorState *ErrorState) *ErrorState {
	if errorState == nil {
		return nil
	}
	cloned := *errorState
	return &cloned
}

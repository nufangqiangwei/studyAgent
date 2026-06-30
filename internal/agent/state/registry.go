package state

import (
	"context"

	runtimeevent "agent/internal/event"
)

type ReducerRegistry struct {
	reducers []Reducer
}

func NewReducerRegistry() *ReducerRegistry {
	return &ReducerRegistry{}
}

func (r *ReducerRegistry) Register(reducer Reducer) {
	if reducer == nil {
		return
	}
	r.reducers = append(r.reducers, reducer)
}

func (r *ReducerRegistry) Reduce(ctx context.Context, s RunState, event runtimeevent.Event) (RunState, []Effect, error) {
	if r == nil {
		return s, nil, nil
	}

	for _, reducer := range r.reducers {
		if reducer.Match(ctx, s, event) {
			next, effects, err := reducer.Reduce(ctx, s, event)
			if err != nil {
				return s, nil, err
			}
			next.LastEventID = event.ID
			return next, effects, nil
		}
	}

	return s, nil, nil
}

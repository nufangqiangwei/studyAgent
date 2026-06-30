package state

import (
	"context"

	runtimeevent "agent/internal/event"
)

type Reducer interface {
	Match(ctx context.Context, state RunState, event runtimeevent.Event) bool
	Reduce(ctx context.Context, state RunState, event runtimeevent.Event) (RunState, []Effect, error)
}

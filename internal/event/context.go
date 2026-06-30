package event

import (
	"context"
	"fmt"
)

type dispatcherContextKey struct{}

func WithDispatcher(ctx context.Context, dispatcher *Dispatcher) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, dispatcherContextKey{}, dispatcher)
}

func DispatcherFromContext(ctx context.Context) (*Dispatcher, bool) {
	if ctx == nil {
		return nil, false
	}
	dispatcher, ok := ctx.Value(dispatcherContextKey{}).(*Dispatcher)
	return dispatcher, ok && dispatcher != nil
}

func NewFromContext(ctx context.Context, eventType Type, payload any, options ...EventOption) (Event, error) {
	dispatcher, ok := DispatcherFromContext(ctx)
	if !ok {
		return Event{}, fmt.Errorf("event dispatcher is not configured in context")
	}
	return dispatcher.NewEvent(eventType, payload, options...)
}

func Emit(ctx context.Context, event Event) (DispatchResult, error) {
	dispatcher, ok := DispatcherFromContext(ctx)
	if !ok {
		return DispatchResult{}, fmt.Errorf("event dispatcher is not configured in context")
	}
	return dispatcher.Emit(ctx, event)
}

func EmitNew(ctx context.Context, eventType Type, payload any, options ...EventOption) (DispatchResult, error) {
	dispatcher, ok := DispatcherFromContext(ctx)
	if !ok {
		return DispatchResult{}, fmt.Errorf("event dispatcher is not configured in context")
	}
	event, err := dispatcher.NewEvent(eventType, payload, options...)
	if err != nil {
		return DispatchResult{}, err
	}
	return dispatcher.Emit(ctx, event)
}

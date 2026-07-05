package eventbus

import (
	"context"
	"fmt"
	"strings"
)

type Handler interface {
	HandleEvent(ctx context.Context, event Event) error
}

type HandlerFunc func(ctx context.Context, event Event) error

func (f HandlerFunc) HandleEvent(ctx context.Context, event Event) error {
	if f == nil {
		return fmt.Errorf("event handler is nil")
	}
	return f(ctx, event)
}

type Subscription struct {
	ID       string `json:"id"`
	Filter   Filter `json:"filter"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

type SubscribeOption func(*subscribeConfig)

type subscribeConfig struct {
	id       string
	readOnly bool
}

func WithSubscriptionID(id string) SubscribeOption {
	return func(config *subscribeConfig) {
		config.id = id
	}
}

func WithReadOnly() SubscribeOption {
	return func(config *subscribeConfig) {
		config.readOnly = true
	}
}

type registeredSubscription struct {
	subscription Subscription
	handler      Handler
	order        int
}

func validateSubscription(sub Subscription, handler Handler) error {
	if strings.TrimSpace(sub.ID) == "" {
		return fmt.Errorf("subscription id is required")
	}
	if handler == nil {
		return fmt.Errorf("subscription %q: handler is required", sub.ID)
	}
	return nil
}

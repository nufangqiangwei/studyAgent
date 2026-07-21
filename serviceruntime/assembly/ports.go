package assembly

import (
	"agent/serviceruntime/contract"
	"context"
)

// MessageIngress is the durable external message entry exposed while the
// runtime object graph is assembled. Implementations may accept messages while
// live delivery is paused, but must not execute a Service directly.
type MessageIngress interface {
	Send(ctx context.Context, message contract.Message) error
}

type MessageIngressFunc func(ctx context.Context, message contract.Message) error

func (f MessageIngressFunc) Send(ctx context.Context, message contract.Message) error {
	return f(ctx, message)
}

// RuntimePorts contains generic runtime infrastructure that an explicitly
// installed module may bind to. It deliberately contains no business module
// types.
type RuntimePorts struct {
	Ingress MessageIngress
	IDs     contract.IDGenerator
	Clock   contract.Clock
}

// RuntimeBinder participates in object-graph assembly after the generic bus is
// created and before any Service activation or recovery begins.
type RuntimeBinder interface {
	BindRuntime(ports RuntimePorts) error
}

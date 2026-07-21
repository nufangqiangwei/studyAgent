package assembly

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"context"
)

const SystemOperationDeclareInstance = "service.instance.declare"

// ControlRejection is a stable, non-retryable denial returned by a Runtime
// control-plane operation. Infrastructure/storage errors are returned without
// this marker so the persisted system Effect can retry them.
type ControlRejection struct {
	Code  string
	Cause error
}

func (e *ControlRejection) Error() string {
	if e == nil || e.Cause == nil {
		return "runtime control operation rejected"
	}
	return e.Cause.Error()
}

func (e *ControlRejection) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func RejectControl(code string, cause error) error {
	return &ControlRejection{Code: code, Cause: cause}
}

// InstanceControl is the narrow, privileged Runtime control-plane port used
// by built-in system-call handlers. caller is the durable source address and
// is authorized against its registered ServiceDefinition.
type InstanceControl interface {
	Declare(ctx context.Context, caller contract.ServiceAddress, declaration instance.Declaration) (instance.Record, error)
}

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
	RuntimeID    contract.RuntimeID
	PlanRevision contract.PlanRevision
	Ingress      MessageIngress
	Instances    InstanceControl
	Artifacts    artifact.Store
	IDs          contract.IDGenerator
	Clock        contract.Clock
}

// RuntimeBinder participates in object-graph assembly after the generic bus is
// created and before any Service activation or recovery begins.
type RuntimeBinder interface {
	BindRuntime(ports RuntimePorts) error
}

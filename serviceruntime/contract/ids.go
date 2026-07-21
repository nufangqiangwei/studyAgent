package contract

import (
	"context"
	"strings"
	"time"
)

type RuntimeID string
type PlanRevision string
type ServiceType string
type ServiceAddress string
type ServiceInstanceID string
type MailboxID string
type StreamID string
type MessageType string
type EventType string
type EffectType string

type ComponentRef struct {
	Type    ServiceType `json:"type" yaml:"type"`
	Version string      `json:"version" yaml:"version"`
}

func (r ComponentRef) String() string {
	if strings.TrimSpace(r.Version) == "" {
		return string(r.Type)
	}
	return string(r.Type) + "@" + r.Version
}

func (r ComponentRef) Valid() bool {
	return strings.TrimSpace(string(r.Type)) != "" && strings.TrimSpace(r.Version) != ""
}

type SchemaRef struct {
	Name    string `json:"name" yaml:"name"`
	Version int    `json:"version" yaml:"version"`
}

func (r SchemaRef) Empty() bool {
	return strings.TrimSpace(r.Name) == ""
}

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	New(kind string) (string, error)
	Derive(kind string, parts ...string) string
}

type RuntimeEventType string

const (
	RuntimeStateChanged      RuntimeEventType = "runtime.state_changed"
	RuntimeMessageClaimed    RuntimeEventType = "runtime.message_claimed"
	RuntimeServiceHandled    RuntimeEventType = "runtime.service_handled"
	RuntimeCommitCompleted   RuntimeEventType = "runtime.commit_completed"
	RuntimeDeliveryCompleted RuntimeEventType = "runtime.delivery_completed"
	RuntimeEffectReconciled  RuntimeEventType = "runtime.effect_reconciled"
	RuntimeRecoveryCompleted RuntimeEventType = "runtime.recovery_completed"

	RuntimeOperationFailed RuntimeEventType = "runtime.operation_failed"
)

type RuntimeEvent struct {
	ID             string            `json:"id"`
	Type           RuntimeEventType  `json:"type"`
	RuntimeID      RuntimeID         `json:"runtime_id"`
	PlanRevision   PlanRevision      `json:"plan_revision"`
	InstanceID     ServiceInstanceID `json:"instance_id,omitempty"`
	ServiceAddress ServiceAddress    `json:"service_address,omitempty"`
	MessageID      string            `json:"message_id,omitempty"`
	EffectID       string            `json:"effect_id,omitempty"`
	StreamID       StreamID          `json:"stream_id,omitempty"`
	Sequence       uint64            `json:"sequence,omitempty"`
	OccurredAt     time.Time         `json:"occurred_at"`
	Attributes     map[string]string `json:"attributes,omitempty"`
}

type RuntimeEventRecorder interface {
	RecordRuntimeEvent(ctx context.Context, event RuntimeEvent) error
}

package building

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"time"
)

type ServiceScope string

const (
	ScopeRuntimeSingleton ServiceScope = "runtime_singleton"
	ScopeMounted          ServiceScope = "mounted"
	ScopeVirtual          ServiceScope = "virtual"
)

func (s ServiceScope) Valid() bool {
	switch s {
	case ScopeRuntimeSingleton, ScopeMounted, ScopeVirtual:
		return true
	default:
		return false
	}
}

type ServiceDependency struct {
	Name          string                 `json:"name"`
	Required      bool                   `json:"required"`
	AcceptedTypes []contract.ServiceType `json:"accepted_types,omitempty"`
}

type MessageContract struct {
	Kind    contract.MessageKind `json:"kind"`
	Type    contract.MessageType `json:"type"`
	Version int                  `json:"version"`
}

type ServiceDefinition struct {
	Component       contract.ComponentRef
	Factory         service.Factory
	Consumes        []MessageContract
	Produces        []MessageContract
	Dependencies    []ServiceDependency
	EffectExecutors []string
	StateSchema     contract.SchemaRef
	ConfigSchema    contract.SchemaRef
	Scope           ServiceScope
}

type RuntimeSpec struct {
	ID       contract.RuntimeID    `json:"id" yaml:"id"`
	Revision contract.PlanRevision `json:"revision" yaml:"revision"`
}

type ServiceMount struct {
	Address      contract.ServiceAddress            `json:"address" yaml:"address"`
	Component    contract.ComponentRef              `json:"component" yaml:"component"`
	Config       json.RawMessage                    `json:"config,omitempty" yaml:"config,omitempty"`
	Dependencies map[string]contract.ServiceAddress `json:"dependencies,omitempty" yaml:"dependencies,omitempty"`
	Metadata     map[string]string                  `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type RouteManifest struct {
	Commands map[contract.MessageType]contract.ServiceAddress   `json:"commands,omitempty" yaml:"commands,omitempty"`
	Queries  map[contract.MessageType]contract.ServiceAddress   `json:"queries,omitempty" yaml:"queries,omitempty"`
	Events   map[contract.MessageType][]contract.ServiceAddress `json:"events,omitempty" yaml:"events,omitempty"`
}

type RecoveryPolicy struct {
	SnapshotEveryEvents uint64        `json:"snapshot_every_events" yaml:"snapshot_every_events"`
	InboxLease          time.Duration `json:"inbox_lease" yaml:"inbox_lease"`
	OutboxLease         time.Duration `json:"outbox_lease" yaml:"outbox_lease"`
	EffectLease         time.Duration `json:"effect_lease" yaml:"effect_lease"`
	ActivationLease     time.Duration `json:"activation_lease" yaml:"activation_lease"`
	MaxDeliveryAttempts int           `json:"max_delivery_attempts" yaml:"max_delivery_attempts"`
}

func (p RecoveryPolicy) WithDefaults() RecoveryPolicy {
	if p.SnapshotEveryEvents == 0 {
		p.SnapshotEveryEvents = 50
	}
	if p.InboxLease <= 0 {
		p.InboxLease = 30 * time.Second
	}
	if p.OutboxLease <= 0 {
		p.OutboxLease = 30 * time.Second
	}
	if p.EffectLease <= 0 {
		p.EffectLease = 2 * time.Minute
	}
	if p.ActivationLease <= 0 {
		p.ActivationLease = 2 * time.Minute
	}
	if p.MaxDeliveryAttempts <= 0 {
		p.MaxDeliveryAttempts = 8
	}
	return p
}

type RuntimeManifest struct {
	Runtime  RuntimeSpec    `json:"runtime" yaml:"runtime"`
	Services []ServiceMount `json:"services" yaml:"services"`
	Routes   RouteManifest  `json:"routes" yaml:"routes"`
	Recovery RecoveryPolicy `json:"recovery" yaml:"recovery"`
}

type SchemaValidator interface {
	Validate(ctx context.Context, schema contract.SchemaRef, value json.RawMessage) error
}

type DefinitionResolver interface {
	ResolveDefinition(ref contract.ComponentRef) (ServiceDefinition, bool)
}

type ValidationIssue struct {
	Path    string
	Code    string
	Message string
}

type CompileView struct {
	Manifest    RuntimeManifest
	Plan        *RuntimePlan
	Definitions DefinitionResolver
}

type PlanValidator interface {
	ValidatePlan(ctx context.Context, view CompileView) []ValidationIssue
}

type PlanValidatorFunc func(ctx context.Context, view CompileView) []ValidationIssue

func (f PlanValidatorFunc) ValidatePlan(ctx context.Context, view CompileView) []ValidationIssue {
	return f(ctx, view)
}

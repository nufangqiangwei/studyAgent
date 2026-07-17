package instance

import (
	"agent/serviceruntime/contract"
	"context"
	"fmt"
	"time"
)

type ServiceKind string

const (
	ServiceStatic  ServiceKind = "static"
	ServiceVirtual ServiceKind = "virtual"
)

type Lifecycle string

const (
	Declared   Lifecycle = "declared"
	Starting   Lifecycle = "starting"
	Active     Lifecycle = "active"
	Passivated Lifecycle = "passivated"
	Recovering Lifecycle = "recovering"
	Draining   Lifecycle = "draining"
	Terminated Lifecycle = "terminated"
	Failed     Lifecycle = "failed"
)

type Record struct {
	InstanceID contract.ServiceInstanceID `json:"instance_id"`
	Address    contract.ServiceAddress    `json:"address"`
	Kind       ServiceKind                `json:"kind"`

	DefinitionRef contract.ComponentRef `json:"definition_ref"`
	RuntimeID     contract.RuntimeID    `json:"runtime_id"`
	PlanRevision  contract.PlanRevision `json:"plan_revision"`

	ParentID contract.ServiceInstanceID `json:"parent_id,omitempty"`
	RootID   contract.ServiceInstanceID `json:"root_id,omitempty"`
	Depth    int                        `json:"depth,omitempty"`

	MailboxID     contract.MailboxID `json:"mailbox_id"`
	StateStreamID contract.StreamID  `json:"state_stream_id"`

	Lifecycle       Lifecycle `json:"lifecycle"`
	ActivationEpoch uint64    `json:"activation_epoch"`
	RecordVersion   uint64    `json:"record_version"`

	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	ActivatedAt  *time.Time `json:"activated_at,omitempty"`
	PassivatedAt *time.Time `json:"passivated_at,omitempty"`
	TerminatedAt *time.Time `json:"terminated_at,omitempty"`

	LastError string            `json:"last_error,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

func (r Record) Clone() Record {
	r.Metadata = contract.CloneStrings(r.Metadata)
	r.ActivatedAt = cloneTime(r.ActivatedAt)
	r.PassivatedAt = cloneTime(r.PassivatedAt)
	r.TerminatedAt = cloneTime(r.TerminatedAt)
	return r
}

func (r Record) Validate() error {
	if r.InstanceID == "" || r.Address == "" || r.MailboxID == "" || r.StateStreamID == "" {
		return fmt.Errorf("instance id, address, mailbox id and state stream id are required")
	}
	if !r.DefinitionRef.Valid() || r.RuntimeID == "" || r.PlanRevision == "" {
		return fmt.Errorf("definition, runtime id and plan revision are required")
	}
	if r.Kind != ServiceStatic && r.Kind != ServiceVirtual {
		return fmt.Errorf("service kind %q is invalid", r.Kind)
	}
	return nil
}

type Query struct {
	RuntimeID    contract.RuntimeID
	PlanRevision contract.PlanRevision
	Lifecycle    []Lifecycle
	Kind         *ServiceKind
}

type Store interface {
	Create(ctx context.Context, record Record) error
	Get(ctx context.Context, instanceID contract.ServiceInstanceID) (Record, bool, error)
	GetByAddress(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision, address contract.ServiceAddress) (Record, bool, error)
	CompareAndSwap(ctx context.Context, next Record, expectedRecordVersion uint64) error
	List(ctx context.Context, query Query) ([]Record, error)
}

type ActivationLease struct {
	InstanceID contract.ServiceInstanceID `json:"instance_id"`
	Epoch      uint64                     `json:"epoch"`
	OwnerID    string                     `json:"owner_id"`
	LeaseToken string                     `json:"lease_token"`
	AcquiredAt time.Time                  `json:"acquired_at"`
	LeaseUntil time.Time                  `json:"lease_until"`
}

type ActivationLeaseStore interface {
	Acquire(ctx context.Context, instanceID contract.ServiceInstanceID, ownerID string, duration time.Duration) (ActivationLease, error)
	Renew(ctx context.Context, lease ActivationLease, duration time.Duration) (ActivationLease, error)
	Release(ctx context.Context, lease ActivationLease) error
	Current(ctx context.Context, instanceID contract.ServiceInstanceID) (ActivationLease, bool, error)
}

func CanTransition(from, to Lifecycle) bool {
	if from == to {
		return true
	}
	allowed := map[Lifecycle]map[Lifecycle]bool{
		Declared:   {Starting: true, Recovering: true, Terminated: true, Failed: true},
		Starting:   {Active: true, Passivated: true, Failed: true},
		Active:     {Passivated: true, Recovering: true, Draining: true, Failed: true},
		Passivated: {Starting: true, Recovering: true, Draining: true, Terminated: true, Failed: true},
		Recovering: {Active: true, Passivated: true, Failed: true},
		Draining:   {Passivated: true, Terminated: true, Failed: true},
		Terminated: {},
		Failed:     {Recovering: true, Terminated: true},
	}
	return allowed[from][to]
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

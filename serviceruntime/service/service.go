package service

import (
	"agent/serviceruntime/contract"
	"context"
	"encoding/json"
)

type State struct {
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data,omitempty"`
}

func (s State) Clone() State {
	s.Data = contract.CloneRaw(s.Data)
	return s
}

type Descriptor struct {
	Component   contract.ComponentRef `json:"component"`
	StateSchema contract.SchemaRef    `json:"state_schema"`
}

type Init struct {
	RuntimeID     contract.RuntimeID         `json:"runtime_id"`
	PlanRevision  contract.PlanRevision      `json:"plan_revision"`
	InstanceID    contract.ServiceInstanceID `json:"instance_id"`
	Address       contract.ServiceAddress    `json:"address"`
	StateStreamID contract.StreamID          `json:"state_stream_id"`
	Config        json.RawMessage            `json:"config,omitempty"`
	Metadata      map[string]string          `json:"metadata,omitempty"`
}

type Service interface {
	Descriptor() Descriptor
	InitialState(ctx context.Context, input Init) (State, error)
	Handle(ctx context.Context, state State, message contract.Message) (Decision, error)
	Apply(state State, event contract.StoredEvent) (State, error)
}

type CreateRequest struct {
	RuntimeID    contract.RuntimeID
	PlanRevision contract.PlanRevision
	InstanceID   contract.ServiceInstanceID
	Address      contract.ServiceAddress
	Component    contract.ComponentRef
	Config       json.RawMessage
	Metadata     map[string]string
}

type Factory interface {
	Create(ctx context.Context, request CreateRequest) (Service, error)
}

type FactoryFunc func(ctx context.Context, request CreateRequest) (Service, error)

func (f FactoryFunc) Create(ctx context.Context, request CreateRequest) (Service, error) {
	return f(ctx, request)
}

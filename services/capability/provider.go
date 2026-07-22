package capability

import (
	"agent/serviceruntime/contract"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type CapabilityDescriptor struct {
	Ref                string               `json:"ref"`
	Version            string               `json:"version"`
	InputSchema        contract.SchemaRef   `json:"input_schema,omitempty"`
	OutputSchema       contract.SchemaRef   `json:"output_schema,omitempty"`
	RiskTags           []string             `json:"risk_tags,omitempty"`
	ProviderRef        string               `json:"provider_ref"`
	ExecutionKind      ExecutionKind        `json:"execution_kind"`
	ExecutorRef        string               `json:"executor_ref,omitempty"`
	EffectType         contract.EffectType  `json:"effect_type,omitempty"`
	CommandType        contract.MessageType `json:"command_type,omitempty"`
	CommandVersion     int                  `json:"command_version,omitempty"`
	ReplyType          contract.MessageType `json:"reply_type,omitempty"`
	ReplyVersion       int                  `json:"reply_version,omitempty"`
	DescriptorRevision string               `json:"descriptor_revision"`
}

func (d CapabilityDescriptor) Clone() CapabilityDescriptor {
	d.RiskTags = append([]string(nil), d.RiskTags...)
	return d
}

type CapabilityInvocation struct {
	CallID       string
	Caller       contract.ServiceAddress
	UserID       string
	Descriptor   CapabilityDescriptor
	Arguments    Arguments
	Deadline     *time.Time
	PlanRevision contract.PlanRevision
}

type EffectPlan struct {
	Type        contract.EffectType
	Version     int
	ExecutorRef string
	Payload     json.RawMessage
	Deadline    *time.Time
}

type ServiceCommandPlan struct {
	To           contract.ServiceAddress
	Type         contract.MessageType
	Version      int
	Payload      json.RawMessage
	ReplyType    contract.MessageType
	ReplyVersion int
	Deadline     *time.Time
}

type CapabilityExecutionPlan struct {
	Kind           ExecutionKind
	ExecutionKey   string
	Effect         *EffectPlan
	ServiceCommand *ServiceCommandPlan
}

type CapabilityProvider interface {
	Ref() string
	Describe() []CapabilityDescriptor
	Plan(ctx context.Context, input CapabilityInvocation) (CapabilityExecutionPlan, error)
}

type ProviderFunc struct {
	ProviderRef string
	Descriptors []CapabilityDescriptor
	PlanFunc    func(ctx context.Context, input CapabilityInvocation) (CapabilityExecutionPlan, error)
}

func (p ProviderFunc) Ref() string { return p.ProviderRef }

func (p ProviderFunc) Describe() []CapabilityDescriptor {
	values := make([]CapabilityDescriptor, len(p.Descriptors))
	for index, descriptor := range p.Descriptors {
		values[index] = descriptor.Clone()
	}
	return values
}

func (p ProviderFunc) Plan(ctx context.Context, input CapabilityInvocation) (CapabilityExecutionPlan, error) {
	if p.PlanFunc == nil {
		return CapabilityExecutionPlan{}, fmt.Errorf("capability provider %q has no planner", p.ProviderRef)
	}
	return p.PlanFunc(ctx, input)
}

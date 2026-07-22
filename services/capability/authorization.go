package capability

import (
	"agent/serviceruntime/contract"
	"context"
	"time"
)

type AuthorizationInput struct {
	RuntimeID    contract.RuntimeID
	PlanRevision contract.PlanRevision
	Caller       contract.ServiceAddress
	UserID       string
	Descriptor   CapabilityDescriptor
	Arguments    Arguments
	Deadline     *time.Time
}

type AuthorizationDecision struct {
	Decision      AuthorizationDecisionKind
	RuleRef       string
	ReasonCode    string
	RiskSummary   string
	ApprovalScope string
}

type AuthorizationEvaluator interface {
	Evaluate(input AuthorizationInput) (AuthorizationDecision, error)
}

type AuthorizationEvaluatorFunc func(input AuthorizationInput) (AuthorizationDecision, error)

func (f AuthorizationEvaluatorFunc) Evaluate(input AuthorizationInput) (AuthorizationDecision, error) {
	return f(input)
}

// ArgumentValidator validates the descriptor's declared schema without
// granting the Service access to another Service or a mutable Runtime store.
// Implementations must be deterministic for a fixed schema and arguments.
type ArgumentValidator interface {
	Validate(ctx context.Context, schema contract.SchemaRef, arguments Arguments) error
}

type ArgumentValidatorFunc func(ctx context.Context, schema contract.SchemaRef, arguments Arguments) error

func (f ArgumentValidatorFunc) Validate(ctx context.Context, schema contract.SchemaRef, arguments Arguments) error {
	return f(ctx, schema, arguments)
}

type VisibilityEvaluator interface {
	Visible(caller contract.ServiceAddress, userID string, descriptor CapabilityDescriptor) bool
}

type VisibilityEvaluatorFunc func(caller contract.ServiceAddress, userID string, descriptor CapabilityDescriptor) bool

func (f VisibilityEvaluatorFunc) Visible(caller contract.ServiceAddress, userID string, descriptor CapabilityDescriptor) bool {
	return f(caller, userID, descriptor)
}

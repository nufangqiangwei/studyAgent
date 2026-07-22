package capability

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"fmt"
	"sort"
	"time"

	"agent/services/approval"
)

type ServiceFactory struct {
	catalog           *Catalog
	evaluator         AuthorizationEvaluator
	validator         ArgumentValidator
	visibility        VisibilityEvaluator
	clock             contract.Clock
	terminalRetention time.Duration
	idempotencyWindow time.Duration
}

func (f ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	approvalAddress := request.Dependencies[ApprovalDependency]
	if approvalAddress == "" {
		return nil, fmt.Errorf("capability service requires resolved %q dependency", ApprovalDependency)
	}
	return &capabilityService{
		address: request.Address, approvalAddress: approvalAddress,
		schedulerAddress: request.Dependencies[SchedulerDependency],
		catalog:          f.catalog, evaluator: f.evaluator, validator: f.validator,
		visibility: f.visibility, clock: f.clock,
		terminalRetention: f.terminalRetention, idempotencyWindow: f.idempotencyWindow,
	}, nil
}

func Definition(factory service.Factory, catalog *Catalog) building.ServiceDefinition {
	consumes := []building.MessageContract{
		{Kind: contract.MessageCommand, Type: InvokeMessageType, Version: ProtocolVersion},
		{Kind: contract.MessageCommand, Type: CancelMessageType, Version: ProtocolVersion},
		{Kind: contract.MessageCommand, Type: PruneMessageType, Version: ProtocolVersion},
		{Kind: contract.MessageQuery, Type: GetMessageType, Version: ProtocolVersion},
		{Kind: contract.MessageQuery, Type: ListMessageType, Version: ProtocolVersion},
		{Kind: contract.MessageEvent, Type: approval.ResolvedEventType, Version: approval.ProtocolVersion},
		{Kind: contract.MessageEvent, Type: approval.CancelledEventType, Version: approval.ProtocolVersion},
		{Kind: contract.MessageEvent, Type: approval.ExpiredEventType, Version: approval.ProtocolVersion},
		{Kind: contract.MessageReply, Type: approval.ResponseMessageType, Version: approval.ProtocolVersion},
		{Kind: contract.MessageEvent, Type: ExecutionCompletedMessageType, Version: ProtocolVersion},
		{Kind: contract.MessageEvent, Type: ExecutionFailedMessageType, Version: ProtocolVersion},
	}
	produces := []building.MessageContract{
		{Kind: contract.MessageReply, Type: ResultMessageType, Version: ProtocolVersion},
		{Kind: contract.MessageCommand, Type: approval.RequestMessageType, Version: approval.ProtocolVersion},
		{Kind: contract.MessageCommand, Type: approval.CancelMessageType, Version: approval.ProtocolVersion},
	}
	executors := make(map[string]struct{})
	seenConsumes := make(map[string]struct{})
	seenProduces := make(map[string]struct{})
	for _, value := range consumes {
		seenConsumes[contractKey(value)] = struct{}{}
	}
	for _, value := range produces {
		seenProduces[contractKey(value)] = struct{}{}
	}
	for _, descriptor := range catalog.Descriptors() {
		switch descriptor.ExecutionKind {
		case ExecutionEffect:
			executors[descriptor.ExecutorRef] = struct{}{}
		case ExecutionServiceCommand:
			produced := building.MessageContract{Kind: contract.MessageCommand, Type: descriptor.CommandType, Version: descriptor.CommandVersion}
			if _, exists := seenProduces[contractKey(produced)]; !exists {
				seenProduces[contractKey(produced)] = struct{}{}
				produces = append(produces, produced)
			}
			consumed := building.MessageContract{Kind: contract.MessageReply, Type: descriptor.ReplyType, Version: descriptor.ReplyVersion}
			if _, exists := seenConsumes[contractKey(consumed)]; !exists {
				seenConsumes[contractKey(consumed)] = struct{}{}
				consumes = append(consumes, consumed)
			}
		}
	}
	executorRefs := make([]string, 0, len(executors))
	for ref := range executors {
		executorRefs = append(executorRefs, ref)
	}
	sort.Strings(executorRefs)
	return building.ServiceDefinition{
		Component: Component, Factory: factory, Scope: building.ScopeRuntimeSingleton,
		StateSchema: StateSchema, Consumes: consumes, Produces: produces,
		Dependencies: []building.ServiceDependency{
			{Name: ApprovalDependency, Required: true, AcceptedTypes: []contract.ServiceType{approval.Component.Type}},
			{Name: SchedulerDependency, Required: false},
		},
		EffectExecutors: executorRefs,
	}
}

func contractKey(value building.MessageContract) string {
	return string(value.Kind) + "\x00" + string(value.Type) + "\x00" + fmt.Sprint(value.Version)
}

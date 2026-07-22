package approval

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"fmt"
)

type ServiceFactory struct {
	clock             contract.Clock
	trustedRequesters map[contract.ServiceAddress]struct{}
}

func (f ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	interaction := request.Dependencies[InteractionDependency]
	if interaction == "" {
		return nil, fmt.Errorf("approval service requires resolved %q dependency", InteractionDependency)
	}
	trusted := make(map[contract.ServiceAddress]struct{}, len(f.trustedRequesters))
	for address := range f.trustedRequesters {
		trusted[address] = struct{}{}
	}
	return &approvalService{
		address: request.Address, interaction: interaction,
		scheduler: request.Dependencies[SchedulerDependency],
		clock:     f.clock, trustedRequesters: trusted,
	}, nil
}

func Definition(factory service.Factory) building.ServiceDefinition {
	return building.ServiceDefinition{
		Component: Component, Factory: factory,
		Scope: building.ScopeRuntimeSingleton, StateSchema: StateSchema,
		Dependencies: []building.ServiceDependency{
			{Name: InteractionDependency, Required: true},
			{Name: SchedulerDependency, Required: false},
		},
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: RequestMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: ResolveMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: CancelMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: ExpireMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageQuery, Type: GetMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageQuery, Type: ListPendingMessageType, Version: ProtocolVersion},
		},
		Produces: []building.MessageContract{
			{Kind: contract.MessageReply, Type: ResponseMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: RequestedEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: ResolvedEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: CancelledEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: ExpiredEventType, Version: ProtocolVersion},
		},
	}
}

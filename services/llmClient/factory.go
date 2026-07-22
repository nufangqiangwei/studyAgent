package llmClient

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
)

type ServiceFactory struct{}

func (ServiceFactory) Create(context.Context, service.CreateRequest) (service.Service, error) {
	return &modelService{}, nil
}

func Definition(factory service.Factory) building.ServiceDefinition {
	return building.ServiceDefinition{
		Component:   Component,
		Factory:     factory,
		Scope:       building.ScopeRuntimeSingleton,
		StateSchema: StateSchema,
		Consumes: []building.MessageContract{{
			Kind: contract.MessageCommand, Type: CompleteMessageType, Version: ProtocolVersion,
		}},
		Produces: []building.MessageContract{{
			Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion,
		}},
		EffectExecutors: []string{CompleteExecutorRef},
	}
}

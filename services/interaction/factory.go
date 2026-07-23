package interaction

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/agent"
	"agent/services/approval"
	"context"
	"encoding/json"
	"fmt"
)

type ServiceFactory struct {
	clock contract.Clock
}

func (f ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	var config serviceConfig
	if err := json.Unmarshal(request.Config, &config); err != nil {
		return nil, fmt.Errorf("decode interaction service config: %w", err)
	}
	if config.AgentAddress == "" {
		return nil, fmt.Errorf("interaction service requires an Agent address")
	}
	return &interactionService{address: request.Address, agentAddress: config.AgentAddress, clock: f.clock}, nil
}

func Definition(factory service.Factory) building.ServiceDefinition {
	return building.ServiceDefinition{
		Component: Component, Factory: factory, Scope: building.ScopeRuntimeSingleton, StateSchema: StateSchema,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: SubmitMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion},
			{Kind: contract.MessageEvent, Type: approval.RequestedEventType, Version: approval.ProtocolVersion},
		},
		Produces: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: agent.ExecuteMessageType, Version: agent.ProtocolVersion},
		},
		EffectExecutors: []string{PresentExecutorRef},
	}
}

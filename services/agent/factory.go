package agent

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/capability"
	"agent/services/llmClient"
	"context"
	"encoding/json"
	"fmt"
)

type ServiceFactory struct {
	clock contract.Clock
}

func (f ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	modelAddress := request.Dependencies[ModelDependency]
	capabilityAddress := request.Dependencies[CapabilityDependency]
	if modelAddress == "" || capabilityAddress == "" {
		return nil, fmt.Errorf("agent service requires resolved model and capability dependencies")
	}
	if request.Artifacts == nil {
		return nil, fmt.Errorf("agent service requires an artifact reader")
	}
	var spec AgentSpec
	if err := json.Unmarshal(request.Config, &spec); err != nil {
		return nil, fmt.Errorf("decode agent spec: %w", err)
	}
	spec = spec.withDefaults()
	if err := spec.validate(); err != nil {
		return nil, err
	}
	return &agentService{
		address: request.Address, modelAddress: modelAddress, capabilityAddress: capabilityAddress,
		spec: spec, artifacts: request.Artifacts, clock: f.clock,
	}, nil
}

func Definition(factory service.Factory) building.ServiceDefinition {
	return building.ServiceDefinition{
		Component: Component, Factory: factory, Scope: building.ScopeRuntimeSingleton,
		StateSchema: StateSchema,
		Dependencies: []building.ServiceDependency{
			{Name: ModelDependency, Required: true, AcceptedTypes: []contract.ServiceType{llmClient.Component.Type}},
			{Name: CapabilityDependency, Required: true, AcceptedTypes: []contract.ServiceType{capability.Component.Type}},
		},
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: ExecuteMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: CancelMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageQuery, Type: GetMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageReply, Type: llmClient.CompletedMessageType, Version: llmClient.ProtocolVersion},
			{Kind: contract.MessageReply, Type: capability.ResultMessageType, Version: capability.ProtocolVersion},
			{Kind: contract.MessageEvent, Type: ArtifactPreparedMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: ArtifactFailedMessageType, Version: ProtocolVersion},
		},
		Produces: []building.MessageContract{
			{Kind: contract.MessageQuery, Type: capability.ListMessageType, Version: capability.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: llmClient.CompleteMessageType, Version: llmClient.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: capability.InvokeMessageType, Version: capability.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: capability.CancelMessageType, Version: capability.ProtocolVersion},
			{Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageReply, Type: StatusMessageType, Version: ProtocolVersion},
		},
		EffectExecutors: []string{PrepareArtifactExecutorRef},
	}
}

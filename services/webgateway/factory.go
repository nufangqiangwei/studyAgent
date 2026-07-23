package webgateway

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	runtimesystem "agent/serviceruntime/system"
	"agent/services/task"
	"context"
	"fmt"
)

type ServiceFactory struct {
	clock        contract.Clock
	defaultAgent contract.ServiceAddress
}

func (f ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	if request.Component != Component {
		return nil, fmt.Errorf("web gateway factory received component %q", request.Component.String())
	}
	if request.InstanceID == "" || request.Address == "" {
		return nil, fmt.Errorf("web gateway service requires instance id and address")
	}
	return &webGatewayService{
		address: request.Address, instanceID: request.InstanceID,
		clock: f.clock, defaultAgent: f.defaultAgent,
	}, nil
}

func Definition(factory service.Factory) building.ServiceDefinition {
	return building.ServiceDefinition{
		Component: Component, Factory: factory, Scope: building.ScopeRuntimeSingleton, StateSchema: StateSchema,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: CreateTaskMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: GetTaskMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageReply, Type: runtimesystem.ResultMessageType, Version: runtimesystem.CallVersion},
			{Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion},
		},
		Produces: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: runtimesystem.CallMessageType, Version: runtimesystem.CallVersion},
			{Kind: contract.MessageCommand, Type: task.CreateMessageType, Version: task.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: task.MarkReadyMessageType, Version: task.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: task.AssignMessageType, Version: task.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: task.StartMessageType, Version: task.ProtocolVersion},
			{Kind: contract.MessageQuery, Type: task.GetMessageType, Version: task.ProtocolVersion},
		},
		EffectExecutors:  []string{PresentationExecutorRef},
		SystemOperations: []string{assembly.SystemOperationDeclareInstance},
	}
}

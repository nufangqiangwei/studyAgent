package task

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"agent/services/agent"
	"context"
	"fmt"
)

type ServiceFactory struct {
	clock contract.Clock
}

func (f ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	if request.Component != Component {
		return nil, fmt.Errorf("task factory received component %q", request.Component.String())
	}
	if request.InstanceID == "" || request.Address == "" {
		return nil, fmt.Errorf("task service requires instance id and address")
	}
	return &taskService{address: request.Address, instanceID: request.InstanceID, clock: f.clock}, nil
}

func Definition(factory service.Factory) building.ServiceDefinition {
	return building.ServiceDefinition{
		Component: Component, Factory: factory, Scope: building.ScopeVirtual, StateSchema: StateSchema,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: CreateMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: MarkReadyMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: AssignMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: StartMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: SuspendMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: ResumeMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: RetryMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: CancelMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageQuery, Type: GetMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: ExecutionWaitingMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: ExecutionResumedMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageReply, Type: agent.CompletedMessageType, Version: agent.ProtocolVersion},
		},
		Produces: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: agent.ExecuteMessageType, Version: agent.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: agent.CancelMessageType, Version: agent.ProtocolVersion},
			{Kind: contract.MessageReply, Type: StatusMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: StatusChangedEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: CompletedEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: FailedEventType, Version: ProtocolVersion},
			{Kind: contract.MessageEvent, Type: CancelledEventType, Version: ProtocolVersion},
		},
	}
}

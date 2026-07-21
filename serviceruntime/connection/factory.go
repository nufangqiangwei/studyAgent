package connection

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"fmt"
)

type ServiceFactory struct{ module *Module }

func (f *ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	if f == nil || f.module == nil {
		return nil, fmt.Errorf("connection service factory is not configured")
	}
	ingress, ids, clock, err := f.module.runtimeDependencies()
	if err != nil {
		return nil, err
	}
	supervisor := newSupervisor(supervisorOptions{
		Request: request, Drivers: f.module.drivers, Ingress: ingress,
		IDs: ids, Clock: clock, Resources: f.module.resources,
	})
	return newManager(request, ids, clock, supervisor), nil
}

func Definition(factory service.Factory) building.ServiceDefinition {
	return building.ServiceDefinition{
		Component: ManagerComponent,
		Factory:   factory,
		Scope:     building.ScopeRuntimeSingleton,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: OpenMessageType, Version: 1},
			{Kind: contract.MessageCommand, Type: SendMessageType, Version: 1},
			{Kind: contract.MessageCommand, Type: CloseMessageType, Version: 1},
			{Kind: contract.MessageQuery, Type: GetMessageType, Version: 1},
			{Kind: contract.MessageQuery, Type: ListMessageType, Version: 1},
			{Kind: contract.MessageEvent, Type: DriverOpenedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: DriverFrameEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: DriverClosedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: DriverErrorEventType, Version: 1},
		},
		Produces: []building.MessageContract{
			{Kind: contract.MessageReply, Type: ReplyMessageType, Version: 1},
			{Kind: contract.MessageEvent, Type: OpenedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: MessageReceivedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: ClosedEventType, Version: 1},
			{Kind: contract.MessageEvent, Type: ErrorEventType, Version: 1},
		},
		EffectExecutors: []string{OpenExecutorRef, SendExecutorRef, CloseExecutorRef},
	}
}

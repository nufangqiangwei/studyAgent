package connection

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"fmt"
	"sync"
)

type factoryKey struct {
	runtime  contract.RuntimeID
	revision contract.PlanRevision
}

// ServiceFactory is registered while the plan is compiled and is bound to the
// concrete runtime-scoped Manager once Builder has assembled storage and bus.
type ServiceFactory struct {
	mu       sync.RWMutex
	managers map[factoryKey]*Manager
}

func NewServiceFactory() *ServiceFactory {
	return &ServiceFactory{managers: make(map[factoryKey]*Manager)}
}

func (f *ServiceFactory) Bind(runtimeID contract.RuntimeID, revision contract.PlanRevision, manager *Manager) {
	if f == nil || manager == nil {
		return
	}
	f.mu.Lock()
	f.managers[factoryKey{runtime: runtimeID, revision: revision}] = manager
	f.mu.Unlock()
}

func (f *ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	if f == nil {
		return nil, fmt.Errorf("connection manager service factory is nil")
	}
	f.mu.RLock()
	manager := f.managers[factoryKey{runtime: request.RuntimeID, revision: request.PlanRevision}]
	f.mu.RUnlock()
	if manager == nil {
		return nil, fmt.Errorf("connection manager is not bound for runtime %q plan %q", request.RuntimeID, request.PlanRevision)
	}
	return manager, nil
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
		},
		Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: ReplyMessageType, Version: 1}},
	}
}

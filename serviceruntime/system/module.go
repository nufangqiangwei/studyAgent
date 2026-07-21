package system

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/service"
	"context"
	"fmt"
	"sync"
)

type Registrar interface {
	RegisterService(building.ServiceDefinition) error
	RegisterEffect(effect.Spec) error
	RegisterRuntimeBinder(assembly.RuntimeBinder) error
}

type runtimeKey struct {
	runtimeID contract.RuntimeID
	revision  contract.PlanRevision
}

type Module struct {
	mu       sync.RWMutex
	bindings map[runtimeKey]runtimeBinding
}

func NewModule() *Module {
	return &Module{bindings: make(map[runtimeKey]runtimeBinding)}
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("system runtime module and registrar are required")
	}
	if err := registrar.RegisterService(building.ServiceDefinition{
		Component: Component,
		Scope:     building.ScopeRuntimeSingleton,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &runtimeService{}, nil
		}),
		Consumes:        []building.MessageContract{{Kind: contract.MessageCommand, Type: CallMessageType, Version: CallVersion}},
		Produces:        []building.MessageContract{{Kind: contract.MessageReply, Type: ResultMessageType, Version: CallVersion}},
		EffectExecutors: []string{ExecutorRef},
	}); err != nil {
		return err
	}
	if err := registrar.RegisterEffect(effect.Spec{
		Ref: ExecutorRef, Type: EffectType,
		Executor:   effect.ExecutorFunc(m.executeEffect),
		Reconciler: effect.ReconcilerFunc(m.reconcileEffect),
	}); err != nil {
		return err
	}
	return registrar.RegisterRuntimeBinder(m)
}

func (m *Module) BindRuntime(ports assembly.RuntimePorts) error {
	if m == nil || ports.RuntimeID == "" || ports.PlanRevision == "" || ports.Instances == nil || ports.Ingress == nil || ports.IDs == nil {
		return fmt.Errorf("system runtime binding requires runtime identity, ingress, ids and instance control")
	}
	key := runtimeKey{runtimeID: ports.RuntimeID, revision: ports.PlanRevision}
	m.mu.Lock()
	m.bindings[key] = runtimeBinding{control: ports.Instances, ingress: ports.Ingress, ids: ports.IDs}
	m.mu.Unlock()
	return nil
}

func (m *Module) resolve(runtimeID contract.RuntimeID, revision contract.PlanRevision) (runtimeBinding, bool) {
	if m == nil {
		return runtimeBinding{}, false
	}
	m.mu.RLock()
	binding, ok := m.bindings[runtimeKey{runtimeID: runtimeID, revision: revision}]
	m.mu.RUnlock()
	return binding, ok
}

func Mount() building.ServiceMount {
	return building.ServiceMount{Address: Address, Component: Component}
}

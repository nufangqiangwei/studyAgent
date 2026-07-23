package webgateway

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"fmt"
)

type Registrar interface {
	RegisterService(building.ServiceDefinition) error
	RegisterEffect(effect.Spec) error
}

type ModuleOptions struct {
	Presenter Presenter
	Clock     contract.Clock
}

type Module struct {
	presenter Presenter
	factory   ServiceFactory
}

func NewModule(options ModuleOptions) (*Module, error) {
	if options.Presenter == nil {
		return nil, fmt.Errorf("web gateway presenter is required")
	}
	return &Module{
		presenter: options.Presenter,
		factory:   ServiceFactory{clock: options.Clock},
	}, nil
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("web gateway module and registrar are required")
	}
	if err := registrar.RegisterEffect(effect.Spec{
		Ref: PresentationExecutorRef, Type: PresentationEffectType,
		Executor: effect.ExecutorFunc(m.executePresentation), Reconciler: effect.ReconcilerFunc(m.reconcilePresentation),
	}); err != nil {
		return err
	}
	return registrar.RegisterService(Definition(m.factory))
}

func (m *Module) Mount(address contract.ServiceAddress) building.ServiceMount {
	if address == "" {
		address = DefaultAddress
	}
	return building.ServiceMount{Address: address, Component: Component}
}

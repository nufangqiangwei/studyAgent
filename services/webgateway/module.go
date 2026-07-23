package webgateway

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"encoding/json"
	"fmt"
	"strings"
)

const serviceConfigVersion = 1

type serviceConfig struct {
	Version      int                     `json:"version"`
	DefaultAgent contract.ServiceAddress `json:"default_agent"`
}

type Registrar interface {
	RegisterService(building.ServiceDefinition) error
	RegisterEffect(effect.Spec) error
}

type ModuleOptions struct {
	Presenter    Presenter
	Clock        contract.Clock
	DefaultAgent contract.ServiceAddress
}

type Module struct {
	presenter Presenter
	factory   ServiceFactory
	config    json.RawMessage
}

func NewModule(options ModuleOptions) (*Module, error) {
	if options.Presenter == nil {
		return nil, fmt.Errorf("web gateway presenter is required")
	}
	options.DefaultAgent = contract.ServiceAddress(strings.TrimSpace(string(options.DefaultAgent)))
	if options.DefaultAgent == "" {
		return nil, fmt.Errorf("web gateway default agent address is required")
	}
	config, err := json.Marshal(serviceConfig{
		Version: serviceConfigVersion, DefaultAgent: options.DefaultAgent,
	})
	if err != nil {
		return nil, fmt.Errorf("encode web gateway service config: %w", err)
	}
	return &Module{
		presenter: options.Presenter,
		factory:   ServiceFactory{clock: options.Clock},
		config:    config,
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
	return building.ServiceMount{
		Address: address, Component: Component, Config: contract.CloneRaw(m.config),
	}
}

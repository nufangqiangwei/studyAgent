package approval

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"fmt"
	"time"
)

type Registrar interface {
	RegisterService(building.ServiceDefinition) error
}

type ModuleOptions struct {
	Clock             contract.Clock
	TrustedRequesters []contract.ServiceAddress
}

type Module struct {
	factory ServiceFactory
}

func NewModule(options ModuleOptions) (*Module, error) {
	if options.Clock == nil {
		options.Clock = systemClock{}
	}
	if len(options.TrustedRequesters) == 0 {
		options.TrustedRequesters = []contract.ServiceAddress{"capability.main"}
	}
	trusted := make(map[contract.ServiceAddress]struct{}, len(options.TrustedRequesters))
	for _, address := range options.TrustedRequesters {
		if clean(string(address)) == "" {
			return nil, fmt.Errorf("trusted approval requester address is required")
		}
		trusted[address] = struct{}{}
	}
	return &Module{factory: ServiceFactory{clock: options.Clock, trustedRequesters: trusted}}, nil
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("approval module and registrar are required")
	}
	return registrar.RegisterService(Definition(m.factory))
}

func (m *Module) Mount(address, interaction, scheduler contract.ServiceAddress) building.ServiceMount {
	if address == "" {
		address = DefaultAddress
	}
	dependencies := map[string]contract.ServiceAddress{InteractionDependency: interaction}
	if scheduler != "" {
		dependencies[SchedulerDependency] = scheduler
	}
	return building.ServiceMount{Address: address, Component: Component, Dependencies: dependencies}
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

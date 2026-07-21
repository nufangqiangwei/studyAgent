package connection

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"fmt"
	"sync"
)

type Registrar interface {
	RegisterService(definition building.ServiceDefinition) error
	RegisterEffect(spec effect.Spec) error
	RegisterRuntimeBinder(binder assembly.RuntimeBinder) error
}

type ModuleOptions struct {
	Drivers *Registry
	IDs     contract.IDGenerator
	Clock   contract.Clock
}

// Module owns all connection-specific factories, drivers, executors, and
// process resources. The generic Runtime sees it only through registration and
// RuntimeBinder ports.
type Module struct {
	drivers   *Registry
	resources *resourceDirectory
	factory   *ServiceFactory

	mu      sync.RWMutex
	ingress assembly.MessageIngress
	ids     contract.IDGenerator
	clock   contract.Clock
}

func NewModule(options ModuleOptions) *Module {
	if options.Drivers == nil {
		options.Drivers = NewRegistry()
	}
	module := &Module{
		drivers: options.Drivers, resources: newResourceDirectory(),
		ids: options.IDs, clock: options.Clock,
	}
	module.factory = &ServiceFactory{module: module}
	return module
}

func (m *Module) RegisterDriver(name string, driver Driver) error {
	if m == nil {
		return fmt.Errorf("connection module is nil")
	}
	return m.drivers.Register(name, driver)
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("connection module and registrar are required")
	}
	for _, spec := range m.effectSpecs() {
		if err := registrar.RegisterEffect(spec); err != nil {
			return err
		}
	}
	if err := registrar.RegisterService(Definition(m.factory)); err != nil {
		return err
	}
	return registrar.RegisterRuntimeBinder(m)
}

func (m *Module) Mount(address contract.ServiceAddress) building.ServiceMount {
	if address == "" {
		address = DefaultAddress
	}
	return building.ServiceMount{Address: address, Component: ManagerComponent}
}

func (m *Module) BindRuntime(ports assembly.RuntimePorts) error {
	if m == nil {
		return fmt.Errorf("connection module is nil")
	}
	if ports.Ingress == nil || ports.IDs == nil {
		return fmt.Errorf("connection module requires runtime ingress and id generator")
	}
	m.mu.Lock()
	m.ingress = ports.Ingress
	if m.ids == nil {
		m.ids = ports.IDs
	}
	if m.clock == nil {
		m.clock = ports.Clock
	}
	m.mu.Unlock()
	return nil
}

func (m *Module) runtimeDependencies() (assembly.MessageIngress, contract.IDGenerator, contract.Clock, error) {
	if m == nil {
		return nil, nil, nil, fmt.Errorf("connection module is nil")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.ingress == nil || m.ids == nil {
		return nil, nil, nil, fmt.Errorf("connection module is not bound to a runtime")
	}
	return m.ingress, m.ids, m.clock, nil
}

var _ assembly.RuntimeBinder = (*Module)(nil)

package llmClient

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"fmt"
	"sync"
)

type Registrar interface {
	RegisterService(building.ServiceDefinition) error
	RegisterEffect(effect.Spec) error
	RegisterRuntimeBinder(assembly.RuntimeBinder) error
}

type ModuleOption func(*Module) error

// WithClient installs a provider adapter or test double. When omitted, the
// module uses the HTTP adapter selected by Config.Provider.
func WithClient(client Client) ModuleOption {
	return func(module *Module) error {
		if client == nil {
			return fmt.Errorf("llm client is required")
		}
		module.client = client
		return nil
	}
}

type runtimeBinding struct {
	ingress   assembly.MessageIngress
	artifacts artifact.Store
	ids       contract.IDGenerator
}

type Module struct {
	config  Config
	client  Client
	factory ServiceFactory

	mu       sync.RWMutex
	bindings map[contract.RuntimeID]runtimeBinding
}

func NewModule(config Config, options ...ModuleOption) (*Module, error) {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	module := &Module{config: config, factory: ServiceFactory{}, bindings: make(map[contract.RuntimeID]runtimeBinding)}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(module); err != nil {
			return nil, err
		}
	}
	if module.client == nil {
		module.client = newHTTPCompletionClient(config)
	}
	return module, nil
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("llm client module and registrar are required")
	}
	if err := registrar.RegisterEffect(effect.Spec{
		Ref: CompleteExecutorRef, Type: CompleteEffectType,
		Executor:        effect.ExecutorFunc(m.executeEffect),
		Reconciler:      effect.ReconcilerFunc(m.reconcileEffect),
		TerminalFailure: effect.TerminalFailureNotifierFunc(m.notifyTerminalFailure),
	}); err != nil {
		return err
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
	return building.ServiceMount{Address: address, Component: Component}
}

func (m *Module) BindRuntime(ports assembly.RuntimePorts) error {
	if m == nil || ports.RuntimeID == "" || ports.Ingress == nil || ports.Artifacts == nil || ports.IDs == nil {
		return fmt.Errorf("llm client module requires runtime identity, ingress, artifact store, and id generator")
	}
	m.mu.Lock()
	m.bindings[ports.RuntimeID] = runtimeBinding{ingress: ports.Ingress, artifacts: ports.Artifacts, ids: ports.IDs}
	m.mu.Unlock()
	return nil
}

func (m *Module) resolve(runtimeID contract.RuntimeID) (runtimeBinding, bool) {
	if m == nil {
		return runtimeBinding{}, false
	}
	m.mu.RLock()
	binding, found := m.bindings[runtimeID]
	m.mu.RUnlock()
	return binding, found
}

var _ assembly.RuntimeBinder = (*Module)(nil)

package interaction

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/services/agent"
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type Registrar interface {
	RegisterService(building.ServiceDefinition) error
	RegisterEffect(effect.Spec) error
	RegisterPlanValidator(building.PlanValidator) error
	RegisterRuntimeBinder(assembly.RuntimeBinder) error
}

type ModuleOptions struct {
	Presenter      Presenter
	Clock          contract.Clock
	MaxOutputBytes int64
}

type runtimeBinding struct {
	artifacts artifact.Store
}

type serviceConfig struct {
	AgentAddress contract.ServiceAddress `json:"agent_address"`
}

type Module struct {
	presenter      Presenter
	maxOutputBytes int64
	factory        ServiceFactory

	mu       sync.RWMutex
	bindings map[contract.RuntimeID]runtimeBinding
}

func NewModule(options ModuleOptions) (*Module, error) {
	if options.Presenter == nil {
		return nil, fmt.Errorf("interaction presenter is required")
	}
	if options.MaxOutputBytes <= 0 {
		options.MaxOutputBytes = defaultMaxOutputBytes
	}
	return &Module{
		presenter: options.Presenter, maxOutputBytes: options.MaxOutputBytes,
		factory: ServiceFactory{clock: options.Clock}, bindings: make(map[contract.RuntimeID]runtimeBinding),
	}, nil
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("interaction module and registrar are required")
	}
	if err := registrar.RegisterEffect(effect.Spec{
		Ref: PresentExecutorRef, Type: PresentEffectType,
		Executor:   effect.ExecutorFunc(m.executePresentation),
		Reconciler: effect.ReconcilerFunc(m.reconcilePresentation),
	}); err != nil {
		return err
	}
	if err := registrar.RegisterService(Definition(m.factory)); err != nil {
		return err
	}
	if err := registrar.RegisterPlanValidator(building.PlanValidatorFunc(m.validatePlan)); err != nil {
		return err
	}
	return registrar.RegisterRuntimeBinder(m)
}

func (m *Module) Mount(address, agentAddress contract.ServiceAddress) building.ServiceMount {
	if address == "" {
		address = DefaultAddress
	}
	if agentAddress == "" {
		agentAddress = agent.DefaultAddress
	}
	config, _ := json.Marshal(serviceConfig{AgentAddress: agentAddress})
	return building.ServiceMount{Address: address, Component: Component, Config: config}
}

func (m *Module) BindRuntime(ports assembly.RuntimePorts) error {
	if m == nil || ports.RuntimeID == "" || ports.Artifacts == nil {
		return fmt.Errorf("interaction module requires runtime identity and artifact store")
	}
	m.mu.Lock()
	m.bindings[ports.RuntimeID] = runtimeBinding{artifacts: ports.Artifacts}
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

func (m *Module) validatePlan(_ context.Context, view building.CompileView) []building.ValidationIssue {
	var issues []building.ValidationIssue
	for index, mount := range view.Manifest.Services {
		if mount.Component != Component {
			continue
		}
		var config serviceConfig
		if err := json.Unmarshal(mount.Config, &config); err != nil {
			issues = append(issues, building.ValidationIssue{
				Path: fmt.Sprintf("services[%d].config", index),
				Code: "interaction_config", Message: "interaction service config is invalid",
			})
			continue
		}
		planned, found := view.Plan.Service(config.AgentAddress)
		if !found || planned.Component.Type != agent.Component.Type {
			issues = append(issues, building.ValidationIssue{
				Path: fmt.Sprintf("services[%d].config.agent_address", index),
				Code: "interaction_agent_contract", Message: "interaction service requires an Agent Service target",
			})
		}
	}
	return issues
}

var _ assembly.RuntimeBinder = (*Module)(nil)

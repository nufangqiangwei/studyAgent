package agent

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/services/capability"
	"agent/services/llmClient"
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

type runtimeBinding struct {
	ingress   assembly.MessageIngress
	artifacts artifact.Store
	ids       contract.IDGenerator
}

type Module struct {
	config  json.RawMessage
	factory ServiceFactory

	mu       sync.RWMutex
	bindings map[contract.RuntimeID]runtimeBinding
}

func NewModule(spec AgentSpec, clock contract.Clock) (*Module, error) {
	spec = spec.withDefaults()
	if err := spec.validate(); err != nil {
		return nil, err
	}
	config, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("encode agent spec: %w", err)
	}
	return &Module{
		config: config, factory: ServiceFactory{clock: clock},
		bindings: make(map[contract.RuntimeID]runtimeBinding),
	}, nil
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("agent module and registrar are required")
	}
	if err := registrar.RegisterEffect(effect.Spec{
		Ref: PrepareArtifactExecutorRef, Type: PrepareArtifactEffectType,
		Executor:        effect.ExecutorFunc(m.executeArtifactEffect),
		Reconciler:      effect.ReconcilerFunc(m.reconcileArtifactEffect),
		TerminalFailure: effect.TerminalFailureNotifierFunc(m.notifyArtifactTerminalFailure),
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

func (m *Module) Mount(address, modelAddress, capabilityAddress contract.ServiceAddress) building.ServiceMount {
	if address == "" {
		address = DefaultAddress
	}
	if modelAddress == "" {
		modelAddress = llmClient.DefaultAddress
	}
	if capabilityAddress == "" {
		capabilityAddress = capability.DefaultAddress
	}
	return building.ServiceMount{
		Address: address, Component: Component, Config: contract.CloneRaw(m.config),
		Dependencies: map[string]contract.ServiceAddress{
			ModelDependency: modelAddress, CapabilityDependency: capabilityAddress,
		},
	}
}

func (m *Module) BindRuntime(ports assembly.RuntimePorts) error {
	if m == nil || ports.RuntimeID == "" || ports.Ingress == nil || ports.Artifacts == nil || ports.IDs == nil {
		return fmt.Errorf("agent module requires runtime identity, durable ingress, artifact store, and id generator")
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

func (m *Module) validatePlan(_ context.Context, view building.CompileView) []building.ValidationIssue {
	var issues []building.ValidationIssue
	for index, mount := range view.Manifest.Services {
		if mount.Component != Component {
			continue
		}
		model, modelFound := view.Plan.Service(mount.Dependencies[ModelDependency])
		if !modelFound || model.Component.Type != llmClient.Component.Type {
			issues = append(issues, building.ValidationIssue{
				Path: fmt.Sprintf("services[%d].dependencies.%s", index, ModelDependency),
				Code: "agent_model_contract", Message: "agent service requires an LLM Client dependency",
			})
		}
		capabilities, capabilityFound := view.Plan.Service(mount.Dependencies[CapabilityDependency])
		if !capabilityFound || capabilities.Component.Type != capability.Component.Type {
			issues = append(issues, building.ValidationIssue{
				Path: fmt.Sprintf("services[%d].dependencies.%s", index, CapabilityDependency),
				Code: "agent_capability_contract", Message: "agent service requires a CapabilityService dependency",
			})
		}
	}
	return issues
}

package capability

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"agent/services/approval"
)

type Registrar interface {
	RegisterService(building.ServiceDefinition) error
	RegisterEffect(effect.Spec) error
	RegisterPlanValidator(building.PlanValidator) error
	RegisterRuntimeBinder(assembly.RuntimeBinder) error
}

type ModuleOptions struct {
	Evaluator         AuthorizationEvaluator
	ArgumentValidator ArgumentValidator
	Visibility        VisibilityEvaluator
	Clock             contract.Clock
	TerminalRetention time.Duration
	IdempotencyWindow time.Duration
}

type runtimeBinding struct {
	ingress assembly.MessageIngress
}

type Module struct {
	options ModuleOptions

	mu        sync.RWMutex
	providers []CapabilityProvider
	executors map[string]effect.Spec
	bindings  map[contract.RuntimeID]runtimeBinding
	frozen    bool
	catalog   *Catalog
}

func NewModule(options ModuleOptions) (*Module, error) {
	if options.Evaluator == nil {
		return nil, fmt.Errorf("capability authorization evaluator is required")
	}
	if options.Clock == nil {
		options.Clock = systemClock{}
	}
	if options.TerminalRetention <= 0 {
		options.TerminalRetention = 24 * time.Hour
	}
	if options.IdempotencyWindow <= 0 {
		options.IdempotencyWindow = 7 * 24 * time.Hour
	}
	if options.IdempotencyWindow <= options.TerminalRetention {
		return nil, fmt.Errorf("capability idempotency window must exceed terminal record retention")
	}
	return &Module{
		options: options, executors: make(map[string]effect.Spec),
		bindings: make(map[contract.RuntimeID]runtimeBinding),
	}, nil
}

func (m *Module) RegisterProvider(provider CapabilityProvider) error {
	if m == nil || provider == nil {
		return fmt.Errorf("capability module and provider are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return fmt.Errorf("capability module is already registered")
	}
	m.providers = append(m.providers, provider)
	return nil
}

func (m *Module) RegisterExecutor(spec effect.Spec) error {
	if m == nil {
		return fmt.Errorf("capability module is nil")
	}
	if clean(spec.Ref) == "" || spec.Type == "" || spec.Executor == nil || spec.Reconciler == nil {
		return fmt.Errorf("capability executor ref, type, executor, and reconciler are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return fmt.Errorf("capability module is already registered")
	}
	if _, exists := m.executors[spec.Ref]; exists {
		return fmt.Errorf("capability executor %q is duplicated", spec.Ref)
	}
	m.executors[spec.Ref] = spec
	return nil
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("capability module and registrar are required")
	}
	m.mu.Lock()
	if m.frozen {
		m.mu.Unlock()
		return fmt.Errorf("capability module is already registered")
	}
	catalog, err := NewCatalog(append([]CapabilityProvider(nil), m.providers...))
	if err != nil {
		m.mu.Unlock()
		return err
	}
	for _, descriptor := range catalog.Descriptors() {
		if !descriptor.InputSchema.Empty() && m.options.ArgumentValidator == nil {
			m.mu.Unlock()
			return fmt.Errorf("capability %q declares an input schema without an argument validator", descriptorKey(descriptor.Ref, descriptor.Version))
		}
		if descriptor.ExecutionKind == ExecutionEffect {
			spec, found := m.executors[descriptor.ExecutorRef]
			if !found || spec.Type != descriptor.EffectType {
				m.mu.Unlock()
				return fmt.Errorf("capability %q references unknown or incompatible executor %q", descriptor.Ref, descriptor.ExecutorRef)
			}
		}
	}
	executors := make(map[string]effect.Spec, len(m.executors))
	for ref, spec := range m.executors {
		executors[ref] = spec
	}
	m.catalog, m.frozen = catalog, true
	m.mu.Unlock()
	usedExecutors := make(map[string]struct{})
	for _, descriptor := range catalog.Descriptors() {
		if descriptor.ExecutionKind == ExecutionEffect {
			usedExecutors[descriptor.ExecutorRef] = struct{}{}
		}
	}
	refs := make([]string, 0, len(usedExecutors))
	for ref := range usedExecutors {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	for _, ref := range refs {
		spec := executors[ref]
		wrapped := effect.Spec{
			Ref: spec.Ref, Type: spec.Type,
			Executor:        wrappedExecutor{module: m, delegate: spec.Executor},
			Reconciler:      wrappedReconciler{module: m, delegate: spec.Reconciler},
			TerminalFailure: wrappedTerminalFailure{module: m},
		}
		if err := registrar.RegisterEffect(wrapped); err != nil {
			return err
		}
	}
	factory := ServiceFactory{
		catalog: catalog, evaluator: m.options.Evaluator,
		validator: m.options.ArgumentValidator, visibility: m.options.Visibility,
		clock: m.options.Clock, terminalRetention: m.options.TerminalRetention,
		idempotencyWindow: m.options.IdempotencyWindow,
	}
	if err := registrar.RegisterService(Definition(factory, catalog)); err != nil {
		return err
	}
	if err := registrar.RegisterPlanValidator(building.PlanValidatorFunc(m.validatePlan)); err != nil {
		return err
	}
	return registrar.RegisterRuntimeBinder(m)
}

func (m *Module) Mount(address, approvalAddress, schedulerAddress contract.ServiceAddress) building.ServiceMount {
	if address == "" {
		address = DefaultAddress
	}
	if approvalAddress == "" {
		approvalAddress = approval.DefaultAddress
	}
	dependencies := map[string]contract.ServiceAddress{ApprovalDependency: approvalAddress}
	if schedulerAddress != "" {
		dependencies[SchedulerDependency] = schedulerAddress
	}
	return building.ServiceMount{Address: address, Component: Component, Dependencies: dependencies}
}

func (m *Module) BindRuntime(ports assembly.RuntimePorts) error {
	if m == nil || ports.RuntimeID == "" || ports.Ingress == nil {
		return fmt.Errorf("capability module requires runtime identity and durable ingress")
	}
	m.mu.Lock()
	m.bindings[ports.RuntimeID] = runtimeBinding{ingress: ports.Ingress}
	m.mu.Unlock()
	return nil
}

func (m *Module) resolveBinding(runtimeID contract.RuntimeID) (runtimeBinding, bool) {
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
		approvalAddress := mount.Dependencies[ApprovalDependency]
		planned, found := view.Plan.Service(approvalAddress)
		if !found || planned.Component.Type != approval.Component.Type {
			issues = append(issues, building.ValidationIssue{
				Path: fmt.Sprintf("services[%d].dependencies.%s", index, ApprovalDependency),
				Code: "approval_contract", Message: "capability service requires an ApprovalService dependency",
			})
		}
	}
	return issues
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

package activation

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/request"
	"agent/serviceruntime/service"
	"context"
	"fmt"
	"sync"
	"time"
)

type Activation struct {
	Instance instance.Record
	Lease    instance.ActivationLease
	Service  service.Service
	Requests *request.Client

	mu       sync.RWMutex
	state    service.State
	sequence uint64
	replayed int
}

func (a *Activation) ReplayedEvents() int {
	if a == nil {
		return 0
	}
	return a.replayed
}

func (a *Activation) Current() (service.State, uint64) {
	if a == nil {
		return service.State{}, 0
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.state.Clone(), a.sequence
}

func (a *Activation) CurrentLease() instance.ActivationLease {
	if a == nil {
		return instance.ActivationLease{}
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Lease
}

func (a *Activation) updateLease(lease instance.ActivationLease) {
	a.mu.Lock()
	a.Lease = lease
	a.mu.Unlock()
}

func (a *Activation) CommitState(state service.State, sequence uint64) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.state = state.Clone()
	a.sequence = sequence
	a.mu.Unlock()
}

type Activator interface {
	Activate(ctx context.Context, instanceID contract.ServiceInstanceID) (*Activation, error)
	Lookup(instanceID contract.ServiceInstanceID) (*Activation, bool)
	Passivate(ctx context.Context, instanceID contract.ServiceInstanceID) error
	Terminate(ctx context.Context, instanceID contract.ServiceInstanceID) error
}

type Manager struct {
	plan        *building.RuntimePlan
	definitions building.DefinitionResolver
	instances   instance.Store
	leases      instance.ActivationLeaseStore
	restorer    StateRestorer
	ownerID     string
	leaseTTL    time.Duration
	clock       contract.Clock
	requests    *request.ClientFactory
	opMu        sync.Mutex

	mu     sync.RWMutex
	active map[contract.ServiceInstanceID]*Activation
}

type Options struct {
	Plan        *building.RuntimePlan
	Definitions building.DefinitionResolver
	Instances   instance.Store
	Leases      instance.ActivationLeaseStore
	Restorer    StateRestorer
	OwnerID     string
	LeaseTTL    time.Duration
	Clock       contract.Clock
	Requests    *request.ClientFactory
}

func NewManager(options Options) (*Manager, error) {
	if options.Plan == nil || options.Definitions == nil || options.Instances == nil || options.Leases == nil || options.Restorer == nil {
		return nil, fmt.Errorf("activation manager requires plan, definitions, instance store, lease store and restorer")
	}
	if options.OwnerID == "" {
		return nil, fmt.Errorf("activation manager owner id is required")
	}
	if options.LeaseTTL <= 0 {
		options.LeaseTTL = options.Plan.Recovery().ActivationLease
	}
	return &Manager{
		plan: options.Plan, definitions: options.Definitions,
		instances: options.Instances, leases: options.Leases, restorer: options.Restorer,
		ownerID: options.OwnerID, leaseTTL: options.LeaseTTL, clock: options.Clock,
		requests: options.Requests,
		active:   make(map[contract.ServiceInstanceID]*Activation),
	}, nil
}

func (m *Manager) Lookup(instanceID contract.ServiceInstanceID) (*Activation, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.RLock()
	value, ok := m.active[instanceID]
	m.mu.RUnlock()
	return value, ok
}

func (m *Manager) Activate(ctx context.Context, instanceID contract.ServiceInstanceID) (*Activation, error) {
	if m == nil {
		return nil, fmt.Errorf("activation manager is nil")
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if existing, ok := m.Lookup(instanceID); ok {
		renewed, renewErr := m.leases.Renew(ctx, existing.CurrentLease(), m.leaseTTL)
		if renewErr == nil {
			existing.updateLease(renewed)
			return existing, nil
		}
		m.mu.Lock()
		delete(m.active, instanceID)
		m.mu.Unlock()
	}
	record, found, err := m.instances.Get(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("service instance %q not found", instanceID)
	}
	if record.Lifecycle == instance.Terminated || record.Lifecycle == instance.Draining {
		return nil, fmt.Errorf("service instance %q cannot activate from %q", instanceID, record.Lifecycle)
	}
	lease, err := m.leases.Acquire(ctx, instanceID, m.ownerID, m.leaseTTL)
	if err != nil {
		return nil, err
	}
	record, found, err = m.instances.Get(ctx, instanceID)
	if err != nil || !found {
		_ = m.leases.Release(ctx, lease)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("service instance %q disappeared during activation", instanceID)
	}
	starting := record.Clone()
	if record.Lifecycle == instance.Active || record.Lifecycle == instance.Failed || record.Lifecycle == instance.Recovering {
		starting.Lifecycle = instance.Recovering
	} else {
		starting.Lifecycle = instance.Starting
	}
	if err := m.instances.CompareAndSwap(ctx, starting, record.RecordVersion); err != nil {
		_ = m.leases.Release(ctx, lease)
		return nil, err
	}
	record, _, _ = m.instances.Get(ctx, instanceID)
	definition, ok := m.definitions.ResolveDefinition(record.DefinitionRef)
	if !ok {
		m.fail(ctx, record, lease, fmt.Errorf("service definition %q not found", record.DefinitionRef.String()))
		return nil, fmt.Errorf("service definition %q not found", record.DefinitionRef.String())
	}
	mount, mounted := m.plan.Service(record.Address)
	var config []byte
	if mounted {
		config = mount.Config
	}
	requestClient := m.requestClient(record.Address)
	target, err := definition.Factory.Create(ctx, service.CreateRequest{
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		InstanceID: record.InstanceID, Address: record.Address,
		Component: record.DefinitionRef, Config: contract.CloneRaw(config),
		Metadata: contract.CloneStrings(record.Metadata),
		Requests: requestClient,
	})
	if err != nil {
		m.fail(ctx, record, lease, err)
		return nil, err
	}
	if descriptor := target.Descriptor(); descriptor.Component != record.DefinitionRef {
		err = fmt.Errorf("factory returned component %q for instance definition %q", descriptor.Component.String(), record.DefinitionRef.String())
		m.fail(ctx, record, lease, err)
		return nil, err
	}
	restored, err := m.restorer.Restore(ctx, target, record, config)
	if err != nil {
		m.fail(ctx, record, lease, err)
		return nil, err
	}
	latest, found, err := m.instances.Get(ctx, instanceID)
	if err != nil || !found {
		_ = m.leases.Release(ctx, lease)
		return nil, fmt.Errorf("reload service instance after restore: %w", err)
	}
	latest.Lifecycle = instance.Active
	now := m.now()
	latest.ActivatedAt = &now
	if err := m.instances.CompareAndSwap(ctx, latest, latest.RecordVersion); err != nil {
		_ = m.leases.Release(ctx, lease)
		return nil, err
	}
	latest, _, _ = m.instances.Get(ctx, instanceID)
	activation := &Activation{Instance: latest, Lease: lease, Service: target, Requests: requestClient, state: restored.State.Clone(), sequence: restored.LastSequence, replayed: restored.ReplayedEvents}
	m.mu.Lock()
	if existing, ok := m.active[instanceID]; ok {
		m.mu.Unlock()
		_ = m.leases.Release(ctx, lease)
		return existing, nil
	}
	m.active[instanceID] = activation
	m.mu.Unlock()
	return activation, nil
}

func (m *Manager) requestClient(address contract.ServiceAddress) *request.Client {
	if m.requests == nil {
		return nil
	}
	return m.requests.ForSource(address)
}

func (m *Manager) Passivate(ctx context.Context, instanceID contract.ServiceInstanceID) error {
	if m == nil {
		return fmt.Errorf("activation manager is nil")
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	return m.passivate(ctx, instanceID)
}

func (m *Manager) passivate(ctx context.Context, instanceID contract.ServiceInstanceID) error {
	m.mu.Lock()
	active, ok := m.active[instanceID]
	if ok {
		delete(m.active, instanceID)
	}
	m.mu.Unlock()
	if !ok {
		return nil
	}
	record, found, err := m.instances.Get(ctx, instanceID)
	if err != nil {
		return err
	}
	if found && record.Lifecycle != instance.Terminated {
		record.Lifecycle = instance.Passivated
		now := m.now()
		record.PassivatedAt = &now
		if err := m.instances.CompareAndSwap(ctx, record, record.RecordVersion); err != nil {
			return err
		}
	}
	return m.leases.Release(ctx, active.CurrentLease())
}

func (m *Manager) Terminate(ctx context.Context, instanceID contract.ServiceInstanceID) error {
	if m == nil {
		return fmt.Errorf("activation manager is nil")
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if active, ok := m.Lookup(instanceID); ok {
		if err := m.passivate(ctx, active.Instance.InstanceID); err != nil {
			return err
		}
	}
	record, found, err := m.instances.Get(ctx, instanceID)
	if err != nil || !found {
		return err
	}
	record.Lifecycle = instance.Terminated
	now := m.now()
	record.TerminatedAt = &now
	return m.instances.CompareAndSwap(ctx, record, record.RecordVersion)
}

func (m *Manager) fail(ctx context.Context, record instance.Record, lease instance.ActivationLease, cause error) {
	latest, found, err := m.instances.Get(ctx, record.InstanceID)
	if err == nil && found {
		latest.Lifecycle = instance.Failed
		latest.LastError = cause.Error()
		_ = m.instances.CompareAndSwap(ctx, latest, latest.RecordVersion)
	}
	_ = m.leases.Release(ctx, lease)
}

func (m *Manager) now() time.Time {
	if m.clock == nil {
		return time.Now().UTC()
	}
	return m.clock.Now().UTC()
}

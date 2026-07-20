package activation

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/instance"
	leaseguard "agent/serviceruntime/lease"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/request"
	"agent/serviceruntime/service"
	"context"
	"errors"
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
	Renew(ctx context.Context, instanceID contract.ServiceInstanceID) error
	Passivate(ctx context.Context, instanceID contract.ServiceInstanceID) error
	PassivateAll(ctx context.Context) error
	Terminate(ctx context.Context, instanceID contract.ServiceInstanceID) error
}

type Manager struct {
	plan        *building.RuntimePlan
	plans       building.PlanResolver
	definitions building.DefinitionResolver
	instances   instance.Store
	leases      instance.ActivationLeaseStore
	restorer    StateRestorer
	ownerID     string
	leaseTTL    time.Duration
	clock       contract.Clock
	requests    RequestClientResolver
	opMu        sync.Mutex

	mu     sync.RWMutex
	active map[contract.ServiceInstanceID]*Activation
}

type RequestClientResolver interface {
	ClientFor(revision contract.PlanRevision, address contract.ServiceAddress) *request.Client
}

type Options struct {
	Plan        *building.RuntimePlan
	Plans       building.PlanResolver
	Definitions building.DefinitionResolver
	Instances   instance.Store
	Leases      instance.ActivationLeaseStore
	Restorer    StateRestorer
	OwnerID     string
	LeaseTTL    time.Duration
	Clock       contract.Clock
	Requests    RequestClientResolver
}

func NewManager(options Options) (*Manager, error) {
	if options.Plan == nil || options.Definitions == nil || options.Instances == nil || options.Leases == nil || options.Restorer == nil {
		return nil, fmt.Errorf("activation manager requires plan, definitions, instance store, lease store and restorer")
	}
	if options.OwnerID == "" {
		return nil, fmt.Errorf("activation manager owner id is required")
	}
	if options.Plans == nil {
		options.Plans = building.NewPlanCatalog(options.Plan)
	}
	if options.LeaseTTL <= 0 {
		options.LeaseTTL = options.Plan.Recovery().ActivationLease
	}
	return &Manager{
		plan: options.Plan, plans: options.Plans, definitions: options.Definitions,
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
	lease, err := m.acquireLease(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	heartbeat := leaseguard.Start(ctx, leaseguard.Interval(m.leaseTTL), func(renewCtx context.Context) error {
		_, renewErr := m.leases.Renew(renewCtx, lease, m.leaseTTL)
		return renewErr
	})
	stopHeartbeat := func() error { return heartbeat.Stop() }
	record, found, err = m.instances.Get(ctx, instanceID)
	if err != nil || !found {
		_ = stopHeartbeat()
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
		_ = stopHeartbeat()
		_ = m.leases.Release(ctx, lease)
		return nil, err
	}
	record, _, _ = m.instances.Get(ctx, instanceID)
	definition, ok := m.definitions.ResolveDefinition(record.DefinitionRef)
	if !ok {
		_ = stopHeartbeat()
		m.fail(ctx, record, lease, fmt.Errorf("service definition %q not found", record.DefinitionRef.String()))
		return nil, fmt.Errorf("service definition %q not found", record.DefinitionRef.String())
	}
	instancePlan, planFound := m.plans.ResolvePlan(record.RuntimeID, record.PlanRevision)
	if !planFound {
		_ = stopHeartbeat()
		err = fmt.Errorf("runtime plan %q revision %q is not available", record.RuntimeID, record.PlanRevision)
		m.fail(ctx, record, lease, err)
		return nil, err
	}
	mount, mounted := instancePlan.Service(record.Address)
	var config []byte
	if mounted {
		config = mount.Config
	}
	requestClient := m.requestClient(record.PlanRevision, record.Address)
	target, err := definition.Factory.Create(heartbeat.Context(), service.CreateRequest{
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		InstanceID: record.InstanceID, Address: record.Address,
		Component: record.DefinitionRef, Config: contract.CloneRaw(config),
		Metadata: contract.CloneStrings(record.Metadata),
		Requests: requestClient,
	})
	if err != nil {
		_ = stopHeartbeat()
		m.fail(ctx, record, lease, err)
		return nil, err
	}
	if descriptor := target.Descriptor(); descriptor.Component != record.DefinitionRef {
		err = fmt.Errorf("factory returned component %q for instance definition %q", descriptor.Component.String(), record.DefinitionRef.String())
		_ = stopHeartbeat()
		m.fail(ctx, record, lease, err)
		return nil, err
	} else if !definition.StateSchema.Empty() && descriptor.StateSchema != definition.StateSchema {
		err = fault.Wrap(fault.CorruptState, "validate_service_descriptor", fmt.Errorf("factory returned state schema %v for definition schema %v", descriptor.StateSchema, definition.StateSchema))
		_ = stopHeartbeat()
		m.fail(ctx, record, lease, err)
		return nil, err
	}
	restored, err := m.restorer.Restore(heartbeat.Context(), target, record, config)
	if err != nil {
		_ = stopHeartbeat()
		m.fail(ctx, record, lease, err)
		return nil, err
	}
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		m.fail(ctx, record, lease, heartbeatErr)
		return nil, fault.Wrap(fault.LeaseLost, "activate_service", heartbeatErr)
	}
	if current, currentFound, currentErr := m.leases.Current(ctx, instanceID); currentErr != nil {
		m.fail(ctx, record, lease, currentErr)
		return nil, currentErr
	} else if !currentFound || current.Epoch != lease.Epoch || current.LeaseToken != lease.LeaseToken {
		err = persistence.ErrLeaseLost
		m.fail(ctx, record, lease, err)
		return nil, fault.Wrap(fault.LeaseLost, "activate_service", err)
	} else {
		lease = current
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

func (m *Manager) requestClient(revision contract.PlanRevision, address contract.ServiceAddress) *request.Client {
	if m.requests == nil {
		return nil
	}
	return m.requests.ClientFor(revision, address)
}

func (m *Manager) acquireLease(ctx context.Context, instanceID contract.ServiceInstanceID) (instance.ActivationLease, error) {
	for {
		acquired, err := m.leases.Acquire(ctx, instanceID, m.ownerID, m.leaseTTL)
		if err == nil {
			return acquired, nil
		}
		if !errors.Is(err, persistence.ErrLeaseLost) {
			return instance.ActivationLease{}, err
		}
		current, found, currentErr := m.leases.Current(ctx, instanceID)
		if currentErr != nil {
			return instance.ActivationLease{}, currentErr
		}
		if !found || !current.LeaseUntil.After(m.now()) {
			continue
		}
		delay := current.LeaseUntil.Sub(m.now())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return instance.ActivationLease{}, fault.Wrap(fault.LeaseLost, "acquire_activation", ctx.Err())
		case <-timer.C:
		}
	}
}

func (m *Manager) Passivate(ctx context.Context, instanceID contract.ServiceInstanceID) error {
	if m == nil {
		return fmt.Errorf("activation manager is nil")
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	return m.passivate(ctx, instanceID)
}

func (m *Manager) Renew(ctx context.Context, instanceID contract.ServiceInstanceID) error {
	if m == nil {
		return fmt.Errorf("activation manager is nil")
	}
	active, ok := m.Lookup(instanceID)
	if !ok {
		return persistence.ErrLeaseLost
	}
	renewed, err := m.leases.Renew(ctx, active.CurrentLease(), m.leaseTTL)
	if err != nil {
		return err
	}
	active.updateLease(renewed)
	return nil
}

func (m *Manager) PassivateAll(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.RLock()
	ids := make([]contract.ServiceInstanceID, 0, len(m.active))
	for id := range m.active {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	var first error
	for _, id := range ids {
		if err := m.passivate(ctx, id); err != nil && first == nil {
			first = err
		}
	}
	return first
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
		transitionErr := m.instances.CompareAndSwap(ctx, record, record.RecordVersion)
		releaseErr := m.leases.Release(ctx, active.CurrentLease())
		if transitionErr != nil {
			return transitionErr
		}
		return releaseErr
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

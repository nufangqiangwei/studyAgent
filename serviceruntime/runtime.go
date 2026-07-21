package serviceruntime

import (
	"agent/serviceruntime/activation"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/host"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/recovery"
	"agent/serviceruntime/transport"
	"context"
	"fmt"
	"sync"
	"time"
)

type RuntimeStatus string

const (
	RuntimeCreated    RuntimeStatus = "created"
	RuntimeRecovering RuntimeStatus = "recovering"
	RuntimeReady      RuntimeStatus = "ready"
	RuntimeLive       RuntimeStatus = "live"
	RuntimeDraining   RuntimeStatus = "draining"
	RuntimeStopped    RuntimeStatus = "stopped"
	RuntimeFailed     RuntimeStatus = "failed"
)

type Runtime struct {
	plan        *building.RuntimePlan
	plans       *building.PlanCatalog
	definitions building.DefinitionResolver
	storage     persistence.RuntimeStorage
	ownsStorage bool
	directory   instance.InstanceDirectory
	activator   activation.Activator
	bus         transport.EventBus
	host        host.Host
	effects     effect.Worker
	recovery    recovery.Manager
	ids         contract.IDGenerator
	clock       contract.Clock
	ownerID     string

	mu     sync.RWMutex
	status RuntimeStatus

	serveMu     sync.Mutex
	serving     bool
	serveCancel context.CancelFunc
	serveDone   chan struct{}
}

func (r *Runtime) Plan() *building.RuntimePlan {
	if r == nil {
		return nil
	}
	return r.plan
}

func (r *Runtime) Status() RuntimeStatus {
	if r == nil {
		return RuntimeStopped
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}

func (r *Runtime) Start(ctx context.Context) (recovery.Report, error) {
	if r == nil {
		return recovery.Report{}, fmt.Errorf("service runtime is nil")
	}
	if err := r.transition(RuntimeCreated, RuntimeRecovering); err != nil {
		return recovery.Report{}, err
	}
	if err := r.host.Start(ctx); err != nil {
		r.setStatus(RuntimeFailed)
		return recovery.Report{}, err
	}
	report, err := r.recovery.Recover(ctx, r.plan)
	if err != nil {
		r.setStatus(RuntimeFailed)
		return report, err
	}

	r.setStatus(RuntimeLive)
	return report, nil
}

func (r *Runtime) Publish(ctx context.Context, message contract.Message) (transport.PublishResult, error) {
	if r == nil {
		return transport.PublishResult{}, fmt.Errorf("service runtime is nil")
	}
	if r.Status() != RuntimeLive {
		return transport.PublishResult{}, fmt.Errorf("service runtime is not live")
	}
	spec := r.plan.Runtime()
	if message.ID == "" {
		id, err := r.ids.New("message")
		if err != nil {
			return transport.PublishResult{}, err
		}
		message.ID = id
	}
	if message.RuntimeID == "" {
		message.RuntimeID = spec.ID
	}
	if message.PlanRevision == "" {
		message.PlanRevision = spec.Revision
	}
	if message.CorrelationID == "" {
		message.CorrelationID = message.ID
	}
	return r.bus.Publish(ctx, message)
}

func (r *Runtime) HandleNext(ctx context.Context, address contract.ServiceAddress) (host.HandleResult, error) {
	if r == nil {
		return host.HandleResult{}, fmt.Errorf("service runtime is nil")
	}
	spec := r.plan.Runtime()
	target, err := r.directory.ResolveAddress(ctx, spec.ID, spec.Revision, address)
	if err != nil {
		return host.HandleResult{}, err
	}
	return r.host.HandleNext(ctx, target.InstanceID)
}

func (r *Runtime) DispatchNextOutbox(ctx context.Context) (transport.DispatchResult, error) {
	if r == nil {
		return transport.DispatchResult{}, fmt.Errorf("service runtime is nil")
	}
	return r.bus.DispatchNextOutbox(ctx, r.ownerID+".outbox")
}

func (r *Runtime) DispatchNextEffect(ctx context.Context) (effect.WorkResult, error) {
	if r == nil {
		return effect.WorkResult{}, fmt.Errorf("service runtime is nil")
	}
	return r.effects.DispatchNext(ctx, r.ownerID+".effect")
}

type InstanceDeclaration struct {
	InstanceID contract.ServiceInstanceID
	Address    contract.ServiceAddress
	Component  contract.ComponentRef
	ParentID   contract.ServiceInstanceID
	Metadata   map[string]string
}

func (r *Runtime) DeclareInstance(ctx context.Context, declaration InstanceDeclaration) (instance.Record, error) {
	if r == nil {
		return instance.Record{}, fmt.Errorf("service runtime is nil")
	}
	status := r.Status()
	if status == RuntimeDraining || status == RuntimeStopped || status == RuntimeFailed {
		return instance.Record{}, fmt.Errorf("cannot declare an instance while runtime is %q", status)
	}
	if declaration.Address == "" || !declaration.Component.Valid() {
		return instance.Record{}, fmt.Errorf("dynamic instance address and component are required")
	}
	definition, ok := r.definitions.ResolveDefinition(declaration.Component)
	if !ok {
		return instance.Record{}, fmt.Errorf("service definition %q is not registered", declaration.Component.String())
	}
	if definition.Scope != building.ScopeVirtual {
		return instance.Record{}, fmt.Errorf("service definition %q has scope %q, want %q", declaration.Component.String(), definition.Scope, building.ScopeVirtual)
	}
	spec := r.plan.Runtime()
	existing, found, err := r.storage.Instances().GetByAddress(ctx, spec.ID, spec.Revision, declaration.Address)
	if err != nil {
		return instance.Record{}, err
	}
	if found {
		if existing.DefinitionRef != declaration.Component {
			return instance.Record{}, fmt.Errorf("dynamic address %q already uses component %q", declaration.Address, existing.DefinitionRef.String())
		}
		if existing.Lifecycle == instance.Terminated {
			return instance.Record{}, fmt.Errorf("dynamic address %q is tombstoned", declaration.Address)
		}
		return existing, nil
	}
	instanceID := declaration.InstanceID
	if instanceID == "" {
		instanceID = contract.ServiceInstanceID(r.ids.Derive("service-instance", string(spec.ID), string(spec.Revision), string(declaration.Address)))
	}
	rootID := instanceID
	depth := 0
	if declaration.ParentID != "" {
		parent, parentFound, parentErr := r.storage.Instances().Get(ctx, declaration.ParentID)
		if parentErr != nil {
			return instance.Record{}, parentErr
		}
		if !parentFound || parent.RuntimeID != spec.ID || parent.PlanRevision != spec.Revision || parent.Lifecycle == instance.Terminated {
			return instance.Record{}, fmt.Errorf("parent instance %q is not available in the runtime plan", declaration.ParentID)
		}
		rootID = parent.RootID
		if rootID == "" {
			rootID = parent.InstanceID
		}
		depth = parent.Depth + 1
	}
	now := r.now()
	record := instance.Record{
		InstanceID: instanceID, Address: declaration.Address, Kind: instance.ServiceVirtual,
		DefinitionRef: declaration.Component, RuntimeID: spec.ID, PlanRevision: spec.Revision,
		ParentID: declaration.ParentID, RootID: rootID, Depth: depth,
		MailboxID:     contract.MailboxID(r.ids.Derive("mailbox", string(instanceID))),
		StateStreamID: contract.StreamID("service/" + string(instanceID)),
		Lifecycle:     instance.Declared, CreatedAt: now, UpdatedAt: now,
		Metadata: contract.CloneStrings(declaration.Metadata),
	}
	if err := r.storage.Instances().Create(ctx, record); err != nil {
		return instance.Record{}, err
	}
	created, _, err := r.storage.Instances().Get(ctx, instanceID)
	if err != nil {
		return instance.Record{}, err
	}
	if err := r.directory.Register(ctx, created); err != nil {
		return instance.Record{}, err
	}
	return created, nil
}

func (r *Runtime) PassivateInstance(ctx context.Context, instanceID contract.ServiceInstanceID) error {
	if r == nil {
		return fmt.Errorf("service runtime is nil")
	}
	return r.activator.Passivate(ctx, instanceID)
}

func (r *Runtime) TerminateInstance(ctx context.Context, instanceID contract.ServiceInstanceID) error {
	if r == nil {
		return fmt.Errorf("service runtime is nil")
	}
	if err := r.activator.Terminate(ctx, instanceID); err != nil {
		return err
	}
	return r.directory.Remove(ctx, instanceID)
}

func (r *Runtime) Drain(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.setStatus(RuntimeDraining)
	if err := r.host.Drain(ctx); err != nil {
		return err
	}
	return r.bus.Drain(ctx)
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.stopServing()
	_ = r.host.Stop(context.Background())
	activationErr := r.activator.PassivateAll(context.Background())
	busErr := r.bus.Close()
	var storageErr error
	if r.ownsStorage {
		storageErr = r.storage.Close()
	}
	r.setStatus(RuntimeStopped)
	if activationErr != nil {
		return activationErr
	}

	if busErr != nil {
		return busErr
	}
	return storageErr
}

func (r *Runtime) stopServing() {
	if r == nil {
		return
	}
	r.serveMu.Lock()
	cancel := r.serveCancel
	done := r.serveDone
	r.serveMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (r *Runtime) transition(from, to RuntimeStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status != from {
		return fmt.Errorf("runtime state is %q, want %q", r.status, from)
	}
	r.status = to
	return nil
}

func (r *Runtime) setStatus(status RuntimeStatus) {
	r.mu.Lock()
	r.status = status
	r.mu.Unlock()
}

func (r *Runtime) now() time.Time {
	if r.clock == nil {
		return time.Now().UTC()
	}
	return r.clock.Now().UTC()
}

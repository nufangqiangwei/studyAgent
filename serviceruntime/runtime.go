package serviceruntime

import (
	"agent/serviceruntime/activation"
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/assembly"
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
	"io"
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
	artifacts   artifact.Store
	directory   instance.InstanceDirectory
	activator   activation.Activator
	bus         transport.EventBus
	host        host.Host
	effects     effect.Worker
	recovery    recovery.Manager
	ids         contract.IDGenerator
	clock       contract.Clock
	ownerID     string
	instances   assembly.InstanceControl

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

func (r *Runtime) BeginArtifact(ctx context.Context, request artifact.WriteRequest) (artifact.WriteSession, error) {
	if r == nil || r.artifacts == nil {
		return nil, artifact.ErrUnavailable
	}
	return r.artifacts.Begin(ctx, request)
}

func (r *Runtime) WriteArtifact(ctx context.Context, request artifact.WriteRequest, source io.Reader) (contract.ArtifactRef, error) {
	if r == nil || r.artifacts == nil {
		return contract.ArtifactRef{}, artifact.ErrUnavailable
	}
	return artifact.WriteAll(ctx, r.artifacts, request, source)
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref contract.ArtifactRef) (io.ReadCloser, artifact.Info, error) {
	if r == nil || r.artifacts == nil {
		return nil, artifact.Info{}, artifact.ErrUnavailable
	}
	return r.artifacts.Open(ctx, ref)
}

func (r *Runtime) StatArtifact(ctx context.Context, ref contract.ArtifactRef) (artifact.Info, error) {
	if r == nil || r.artifacts == nil {
		return artifact.Info{}, artifact.ErrUnavailable
	}
	return r.artifacts.Stat(ctx, ref)
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

type InstanceDeclaration = instance.Declaration

func (r *Runtime) DeclareInstance(ctx context.Context, declaration InstanceDeclaration) (instance.Record, error) {
	if r == nil {
		return instance.Record{}, fmt.Errorf("service runtime is nil")
	}
	status := r.Status()
	if status == RuntimeDraining || status == RuntimeStopped || status == RuntimeFailed {
		return instance.Record{}, fmt.Errorf("cannot declare an instance while runtime is %q", status)
	}
	if r.instances == nil {
		return instance.Record{}, fmt.Errorf("instance controller is unavailable")
	}
	return r.instances.Declare(ctx, "", declaration)
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

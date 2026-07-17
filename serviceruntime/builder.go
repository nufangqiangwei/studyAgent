package serviceruntime

import (
	"agent/serviceruntime/activation"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/host"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/recovery"
	"agent/serviceruntime/request"
	"agent/serviceruntime/transport"
	"context"
	"fmt"
	"time"
)

type BuilderOptions struct {
	Register       *building.Register
	Effects        *effect.Registry
	Storage        persistence.RuntimeStorage
	Clock          contract.Clock
	IDs            contract.IDGenerator
	Observer       contract.RuntimeEventRecorder
	OwnerID        string
	RequestTimeout time.Duration
}

type Builder struct {
	register       *building.Register
	effects        *effect.Registry
	storage        persistence.RuntimeStorage
	clock          contract.Clock
	ids            contract.IDGenerator
	observer       contract.RuntimeEventRecorder
	ownerID        string
	requestTimeout time.Duration
}

func NewBuilder(options BuilderOptions) (*Builder, error) {
	if options.Register == nil {
		options.Register = building.NewRegister(nil)
	}
	if options.Effects == nil {
		options.Effects = effect.NewRegistry()
	}
	if options.Clock == nil {
		options.Clock = SystemClock{}
	}
	if options.IDs == nil {
		options.IDs = StableIDs{}
	}
	if options.Observer == nil {
		options.Observer = NoopRecorder{}
	}
	if options.OwnerID == "" {
		ownerID, err := options.IDs.New("runtime-owner")
		if err != nil {
			return nil, err
		}
		options.OwnerID = ownerID
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 30 * time.Second
	}
	return &Builder{
		register: options.Register, effects: options.Effects, storage: options.Storage,
		clock: options.Clock, ids: options.IDs, observer: options.Observer, ownerID: options.OwnerID,
		requestTimeout: options.RequestTimeout,
	}, nil
}

func (b *Builder) RegisterService(definition building.ServiceDefinition) error {
	if b == nil {
		return fmt.Errorf("runtime builder is nil")
	}
	return b.register.RegisterService(definition)
}

func (b *Builder) RegisterPlanValidator(validator building.PlanValidator) error {
	if b == nil {
		return fmt.Errorf("runtime builder is nil")
	}
	return b.register.RegisterPlanValidator(validator)
}

func (b *Builder) RegisterEffect(spec effect.Spec) error {
	if b == nil {
		return fmt.Errorf("runtime builder is nil")
	}
	if err := b.effects.Register(spec); err != nil {
		return err
	}
	return b.register.RegisterEffectExecutor(spec.Ref)
}

func (b *Builder) Build(ctx context.Context, manifest building.RuntimeManifest) (*Runtime, error) {
	if b == nil {
		return nil, fmt.Errorf("runtime builder is nil")
	}
	plan, err := b.register.Compile(ctx, manifest)
	if err != nil {
		return nil, err
	}
	storage := b.storage
	ownsStorage := false
	if storage == nil {
		storage = memory.New(b.clock)
		ownsStorage = true
	}
	directory, err := instance.NewDirectory(storage.Instances())
	if err != nil {
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	if err := b.ensureStaticInstances(ctx, plan, storage, directory); err != nil {
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	restorer, err := activation.NewJournalRestorer(storage.Journal(), storage.Snapshots())
	if err != nil {
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	replyBroker, err := request.NewBroker(request.DefaultReplyAddress)
	if err != nil {
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	bus, err := transport.New(transport.Options{
		Plan: plan, Resolver: directory, Inbox: storage.Inbox(), Outbox: storage.Outbox(),
		Clock: b.clock, Observer: b.observer, IDs: b.ids,
		OutboxLease: plan.Recovery().OutboxLease, MaxAttempts: plan.Recovery().MaxDeliveryAttempts,
		ReplySink: replyBroker,
	})
	if err != nil {
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	spec := plan.Runtime()
	requestClients, err := request.NewClientFactory(request.ClientFactoryOptions{
		RuntimeID: spec.ID, PlanRevision: spec.Revision, IDs: b.ids,
		DefaultTimeout: b.requestTimeout,
		Broker:         replyBroker,
		Sender: request.SenderFunc(func(ctx context.Context, message contract.Message) error {
			_, publishErr := bus.Publish(ctx, message)
			return publishErr
		}),
	})
	if err != nil {
		_ = bus.Close()
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	activator, err := activation.NewManager(activation.Options{
		Plan: plan, Definitions: b.register, Instances: storage.Instances(), Leases: storage.Leases(),
		Restorer: restorer, OwnerID: b.ownerID + ".activation", LeaseTTL: plan.Recovery().ActivationLease, Clock: b.clock,
		Requests: requestClients,
	})
	if err != nil {
		_ = bus.Close()
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	hostRuntime, err := host.New(host.Options{
		Plan: plan, Definitions: b.register, Activator: activator, Storage: storage, IDs: b.ids, Clock: b.clock,
		Observer: b.observer, OwnerID: b.ownerID + ".host",
		InboxLease: plan.Recovery().InboxLease, MaxAttempts: plan.Recovery().MaxDeliveryAttempts,
	})
	if err != nil {
		_ = bus.Close()
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	effectWorker, err := effect.NewWorker(effect.WorkerOptions{
		Plan: plan, Store: storage.Effects(), Registry: b.effects, Clock: b.clock,
		Lease: plan.Recovery().EffectLease, MaxAttempts: plan.Recovery().MaxDeliveryAttempts,
	})
	if err != nil {
		_ = bus.Close()
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	recoveryRuntime, err := recovery.New(recovery.Options{
		Storage: storage, Directory: directory, Activator: activator, Effects: effectWorker,
		Bus: bus, Observer: b.observer, IDs: b.ids, Clock: b.clock, OwnerID: b.ownerID + ".recovery",
	})
	if err != nil {
		_ = bus.Close()
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	return &Runtime{
		plan: plan, register: b.register, storage: storage, ownsStorage: ownsStorage,
		directory: directory, activator: activator, bus: bus, host: hostRuntime,
		effects: effectWorker, recovery: recoveryRuntime,
		requestClients: requestClients,
		ids:            b.ids, clock: b.clock, ownerID: b.ownerID, status: RuntimeCreated,
	}, nil
}

func (b *Builder) ensureStaticInstances(ctx context.Context, plan *building.RuntimePlan, storage persistence.RuntimeStorage, directory *instance.Directory) error {
	spec := plan.Runtime()
	now := b.clock.Now().UTC()
	for _, mount := range plan.Services() {
		existing, found, err := storage.Instances().GetByAddress(ctx, spec.ID, spec.Revision, mount.Address)
		if err != nil {
			return err
		}
		if found {
			if existing.DefinitionRef != mount.Component {
				return fmt.Errorf("existing instance at %q uses component %q, want %q", mount.Address, existing.DefinitionRef.String(), mount.Component.String())
			}
			if err := directory.Register(ctx, existing); err != nil {
				return err
			}
			continue
		}
		instanceID := contract.ServiceInstanceID(b.ids.Derive("service-instance", string(spec.ID), string(spec.Revision), string(mount.Address)))
		record := instance.Record{
			InstanceID: instanceID, Address: mount.Address, Kind: instance.ServiceStatic,
			DefinitionRef: mount.Component, RuntimeID: spec.ID, PlanRevision: spec.Revision,
			RootID:        instanceID,
			MailboxID:     contract.MailboxID(b.ids.Derive("mailbox", string(instanceID))),
			StateStreamID: contract.StreamID("service/" + string(instanceID)),
			Lifecycle:     instance.Declared, CreatedAt: now, UpdatedAt: now,
			Metadata: contract.CloneStrings(mount.Metadata),
		}
		if err := storage.Instances().Create(ctx, record); err != nil {
			return err
		}
		created, _, _ := storage.Instances().Get(ctx, instanceID)
		if err := directory.Register(ctx, created); err != nil {
			return err
		}
	}
	return nil
}

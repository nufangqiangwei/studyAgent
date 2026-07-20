package serviceruntime

import (
	"agent/serviceruntime/activation"
	"agent/serviceruntime/building"
	"agent/serviceruntime/connection"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/host"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/recovery"
	"agent/serviceruntime/transport"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type BuilderOptions struct {
	Register          *building.Register
	Effects           *effect.Registry
	Storage           persistence.RuntimeStorage
	Clock             contract.Clock
	IDs               contract.IDGenerator
	Observer          contract.RuntimeEventRecorder
	OwnerID           string
	RetryPolicy       fault.RetryPolicy
	StateMigrator     activation.SnapshotMigrator
	EventUpcaster     activation.EventUpcaster
	ConnectionDrivers *connection.Registry
}

type Builder struct {
	register          *building.Register
	effects           *effect.Registry
	storage           persistence.RuntimeStorage
	clock             contract.Clock
	ids               contract.IDGenerator
	observer          contract.RuntimeEventRecorder
	ownerID           string
	retryPolicy       fault.RetryPolicy
	stateMigrator     activation.SnapshotMigrator
	eventUpcaster     activation.EventUpcaster
	connections       *connection.Registry
	connectionFactory *connection.ServiceFactory
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
	if options.RetryPolicy == nil {
		options.RetryPolicy = fault.ExponentialRetryPolicy{}
	}
	if options.ConnectionDrivers == nil {
		options.ConnectionDrivers = connection.NewRegistry()
	}
	var connectionFactory *connection.ServiceFactory
	if existing, found := options.Register.ResolveDefinition(connection.ManagerComponent); found {
		var ok bool
		connectionFactory, ok = existing.Factory.(*connection.ServiceFactory)
		if !ok {
			return nil, fmt.Errorf("service component %q is reserved for the runtime connection manager", connection.ManagerComponent.String())
		}
	} else {
		connectionFactory = connection.NewServiceFactory()
		if err := options.Register.RegisterService(connection.Definition(connectionFactory)); err != nil {
			return nil, err
		}
	}
	return &Builder{
		register: options.Register, effects: options.Effects, storage: options.Storage,
		clock: options.Clock, ids: options.IDs, observer: options.Observer, ownerID: options.OwnerID,
		retryPolicy:   options.RetryPolicy,
		stateMigrator: options.StateMigrator, eventUpcaster: options.EventUpcaster,
		connections: options.ConnectionDrivers, connectionFactory: connectionFactory,
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

func (b *Builder) RegisterConnectionDriver(name string, driver connection.Driver) error {
	if b == nil {
		return fmt.Errorf("runtime builder is nil")
	}
	return b.connections.Register(name, driver)
}

func (b *Builder) Build(ctx context.Context, manifest building.RuntimeManifest) (*Runtime, error) {
	if b == nil {
		return nil, fmt.Errorf("runtime builder is nil")
	}
	persistedManifest := manifest
	manifest, err := withConnectionManager(manifest)
	if err != nil {
		return nil, err
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
	plans, err := b.persistedPlanCatalog(ctx, storage, persistedManifest, plan)
	if err != nil {
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	definitions := b.register.FreezeDefinitions()
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
	restorer, err := activation.NewJournalRestorerWithOptions(activation.RestorerOptions{
		Journal: storage.Journal(), Snapshots: storage.Snapshots(), Migrator: b.stateMigrator, Upcaster: b.eventUpcaster,
	})
	if err != nil {
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	bus, err := transport.New(transport.Options{
		Plan: plan, Plans: plans, Resolver: directory, Inbox: storage.Inbox(), Outbox: storage.Outbox(), Sequences: storage.Sequences(),
		Clock: b.clock, Observer: b.observer, IDs: b.ids,
		OutboxLease: plan.Recovery().OutboxLease, MaxAttempts: plan.Recovery().MaxDeliveryAttempts,
		RetryPolicy: b.retryPolicy,
	})
	if err != nil {
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	spec := plan.Runtime()
	sender := connection.SenderFunc(func(ctx context.Context, message contract.Message) error {
		_, publishErr := bus.Publish(ctx, message)
		return publishErr
	})
	connectionManager, err := connection.NewManager(connection.Options{
		RuntimeID: spec.ID, Store: storage.Connections(), Resolver: directory,
		Drivers: b.connections, Sender: sender, IDs: b.ids, Clock: b.clock, Observer: b.observer,
	})
	if err != nil {
		_ = bus.Close()
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	for _, revisionPlan := range plans.Plans() {
		revisionSpec := revisionPlan.Runtime()
		b.connectionFactory.Bind(revisionSpec.ID, revisionSpec.Revision, connectionManager)
	}
	activator, err := activation.NewManager(activation.Options{
		Plan: plan, Plans: plans, Definitions: definitions, Instances: storage.Instances(), Leases: storage.Leases(),
		Restorer: restorer, OwnerID: b.ownerID + ".activation", LeaseTTL: plan.Recovery().ActivationLease, Clock: b.clock,
	})
	if err != nil {
		_ = bus.Close()
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	hostRuntime, err := host.New(host.Options{
		Plan: plan, Plans: plans, Definitions: definitions, Activator: activator, Storage: storage, IDs: b.ids, Clock: b.clock,
		Observer: b.observer, OwnerID: b.ownerID + ".host",
		InboxLease: plan.Recovery().InboxLease, ActivationLease: plan.Recovery().ActivationLease,
		MaxAttempts: plan.Recovery().MaxDeliveryAttempts,
		RetryPolicy: b.retryPolicy,
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
		Lease: plan.Recovery().EffectLease, MaxAttempts: plan.Recovery().MaxDeliveryAttempts, RetryPolicy: b.retryPolicy,
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
		Bus: bus, Plans: plans, Observer: b.observer, IDs: b.ids, Clock: b.clock, OwnerID: b.ownerID + ".recovery",
	})
	if err != nil {
		_ = bus.Close()
		if ownsStorage {
			_ = storage.Close()
		}
		return nil, err
	}
	return &Runtime{
		plan: plan, plans: plans, definitions: definitions, storage: storage, ownsStorage: ownsStorage,
		directory: directory, activator: activator, bus: bus, host: hostRuntime,
		effects: effectWorker, recovery: recoveryRuntime,
		connections: connectionManager,
		ids:         b.ids, clock: b.clock, ownerID: b.ownerID, status: RuntimeCreated,
	}, nil
}

func (b *Builder) persistedPlanCatalog(ctx context.Context, storage persistence.RuntimeStorage, manifest building.RuntimeManifest, current *building.RuntimePlan) (*building.PlanCatalog, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("encode runtime manifest: %w", err)
	}
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	spec := current.Runtime()
	if _, err := storage.Plans().Put(ctx, persistence.PlanRecord{
		RuntimeID: spec.ID, PlanRevision: spec.Revision, PlanHash: hash,
		Manifest: data, CreatedAt: b.clock.Now().UTC(),
	}); err != nil {
		return nil, fmt.Errorf("persist runtime plan: %w", err)
	}
	records, err := storage.Plans().List(ctx, spec.ID)
	if err != nil {
		return nil, fmt.Errorf("list runtime plans: %w", err)
	}
	compiled := make([]*building.RuntimePlan, 0, len(records))
	for _, record := range records {
		storedSum := sha256.Sum256(record.Manifest)
		if record.PlanHash != hex.EncodeToString(storedSum[:]) {
			return nil, fmt.Errorf("stored runtime plan %q checksum is invalid", record.PlanRevision)
		}
		if record.PlanRevision == spec.Revision {
			compiled = append(compiled, current)
			continue
		}
		var storedManifest building.RuntimeManifest
		if err := json.Unmarshal(record.Manifest, &storedManifest); err != nil {
			return nil, fmt.Errorf("decode runtime plan %q: %w", record.PlanRevision, err)
		}
		if storedManifest.Runtime.ID != record.RuntimeID || storedManifest.Runtime.Revision != record.PlanRevision {
			return nil, fmt.Errorf("stored runtime plan %q identity does not match its record", record.PlanRevision)
		}
		storedManifest, err = withConnectionManager(storedManifest)
		if err != nil {
			return nil, fmt.Errorf("add runtime connection manager to stored plan %q: %w", record.PlanRevision, err)
		}
		storedPlan, err := b.register.Compile(ctx, storedManifest)
		if err != nil {
			return nil, fmt.Errorf("compile stored runtime plan %q: %w", record.PlanRevision, err)
		}
		compiled = append(compiled, storedPlan)
	}
	return building.NewPlanCatalog(current, compiled...), nil
}

func withConnectionManager(manifest building.RuntimeManifest) (building.RuntimeManifest, error) {
	for _, mount := range manifest.Services {
		if mount.Address == connection.ManagerAddress {
			if mount.Component != connection.ManagerComponent {
				return building.RuntimeManifest{}, fmt.Errorf("service address %q is reserved for component %q", connection.ManagerAddress, connection.ManagerComponent.String())
			}
			return manifest, nil
		}
		if mount.Component == connection.ManagerComponent {
			return building.RuntimeManifest{}, fmt.Errorf("component %q must use reserved address %q", connection.ManagerComponent.String(), connection.ManagerAddress)
		}
	}
	manifest.Services = append(manifest.Services, building.ServiceMount{
		Address: connection.ManagerAddress, Component: connection.ManagerComponent,
		Metadata: map[string]string{"runtime.system": "connection-manager"},
	})
	return manifest, nil
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

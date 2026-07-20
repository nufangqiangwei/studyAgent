package recovery

import (
	"agent/serviceruntime/activation"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/transport"
	"context"
	"fmt"
	"time"
)

type Phase string

const (
	LoadPlan         Phase = "load_plan"
	LoadInstances    Phase = "load_instances"
	RebuildDirectory Phase = "rebuild_directory"
	RestoreState     Phase = "restore_state"
	ScanMessages     Phase = "scan_messages"
	ReconcileEffects Phase = "reconcile_effects"
	AcquireLeases    Phase = "acquire_leases"
	ActivateRequired Phase = "activate_required"
	EnableDelivery   Phase = "enable_delivery"
	Completed        Phase = "completed"
)

type Report struct {
	RuntimeID           contract.RuntimeID
	PlanRevision        contract.PlanRevision
	InstancesLoaded     int
	StreamsRestored     int
	EventsReplayed      int
	PendingInbox        int
	PendingOutbox       int
	EffectsReconciled   int
	InstancesActivated  int
	ConnectionsRestored int
	ConnectionsFailed   int
	StartedAt           time.Time
	CompletedAt         time.Time
	Warnings            []string
}

type Manager interface {
	Recover(ctx context.Context, plan *building.RuntimePlan) (Report, error)
}

type Coordinator struct {
	storage   persistence.RuntimeStorage
	plans     building.PlanResolver
	directory instance.InstanceDirectory
	activator activation.Activator
	effects   effect.Worker
	bus       transport.EventBus
	observer  contract.RuntimeEventRecorder
	ids       contract.IDGenerator
	clock     contract.Clock
	ownerID   string
}

type Options struct {
	Storage   persistence.RuntimeStorage
	Plans     building.PlanResolver
	Directory instance.InstanceDirectory
	Activator activation.Activator
	Effects   effect.Worker
	Bus       transport.EventBus
	Observer  contract.RuntimeEventRecorder
	IDs       contract.IDGenerator
	Clock     contract.Clock
	OwnerID   string
}

func New(options Options) (*Coordinator, error) {
	if options.Storage == nil || options.Directory == nil || options.Activator == nil || options.Effects == nil || options.Bus == nil {
		return nil, fmt.Errorf("recovery coordinator requires storage, directory, activator, effect worker and event bus")
	}
	if options.Plans == nil {
		return nil, fmt.Errorf("recovery coordinator requires a runtime plan catalog")
	}
	if options.OwnerID == "" {
		return nil, fmt.Errorf("recovery owner id is required")
	}
	return &Coordinator{
		storage: options.Storage, plans: options.Plans, directory: options.Directory, activator: options.Activator,
		effects: options.Effects, bus: options.Bus, observer: options.Observer,
		ids: options.IDs, clock: options.Clock, ownerID: options.OwnerID,
	}, nil
}

func (c *Coordinator) Recover(ctx context.Context, plan *building.RuntimePlan) (Report, error) {
	if c == nil || plan == nil {
		return Report{}, fmt.Errorf("recovery coordinator and runtime plan are required")
	}
	spec := plan.Runtime()
	report := Report{RuntimeID: spec.ID, PlanRevision: spec.Revision, StartedAt: c.now()}
	if err := c.bus.Pause(ctx); err != nil {
		return report, err
	}
	records, err := c.storage.Instances().List(ctx, instance.Query{RuntimeID: spec.ID})
	if err != nil {
		return report, err
	}
	report.InstancesLoaded = len(records)
	for _, record := range records {
		if _, found := c.plans.ResolvePlan(record.RuntimeID, record.PlanRevision); !found {
			return report, fmt.Errorf("service instance %q references unavailable plan revision %q", record.InstanceID, record.PlanRevision)
		}
	}
	for index := range records {
		if records[index].Lifecycle != instance.Active {
			continue
		}
		next := records[index].Clone()
		next.Lifecycle = instance.Recovering
		if err := c.storage.Instances().CompareAndSwap(ctx, next, records[index].RecordVersion); err != nil {
			return report, err
		}
		records[index], _, _ = c.storage.Instances().Get(ctx, records[index].InstanceID)
	}
	if err := c.directory.Rebuild(ctx, records); err != nil {
		return report, err
	}
	for _, record := range records {
		pending, countErr := c.storage.Inbox().CountPending(ctx, record.MailboxID)
		if countErr != nil {
			return report, countErr
		}
		report.PendingInbox += pending
	}
	report.PendingOutbox, err = c.storage.Outbox().CountPending(ctx, spec.ID)
	if err != nil {
		return report, err
	}
	unfinished, err := c.storage.Effects().ListUnfinished(ctx, spec.ID)
	if err != nil {
		return report, err
	}
	for _, record := range unfinished {
		if record.Status != persistence.EffectStarted && record.Status != persistence.EffectReconciliationRequired {
			continue
		}
		if _, reconcileErr := c.effects.Reconcile(ctx, record, c.ownerID); reconcileErr != nil {
			report.Warnings = append(report.Warnings, fmt.Sprintf("effect %s: %v", record.EffectID, reconcileErr))
			continue
		}
		report.EffectsReconciled++
	}
	for _, record := range records {
		if record.Lifecycle == instance.Terminated || record.Lifecycle == instance.Draining {
			continue
		}
		pending, countErr := c.storage.Inbox().CountPending(ctx, record.MailboxID)
		if countErr != nil {
			return report, countErr
		}
		if record.Kind != instance.ServiceStatic && pending == 0 {
			if record.Lifecycle == instance.Recovering {
				next := record.Clone()
				next.Lifecycle = instance.Passivated
				if transitionErr := c.storage.Instances().CompareAndSwap(ctx, next, record.RecordVersion); transitionErr != nil {
					return report, transitionErr
				}
			}
			continue
		}
		active, activateErr := c.activator.Activate(ctx, record.InstanceID)
		if activateErr != nil {
			return report, activateErr
		}
		report.InstancesActivated++
		report.StreamsRestored++
		report.EventsReplayed += active.ReplayedEvents()
	}
	if err := c.bus.Resume(ctx); err != nil {
		return report, err
	}
	report.CompletedAt = c.now()
	c.record(ctx, report)
	return report, nil
}

func (c *Coordinator) now() time.Time {
	if c.clock == nil {
		return time.Now().UTC()
	}
	return c.clock.Now().UTC()
}

func (c *Coordinator) record(ctx context.Context, report Report) {
	if c.observer == nil || c.ids == nil {
		return
	}
	id, err := c.ids.New("runtime-event")
	if err != nil {
		return
	}
	_ = c.observer.RecordRuntimeEvent(ctx, contract.RuntimeEvent{
		ID: id, Type: contract.RuntimeRecoveryCompleted,
		RuntimeID: report.RuntimeID, PlanRevision: report.PlanRevision,
		OccurredAt: report.CompletedAt,
		Attributes: map[string]string{
			"instances":          fmt.Sprint(report.InstancesLoaded),
			"activated":          fmt.Sprint(report.InstancesActivated),
			"effects_reconciled": fmt.Sprint(report.EffectsReconciled),
		},
	})
}

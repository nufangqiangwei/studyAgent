package activation

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
)

type RestoredState struct {
	State            service.State
	LastSequence     uint64
	SnapshotSequence uint64
	ReplayedEvents   int
}

type StateRestorer interface {
	Restore(ctx context.Context, target service.Service, record instance.Record, config json.RawMessage) (RestoredState, error)
}

type SnapshotMigrator interface {
	MigrateSnapshot(ctx context.Context, snapshot contract.Snapshot, target service.Descriptor) (service.State, error)
}

type EventUpcaster interface {
	UpcastEvent(ctx context.Context, event contract.StoredEvent, target service.Descriptor) (contract.StoredEvent, error)
}

type RestorerOptions struct {
	Journal   persistence.JournalStore
	Snapshots persistence.SnapshotStore
	Migrator  SnapshotMigrator
	Upcaster  EventUpcaster
}

type JournalRestorer struct {
	journal   persistence.JournalStore
	snapshots persistence.SnapshotStore
	migrator  SnapshotMigrator
	upcaster  EventUpcaster
}

func NewJournalRestorer(journal persistence.JournalStore, snapshots persistence.SnapshotStore) (*JournalRestorer, error) {
	return NewJournalRestorerWithOptions(RestorerOptions{Journal: journal, Snapshots: snapshots})
}

func NewJournalRestorerWithOptions(options RestorerOptions) (*JournalRestorer, error) {
	if options.Journal == nil || options.Snapshots == nil {
		return nil, fmt.Errorf("state restorer requires journal and snapshot stores")
	}
	return &JournalRestorer{
		journal: options.Journal, snapshots: options.Snapshots,
		migrator: options.Migrator, upcaster: options.Upcaster,
	}, nil
}

func (r *JournalRestorer) Restore(ctx context.Context, target service.Service, record instance.Record, config json.RawMessage) (RestoredState, error) {
	if r == nil || target == nil {
		return RestoredState{}, fmt.Errorf("state restorer and service are required")
	}
	descriptor := target.Descriptor()
	snapshot, found, err := r.snapshots.LoadLatest(ctx, record.StateStreamID)
	if err != nil {
		return RestoredState{}, fault.Wrap(fault.Retryable, "load_snapshot", err)
	}

	state, sequence, snapshotSequence, useSnapshot, err := r.restoreSnapshot(ctx, snapshot, found, descriptor, record)
	if err != nil {
		return RestoredState{}, err
	}
	if !useSnapshot {
		state, err = initialState(ctx, target, record, config)
		if err != nil {
			return RestoredState{}, fault.Wrap(fault.CorruptState, "initial_state", err)
		}
		sequence, snapshotSequence = 0, 0
	}

	head, err := r.journal.Head(ctx, record.StateStreamID)
	if err != nil {
		return RestoredState{}, fault.Wrap(fault.Retryable, "load_journal_head", err)
	}
	if sequence > head {
		return RestoredState{}, corrupt(record, fmt.Errorf("snapshot sequence %d is ahead of journal head %d", sequence, head))
	}
	events, err := r.journal.LoadStream(ctx, record.StateStreamID, sequence, 0)
	if err != nil {
		return RestoredState{}, fault.Wrap(fault.Retryable, "load_journal", err)
	}
	for _, stored := range events {
		if err := validateStoredEvent(stored, record, sequence+1); err != nil {
			return RestoredState{}, corrupt(record, err)
		}
		event := stored
		if event.ServiceVersion != record.DefinitionRef.Version {
			if r.upcaster == nil {
				return RestoredState{}, corrupt(record, fmt.Errorf("event %q service version %q does not match %q", event.EventID, event.ServiceVersion, record.DefinitionRef.Version))
			}
			event, err = r.upcaster.UpcastEvent(ctx, event, descriptor)
			if err != nil {
				return RestoredState{}, corrupt(record, fmt.Errorf("upcast event %q: %w", stored.EventID, err))
			}
			if err := validateStoredEvent(event, record, sequence+1); err != nil {
				return RestoredState{}, corrupt(record, fmt.Errorf("upcast event %q: %w", stored.EventID, err))
			}
			if event.ServiceVersion != record.DefinitionRef.Version {
				return RestoredState{}, corrupt(record, fmt.Errorf("upcast event %q still has service version %q", stored.EventID, event.ServiceVersion))
			}
		}
		state, err = target.Apply(state, event)
		if err != nil {
			return RestoredState{}, corrupt(record, fmt.Errorf("apply event %q: %w", event.EventID, err))
		}
		sequence = event.Sequence
	}
	if !descriptor.StateSchema.Empty() && state.SchemaVersion != descriptor.StateSchema.Version {
		return RestoredState{}, corrupt(record, fmt.Errorf("restored state schema version %d does not match descriptor version %d", state.SchemaVersion, descriptor.StateSchema.Version))
	}
	return RestoredState{State: state.Clone(), LastSequence: sequence, SnapshotSequence: snapshotSequence, ReplayedEvents: len(events)}, nil
}

func (r *JournalRestorer) restoreSnapshot(ctx context.Context, snapshot contract.Snapshot, found bool, descriptor service.Descriptor, record instance.Record) (service.State, uint64, uint64, bool, error) {
	if !found {
		return service.State{}, 0, 0, false, nil
	}
	validIdentity := snapshot.PlanRevision == record.PlanRevision && snapshot.StreamID == record.StateStreamID &&
		snapshot.OwnerService == record.Address && snapshot.AggregateType == string(record.DefinitionRef.Type)
	if !validIdentity || snapshot.Checksum == "" || snapshot.Checksum != contract.StateChecksum(snapshot.State) {
		return service.State{}, 0, 0, false, nil
	}
	if descriptor.StateSchema.Empty() || snapshot.SchemaVersion == descriptor.StateSchema.Version {
		state := service.State{SchemaVersion: snapshot.SchemaVersion, Data: contract.CloneRaw(snapshot.State)}
		return state, snapshot.LastSequence, snapshot.LastSequence, true, nil
	}
	if r.migrator == nil {
		return service.State{}, 0, 0, false, nil
	}
	state, err := r.migrator.MigrateSnapshot(ctx, snapshot.Clone(), descriptor)
	if err != nil {
		return service.State{}, 0, 0, false, corrupt(record, fmt.Errorf("migrate snapshot: %w", err))
	}
	if state.SchemaVersion != descriptor.StateSchema.Version {
		return service.State{}, 0, 0, false, corrupt(record, fmt.Errorf("migrated snapshot schema version %d does not match %d", state.SchemaVersion, descriptor.StateSchema.Version))
	}
	return state.Clone(), snapshot.LastSequence, snapshot.LastSequence, true, nil
}

func initialState(ctx context.Context, target service.Service, record instance.Record, config json.RawMessage) (service.State, error) {
	return target.InitialState(ctx, service.Init{
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		InstanceID: record.InstanceID, Address: record.Address,
		StateStreamID: record.StateStreamID, Config: contract.CloneRaw(config),
		Metadata: contract.CloneStrings(record.Metadata),
	})
}

func validateStoredEvent(event contract.StoredEvent, record instance.Record, expectedSequence uint64) error {
	if event.StreamID != record.StateStreamID || event.Sequence != expectedSequence {
		return fmt.Errorf("event stream %q has a sequence or stream mismatch at %d", record.StateStreamID, event.Sequence)
	}
	if event.EventID == "" || event.EventType == "" || event.EventVersion <= 0 {
		return fmt.Errorf("event at sequence %d has an invalid identity or contract", event.Sequence)
	}
	if event.PlanRevision != record.PlanRevision {
		return fmt.Errorf("event %q plan revision %q does not match %q", event.EventID, event.PlanRevision, record.PlanRevision)
	}
	return nil
}

func corrupt(record instance.Record, cause error) error {
	value := fault.New(fault.CorruptState, "restore_state", cause)
	value.RuntimeID = record.RuntimeID
	value.InstanceID = record.InstanceID
	return value
}

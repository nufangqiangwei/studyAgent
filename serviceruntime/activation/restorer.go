package activation

import (
	"agent/serviceruntime/contract"
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

type JournalRestorer struct {
	journal   persistence.JournalStore
	snapshots persistence.SnapshotStore
}

func NewJournalRestorer(journal persistence.JournalStore, snapshots persistence.SnapshotStore) (*JournalRestorer, error) {
	if journal == nil || snapshots == nil {
		return nil, fmt.Errorf("state restorer requires journal and snapshot stores")
	}
	return &JournalRestorer{journal: journal, snapshots: snapshots}, nil
}

func (r *JournalRestorer) Restore(ctx context.Context, target service.Service, record instance.Record, config json.RawMessage) (RestoredState, error) {
	if r == nil || target == nil {
		return RestoredState{}, fmt.Errorf("state restorer and service are required")
	}
	snapshot, found, err := r.snapshots.LoadLatest(ctx, record.StateStreamID)
	if err != nil {
		return RestoredState{}, err
	}
	var state service.State
	var sequence uint64
	if found {
		if snapshot.PlanRevision != record.PlanRevision || snapshot.StreamID != record.StateStreamID {
			return RestoredState{}, fmt.Errorf("snapshot does not belong to the service instance plan")
		}
		state = service.State{SchemaVersion: snapshot.SchemaVersion, Data: contract.CloneRaw(snapshot.State)}
		sequence = snapshot.LastSequence
	} else {
		state, err = target.InitialState(ctx, service.Init{
			RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
			InstanceID: record.InstanceID, Address: record.Address,
			StateStreamID: record.StateStreamID, Config: contract.CloneRaw(config),
			Metadata: contract.CloneStrings(record.Metadata),
		})
		if err != nil {
			return RestoredState{}, err
		}
	}
	events, err := r.journal.LoadStream(ctx, record.StateStreamID, sequence, 0)
	if err != nil {
		return RestoredState{}, err
	}
	for _, event := range events {
		if event.Sequence != sequence+1 {
			return RestoredState{}, fmt.Errorf("event stream %q has a sequence gap at %d", record.StateStreamID, event.Sequence)
		}
		state, err = target.Apply(state, event)
		if err != nil {
			return RestoredState{}, fmt.Errorf("apply event %q: %w", event.EventID, err)
		}
		sequence = event.Sequence
	}
	snapshotSequence := uint64(0)
	if found {
		snapshotSequence = snapshot.LastSequence
	}
	return RestoredState{State: state.Clone(), LastSequence: sequence, SnapshotSequence: snapshotSequence, ReplayedEvents: len(events)}, nil
}

package activation

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"testing"
	"time"
)

type restorerJournal struct{ events []contract.StoredEvent }

func (s restorerJournal) LoadStream(_ context.Context, _ contract.StreamID, after uint64, _ int) ([]contract.StoredEvent, error) {
	var result []contract.StoredEvent
	for _, event := range s.events {
		if event.Sequence > after {
			result = append(result, event.Clone())
		}
	}
	return result, nil
}

func (s restorerJournal) Head(context.Context, contract.StreamID) (uint64, error) {
	if len(s.events) == 0 {
		return 0, nil
	}
	return s.events[len(s.events)-1].Sequence, nil
}

type restorerSnapshots struct {
	snapshot contract.Snapshot
	found    bool
}

func (s restorerSnapshots) LoadLatest(context.Context, contract.StreamID) (contract.Snapshot, bool, error) {
	return s.snapshot.Clone(), s.found, nil
}

type replayCounter struct{}

func (replayCounter) Descriptor() service.Descriptor {
	return service.Descriptor{Component: contract.ComponentRef{Type: "counter", Version: "v1"}, StateSchema: contract.SchemaRef{Name: "counter-state", Version: 1}}
}

func (replayCounter) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{"count":0}`)}, nil
}

func (replayCounter) Handle(context.Context, service.State, contract.Message) (service.Decision, error) {
	return service.Decision{}, nil
}

func (replayCounter) Apply(state service.State, event contract.StoredEvent) (service.State, error) {
	var value struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(state.Data, &value); err != nil {
		return service.State{}, err
	}
	value.Count++
	data, _ := json.Marshal(value)
	return service.State{SchemaVersion: 1, Data: data}, nil
}

func TestRestorerDiscardsCorruptSnapshotAndReplaysJournal(t *testing.T) {
	record := restoreRecord()
	event := restoreEvent(record)
	snapshot := contract.Snapshot{
		StreamID: record.StateStreamID, AggregateType: "counter", OwnerService: record.Address,
		PlanRevision: record.PlanRevision, SchemaVersion: 1, LastSequence: 1,
		State: json.RawMessage(`{"count":99}`), Checksum: "corrupt",
	}
	restorer, err := NewJournalRestorer(restorerJournal{events: []contract.StoredEvent{event}}, restorerSnapshots{snapshot: snapshot, found: true})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := restorer.Restore(context.Background(), replayCounter{}, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	if restored.SnapshotSequence != 0 || restored.ReplayedEvents != 1 || string(restored.State.Data) != `{"count":1}` {
		t.Fatalf("restored=%#v state=%s", restored, restored.State.Data)
	}
}

func TestRestorerClassifiesCorruptJournal(t *testing.T) {
	record := restoreRecord()
	event := restoreEvent(record)
	event.PlanRevision = "other"
	restorer, err := NewJournalRestorer(restorerJournal{events: []contract.StoredEvent{event}}, restorerSnapshots{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = restorer.Restore(context.Background(), replayCounter{}, record, nil)
	if !fault.IsKind(err, fault.CorruptState) {
		t.Fatalf("error=%v, want corrupt state", err)
	}
}

func restoreRecord() instance.Record {
	return instance.Record{
		InstanceID: "counter-1", Address: "counter.main", Kind: instance.ServiceStatic,
		DefinitionRef: contract.ComponentRef{Type: "counter", Version: "v1"}, RuntimeID: "runtime", PlanRevision: "v1",
		MailboxID: "mailbox-1", StateStreamID: "service/counter-1", Lifecycle: instance.Declared,
	}
}

func restoreEvent(record instance.Record) contract.StoredEvent {
	return contract.StoredEvent{
		EventID: "event-1", StreamID: record.StateStreamID, StreamType: "counter", Sequence: 1,
		EventType: "counter.incremented", EventVersion: 1, PlanRevision: record.PlanRevision,
		ServiceVersion: record.DefinitionRef.Version, OccurredAt: time.Now().UTC(),
	}
}

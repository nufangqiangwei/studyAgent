package sqlite

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestConnectionStoreSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "connections.db")
	store, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	record := persistence.ConnectionRecord{
		ConnectionID: "connection-1", RuntimeID: "runtime-1", PlanRevision: "v1",
		OwnerInstanceID: "owner-instance-1", OwnerAddress: "owner.main",
		Key: "primary", Driver: "test", Config: []byte(`{"endpoint":"example"}`),
		DesiredOpen: true, Status: persistence.ConnectionOpen, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.Connections().Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, found, err := reopened.Connections().Get(ctx, contract.RuntimeID("runtime-1"), "connection-1")
	if err != nil {
		t.Fatal(err)
	}
	if !found || loaded.OwnerInstanceID != record.OwnerInstanceID || loaded.Status != persistence.ConnectionOpen || !loaded.DesiredOpen {
		t.Fatalf("reopened connection = %#v, found=%v", loaded, found)
	}
	closedAt := now.Add(time.Minute)
	loaded.DesiredOpen = false
	loaded.Status = persistence.ConnectionClosed
	loaded.UpdatedAt = closedAt
	loaded.ClosedAt = &closedAt
	if err := reopened.Connections().Update(ctx, loaded); err != nil {
		t.Fatal(err)
	}
	records, err := reopened.Connections().List(ctx, "runtime-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Status != persistence.ConnectionClosed || records[0].ClosedAt == nil {
		t.Fatalf("updated connections = %#v", records)
	}
}

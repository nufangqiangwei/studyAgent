package instance

import (
	"agent/serviceruntime/contract"
	"context"
	"fmt"
	"sync"
)

type DeliveryTarget struct {
	RuntimeID    contract.RuntimeID
	PlanRevision contract.PlanRevision
	Address      contract.ServiceAddress
	InstanceID   contract.ServiceInstanceID
	MailboxID    contract.MailboxID
}

type AddressResolver interface {
	ResolveAddress(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision, address contract.ServiceAddress) (DeliveryTarget, error)
}

type InstanceDirectory interface {
	AddressResolver
	Register(ctx context.Context, record Record) error
	Remove(ctx context.Context, instanceID contract.ServiceInstanceID) error
	Rebuild(ctx context.Context, records []Record) error
}

type Directory struct {
	store Store

	mu         sync.RWMutex
	byAddress  map[directoryKey]Record
	byInstance map[contract.ServiceInstanceID]directoryKey
}

type directoryKey struct {
	runtime  contract.RuntimeID
	revision contract.PlanRevision
	address  contract.ServiceAddress
}

func NewDirectory(store Store) (*Directory, error) {
	if store == nil {
		return nil, fmt.Errorf("instance store is required")
	}
	return &Directory{store: store, byAddress: make(map[directoryKey]Record), byInstance: make(map[contract.ServiceInstanceID]directoryKey)}, nil
}

func (d *Directory) Register(_ context.Context, record Record) error {
	if d == nil {
		return fmt.Errorf("instance directory is nil")
	}
	if err := record.Validate(); err != nil {
		return err
	}
	key := directoryKey{runtime: record.RuntimeID, revision: record.PlanRevision, address: record.Address}
	d.mu.Lock()
	defer d.mu.Unlock()
	if existing, ok := d.byAddress[key]; ok && existing.InstanceID != record.InstanceID {
		return fmt.Errorf("service address %q is already assigned to %q", record.Address, existing.InstanceID)
	}
	d.byAddress[key] = record.Clone()
	d.byInstance[record.InstanceID] = key
	return nil
}

func (d *Directory) Remove(_ context.Context, instanceID contract.ServiceInstanceID) error {
	if d == nil {
		return fmt.Errorf("instance directory is nil")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	key, ok := d.byInstance[instanceID]
	if !ok {
		return nil
	}
	delete(d.byInstance, instanceID)
	delete(d.byAddress, key)
	return nil
}

func (d *Directory) Rebuild(ctx context.Context, records []Record) error {
	if d == nil {
		return fmt.Errorf("instance directory is nil")
	}
	d.mu.Lock()
	d.byAddress = make(map[directoryKey]Record, len(records))
	d.byInstance = make(map[contract.ServiceInstanceID]directoryKey, len(records))
	d.mu.Unlock()
	for _, record := range records {
		if record.Lifecycle == Terminated {
			continue
		}
		if err := d.Register(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (d *Directory) ResolveAddress(ctx context.Context, runtimeID contract.RuntimeID, revision contract.PlanRevision, address contract.ServiceAddress) (DeliveryTarget, error) {
	if d == nil {
		return DeliveryTarget{}, fmt.Errorf("instance directory is nil")
	}
	key := directoryKey{runtime: runtimeID, revision: revision, address: address}
	d.mu.RLock()
	record, ok := d.byAddress[key]
	d.mu.RUnlock()
	if !ok {
		loaded, found, err := d.store.GetByAddress(ctx, runtimeID, revision, address)
		if err != nil {
			return DeliveryTarget{}, err
		}
		if !found || loaded.Lifecycle == Terminated {
			return DeliveryTarget{}, fmt.Errorf("service address %q is not available", address)
		}
		if err := d.Register(ctx, loaded); err != nil {
			return DeliveryTarget{}, err
		}
		record = loaded
	}
	return DeliveryTarget{RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision, Address: record.Address, InstanceID: record.InstanceID, MailboxID: record.MailboxID}, nil
}

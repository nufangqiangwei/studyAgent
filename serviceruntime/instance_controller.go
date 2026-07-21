package serviceruntime

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"fmt"
	"strings"
)

type instanceController struct {
	plan        *building.RuntimePlan
	definitions building.DefinitionResolver
	storage     persistence.RuntimeStorage
	directory   instance.InstanceDirectory
	ids         contract.IDGenerator
	clock       contract.Clock
}

func (c *instanceController) Declare(ctx context.Context, caller contract.ServiceAddress, declaration instance.Declaration) (instance.Record, error) {
	if c == nil || c.plan == nil || c.storage == nil || c.directory == nil {
		return instance.Record{}, fmt.Errorf("instance controller is unavailable")
	}
	if declaration.Address == "" || !declaration.Component.Valid() {
		return instance.Record{}, assembly.RejectControl("invalid_declaration", fmt.Errorf("dynamic instance address and component are required"))
	}
	if caller != "" {
		if err := c.authorize(ctx, caller, assembly.SystemOperationDeclareInstance); err != nil {
			return instance.Record{}, err
		}
	}
	definition, ok := c.definitions.ResolveDefinition(declaration.Component)
	if !ok {
		return instance.Record{}, assembly.RejectControl("definition_not_found", fmt.Errorf("service definition %q is not registered", declaration.Component.String()))
	}
	if definition.Scope != building.ScopeVirtual {
		return instance.Record{}, assembly.RejectControl("invalid_scope", fmt.Errorf("service definition %q has scope %q, want %q", declaration.Component.String(), definition.Scope, building.ScopeVirtual))
	}

	spec := c.plan.Runtime()
	existing, found, err := c.storage.Instances().GetByAddress(ctx, spec.ID, spec.Revision, declaration.Address)
	if err != nil {
		return instance.Record{}, err
	}
	if found {
		if err := declarationMatches(existing, declaration); err != nil {
			return instance.Record{}, assembly.RejectControl("declaration_conflict", err)
		}
		if err := c.directory.Register(ctx, existing); err != nil {
			return instance.Record{}, err
		}
		return existing, nil
	}

	instanceID := declaration.InstanceID
	if instanceID == "" {
		instanceID = contract.ServiceInstanceID(c.ids.Derive("service-instance", string(spec.ID), string(spec.Revision), string(declaration.Address)))
	}
	rootID := instanceID
	depth := 0
	if declaration.ParentID != "" {
		parent, parentFound, parentErr := c.storage.Instances().Get(ctx, declaration.ParentID)
		if parentErr != nil {
			return instance.Record{}, parentErr
		}
		if !parentFound || parent.RuntimeID != spec.ID || parent.PlanRevision != spec.Revision || parent.Lifecycle == instance.Terminated {
			return instance.Record{}, assembly.RejectControl("parent_not_available", fmt.Errorf("parent instance %q is not available in the runtime plan", declaration.ParentID))
		}
		rootID = parent.RootID
		if rootID == "" {
			rootID = parent.InstanceID
		}
		depth = parent.Depth + 1
	}
	now := c.clock.Now().UTC()
	record := instance.Record{
		InstanceID: instanceID, Address: declaration.Address, Kind: instance.ServiceVirtual,
		DefinitionRef: declaration.Component, RuntimeID: spec.ID, PlanRevision: spec.Revision,
		ParentID: declaration.ParentID, RootID: rootID, Depth: depth,
		MailboxID:     contract.MailboxID(c.ids.Derive("mailbox", string(instanceID))),
		StateStreamID: contract.StreamID("service/" + string(instanceID)),
		Lifecycle:     instance.Declared, CreatedAt: now, UpdatedAt: now,
		Metadata: contract.CloneStrings(declaration.Metadata),
	}
	if err := c.storage.Instances().Create(ctx, record); err != nil {
		return instance.Record{}, err
	}
	created, found, err := c.storage.Instances().Get(ctx, instanceID)
	if err != nil {
		return instance.Record{}, err
	}
	if !found {
		return instance.Record{}, fmt.Errorf("created service instance %q is unavailable", instanceID)
	}
	if err := c.directory.Register(ctx, created); err != nil {
		return instance.Record{}, err
	}
	return created, nil
}

func (c *instanceController) authorize(ctx context.Context, caller contract.ServiceAddress, operation string) error {
	spec := c.plan.Runtime()
	record, found, err := c.storage.Instances().GetByAddress(ctx, spec.ID, spec.Revision, caller)
	if err != nil {
		return err
	}
	if !found || record.Lifecycle == instance.Terminated {
		return assembly.RejectControl("caller_not_available", fmt.Errorf("system call source %q is not an available service instance", caller))
	}
	definition, found := c.definitions.ResolveDefinition(record.DefinitionRef)
	if !found {
		return assembly.RejectControl("caller_definition_not_found", fmt.Errorf("system call source definition %q is not registered", record.DefinitionRef.String()))
	}
	for _, allowed := range definition.SystemOperations {
		if strings.TrimSpace(allowed) == operation {
			return nil
		}
	}
	return assembly.RejectControl("operation_not_allowed", fmt.Errorf("service %q is not authorized for system operation %q", caller, operation))
}

func declarationMatches(existing instance.Record, declaration instance.Declaration) error {
	if existing.DefinitionRef != declaration.Component {
		return fmt.Errorf("dynamic address %q already uses component %q", declaration.Address, existing.DefinitionRef.String())
	}
	if declaration.InstanceID != "" && existing.InstanceID != declaration.InstanceID {
		return fmt.Errorf("dynamic address %q already uses instance id %q", declaration.Address, existing.InstanceID)
	}
	if existing.ParentID != declaration.ParentID {
		return fmt.Errorf("dynamic address %q already uses parent %q", declaration.Address, existing.ParentID)
	}
	if existing.Lifecycle == instance.Terminated {
		return fmt.Errorf("dynamic address %q is tombstoned", declaration.Address)
	}
	return nil
}

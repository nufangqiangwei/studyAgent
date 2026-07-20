package building

import (
	"agent/serviceruntime/contract"
	"sync"
)

type DefinitionCatalog struct {
	definitions map[contract.ComponentRef]ServiceDefinition
}

func newDefinitionCatalog(definitions map[contract.ComponentRef]ServiceDefinition) *DefinitionCatalog {
	cloned := make(map[contract.ComponentRef]ServiceDefinition, len(definitions))
	for ref, definition := range definitions {
		cloned[ref] = cloneDefinition(definition)
	}
	return &DefinitionCatalog{definitions: cloned}
}

func (c *DefinitionCatalog) ResolveDefinition(ref contract.ComponentRef) (ServiceDefinition, bool) {
	if c == nil {
		return ServiceDefinition{}, false
	}
	definition, found := c.definitions[ref]
	return cloneDefinition(definition), found
}

type PlanResolver interface {
	ResolvePlan(runtimeID contract.RuntimeID, revision contract.PlanRevision) (*RuntimePlan, bool)
}

type PlanCatalog struct {
	runtimeID contract.RuntimeID
	current   contract.PlanRevision

	mu    sync.RWMutex
	plans map[contract.PlanRevision]*RuntimePlan
}

func NewPlanCatalog(current *RuntimePlan, plans ...*RuntimePlan) *PlanCatalog {
	catalog := &PlanCatalog{plans: make(map[contract.PlanRevision]*RuntimePlan)}
	if current != nil {
		spec := current.Runtime()
		catalog.runtimeID = spec.ID
		catalog.current = spec.Revision
		catalog.plans[spec.Revision] = current
	}
	for _, plan := range plans {
		if plan == nil {
			continue
		}
		spec := plan.Runtime()
		if catalog.runtimeID == "" {
			catalog.runtimeID = spec.ID
		}
		if spec.ID == catalog.runtimeID {
			catalog.plans[spec.Revision] = plan
		}
	}
	return catalog
}

func (c *PlanCatalog) ResolvePlan(runtimeID contract.RuntimeID, revision contract.PlanRevision) (*RuntimePlan, bool) {
	if c == nil || runtimeID != c.runtimeID {
		return nil, false
	}
	c.mu.RLock()
	plan, found := c.plans[revision]
	c.mu.RUnlock()
	return plan, found
}

func (c *PlanCatalog) Current() (*RuntimePlan, bool) {
	if c == nil {
		return nil, false
	}
	return c.ResolvePlan(c.runtimeID, c.current)
}

func (c *PlanCatalog) Plans() []*RuntimePlan {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	result := make([]*RuntimePlan, 0, len(c.plans))
	for _, plan := range c.plans {
		result = append(result, plan)
	}
	c.mu.RUnlock()
	return result
}

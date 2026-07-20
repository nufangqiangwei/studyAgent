package building

import (
	"agent/serviceruntime/contract"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type CompileError struct {
	Issues []ValidationIssue
}

func (e *CompileError) Error() string {
	if e == nil || len(e.Issues) == 0 {
		return "runtime manifest is invalid"
	}
	return fmt.Sprintf("runtime manifest is invalid: %s: %s", e.Issues[0].Path, e.Issues[0].Message)
}

type Register struct {
	mu          sync.RWMutex
	definitions map[contract.ComponentRef]ServiceDefinition
	validators  []PlanValidator
	effects     map[string]struct{}
	schemas     SchemaValidator
}

func NewRegister(schemas SchemaValidator) *Register {
	return &Register{
		definitions: make(map[contract.ComponentRef]ServiceDefinition),
		effects:     make(map[string]struct{}),
		schemas:     schemas,
	}
}

func (r *Register) RegisterService(def ServiceDefinition) error {
	if r == nil {
		return fmt.Errorf("register is nil")
	}
	if !def.Component.Valid() {
		return fmt.Errorf("service component type and version are required")
	}
	if def.Factory == nil {
		return fmt.Errorf("service %q factory is required", def.Component.String())
	}
	if !def.Scope.Valid() {
		return fmt.Errorf("service %q scope %q is invalid", def.Component.String(), def.Scope)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.definitions[def.Component]; exists {
		return fmt.Errorf("service %q is already registered", def.Component.String())
	}
	r.definitions[def.Component] = cloneDefinition(def)
	return nil
}

func (r *Register) RegisterEffectExecutor(ref string) error {
	if r == nil {
		return fmt.Errorf("register is nil")
	}
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return fmt.Errorf("effect executor ref is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.effects[ref]; exists {
		return fmt.Errorf("effect executor %q is already registered", ref)
	}
	r.effects[ref] = struct{}{}
	return nil
}

func (r *Register) RegisterPlanValidator(validator PlanValidator) error {
	if r == nil {
		return fmt.Errorf("register is nil")
	}
	if validator == nil {
		return fmt.Errorf("plan validator is required")
	}
	r.mu.Lock()
	r.validators = append(r.validators, validator)
	r.mu.Unlock()
	return nil
}

func (r *Register) ResolveDefinition(ref contract.ComponentRef) (ServiceDefinition, bool) {
	if r == nil {
		return ServiceDefinition{}, false
	}
	r.mu.RLock()
	definition, ok := r.definitions[ref]
	r.mu.RUnlock()
	return cloneDefinition(definition), ok
}

func (r *Register) FreezeDefinitions() *DefinitionCatalog {
	if r == nil {
		return newDefinitionCatalog(nil)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return newDefinitionCatalog(r.definitions)
}

func (r *Register) Compile(ctx context.Context, manifest RuntimeManifest) (*RuntimePlan, error) {
	if r == nil {
		return nil, fmt.Errorf("register is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	definitions := make(map[contract.ComponentRef]ServiceDefinition, len(r.definitions))
	for ref, definition := range r.definitions {
		definitions[ref] = cloneDefinition(definition)
	}
	effects := make(map[string]struct{}, len(r.effects))
	for ref := range r.effects {
		effects[ref] = struct{}{}
	}
	validators := append([]PlanValidator(nil), r.validators...)
	schemas := r.schemas
	r.mu.RUnlock()

	issues := validateManifest(ctx, manifest, definitions, effects, schemas)
	if len(issues) > 0 {
		return nil, &CompileError{Issues: issues}
	}
	plan := compilePlan(manifest, definitions, effects)
	view := CompileView{Manifest: cloneManifest(manifest), Plan: plan, Definitions: r}
	for _, validator := range validators {
		issues = append(issues, validator.ValidatePlan(ctx, view)...)
	}
	if len(issues) > 0 {
		return nil, &CompileError{Issues: issues}
	}
	return plan, nil
}

func validateManifest(ctx context.Context, manifest RuntimeManifest, definitions map[contract.ComponentRef]ServiceDefinition, effects map[string]struct{}, schemas SchemaValidator) []ValidationIssue {
	var issues []ValidationIssue
	if strings.TrimSpace(string(manifest.Runtime.ID)) == "" {
		issues = appendIssue(issues, "runtime.id", "required", "runtime id is required")
	}
	if strings.TrimSpace(string(manifest.Runtime.Revision)) == "" {
		issues = appendIssue(issues, "runtime.revision", "required", "plan revision is required")
	}
	for ref, definition := range definitions {
		if definition.Scope != ScopeVirtual {
			continue
		}
		for _, executorRef := range definition.EffectExecutors {
			if _, ok := effects[strings.TrimSpace(executorRef)]; !ok {
				issues = appendIssue(issues, "definitions."+ref.String()+".effects", "not_found", fmt.Sprintf("effect executor %q is not registered", executorRef))
			}
		}
	}
	mounts := make(map[contract.ServiceAddress]ServiceMount, len(manifest.Services))
	definitionsByAddress := make(map[contract.ServiceAddress]ServiceDefinition, len(manifest.Services))
	singletons := make(map[contract.ComponentRef]contract.ServiceAddress)
	for index, mount := range manifest.Services {
		path := fmt.Sprintf("services[%d]", index)
		if strings.TrimSpace(string(mount.Address)) == "" {
			issues = appendIssue(issues, path+".address", "required", "service address is required")
			continue
		}
		if _, exists := mounts[mount.Address]; exists {
			issues = appendIssue(issues, path+".address", "duplicate", fmt.Sprintf("service address %q is duplicated", mount.Address))
			continue
		}
		mounts[mount.Address] = mount
		definition, ok := definitions[mount.Component]
		if !ok {
			issues = appendIssue(issues, path+".component", "not_found", fmt.Sprintf("component %q is not registered", mount.Component.String()))
			continue
		}
		definitionsByAddress[mount.Address] = definition
		if definition.Scope == ScopeRuntimeSingleton {
			if previous, exists := singletons[mount.Component]; exists {
				issues = appendIssue(issues, path+".component", "singleton", fmt.Sprintf("singleton component is already mounted at %q", previous))
			} else {
				singletons[mount.Component] = mount.Address
			}
		}
		if !definition.ConfigSchema.Empty() {
			if schemas == nil {
				issues = appendIssue(issues, path+".config", "schema_unavailable", "config schema validator is not configured")
			} else if err := schemas.Validate(ctx, definition.ConfigSchema, mount.Config); err != nil {
				issues = appendIssue(issues, path+".config", "schema", err.Error())
			}
		}
		for _, ref := range definition.EffectExecutors {
			if _, ok := effects[strings.TrimSpace(ref)]; !ok {
				issues = appendIssue(issues, path+".effects", "not_found", fmt.Sprintf("effect executor %q is not registered", ref))
			}
		}
	}
	issues = append(issues, validateDependencies(manifest.Services, mounts, definitionsByAddress)...)
	issues = append(issues, validateRoutes(manifest.Routes, mounts, definitionsByAddress)...)
	return issues
}

func validateDependencies(services []ServiceMount, mounts map[contract.ServiceAddress]ServiceMount, definitions map[contract.ServiceAddress]ServiceDefinition) []ValidationIssue {
	var issues []ValidationIssue
	graph := make(map[contract.ServiceAddress][]contract.ServiceAddress, len(services))
	for index, mount := range services {
		definition, ok := definitions[mount.Address]
		if !ok {
			continue
		}
		declared := make(map[string]ServiceDependency, len(definition.Dependencies))
		for _, dependency := range definition.Dependencies {
			declared[dependency.Name] = dependency
			target := mount.Dependencies[dependency.Name]
			if target == "" {
				if dependency.Required {
					issues = appendIssue(issues, fmt.Sprintf("services[%d].dependencies.%s", index, dependency.Name), "required", "required dependency is not bound")
				}
				continue
			}
			targetMount, exists := mounts[target]
			if !exists {
				issues = appendIssue(issues, fmt.Sprintf("services[%d].dependencies.%s", index, dependency.Name), "not_found", fmt.Sprintf("dependency target %q does not exist", target))
				continue
			}
			if !acceptsType(dependency.AcceptedTypes, targetMount.Component.Type) {
				issues = appendIssue(issues, fmt.Sprintf("services[%d].dependencies.%s", index, dependency.Name), "type", fmt.Sprintf("component type %q is not accepted", targetMount.Component.Type))
			}
			graph[mount.Address] = append(graph[mount.Address], target)
		}
		for name := range mount.Dependencies {
			if _, exists := declared[name]; !exists {
				issues = appendIssue(issues, fmt.Sprintf("services[%d].dependencies.%s", index, name), "undeclared", "dependency is not declared by the service definition")
			}
		}
	}
	if cycle := findCycle(graph); len(cycle) > 0 {
		parts := make([]string, 0, len(cycle))
		for _, address := range cycle {
			parts = append(parts, string(address))
		}
		issues = appendIssue(issues, "services.dependencies", "cycle", "dependency cycle: "+strings.Join(parts, " -> "))
	}
	return issues
}

func validateRoutes(routes RouteManifest, mounts map[contract.ServiceAddress]ServiceMount, definitions map[contract.ServiceAddress]ServiceDefinition) []ValidationIssue {
	var issues []ValidationIssue
	for messageType, address := range routes.Commands {
		issues = append(issues, validateRoute("routes.commands", contract.MessageCommand, messageType, address, mounts, definitions)...)
	}
	for messageType, address := range routes.Queries {
		issues = append(issues, validateRoute("routes.queries", contract.MessageQuery, messageType, address, mounts, definitions)...)
	}
	for messageType, addresses := range routes.Events {
		seen := make(map[contract.ServiceAddress]struct{}, len(addresses))
		for _, address := range addresses {
			if _, exists := seen[address]; exists {
				issues = appendIssue(issues, "routes.events."+string(messageType), "duplicate", fmt.Sprintf("event subscriber %q is duplicated", address))
				continue
			}
			seen[address] = struct{}{}
			issues = append(issues, validateRoute("routes.events", contract.MessageEvent, messageType, address, mounts, definitions)...)
		}
	}
	return issues
}

func validateRoute(prefix string, kind contract.MessageKind, messageType contract.MessageType, address contract.ServiceAddress, mounts map[contract.ServiceAddress]ServiceMount, definitions map[contract.ServiceAddress]ServiceDefinition) []ValidationIssue {
	path := prefix + "." + string(messageType)
	if strings.TrimSpace(string(messageType)) == "" {
		return []ValidationIssue{{Path: path, Code: "required", Message: "message type is required"}}
	}
	if _, exists := mounts[address]; !exists {
		return []ValidationIssue{{Path: path, Code: "not_found", Message: fmt.Sprintf("route target %q does not exist", address)}}
	}
	definition := definitions[address]
	for _, consumed := range definition.Consumes {
		if consumed.Kind == kind && consumed.Type == messageType {
			return nil
		}
	}
	return []ValidationIssue{{Path: path, Code: "contract", Message: fmt.Sprintf("target %q does not declare consuming %s %q", address, kind, messageType)}}
}

func compilePlan(manifest RuntimeManifest, definitions map[contract.ComponentRef]ServiceDefinition, effects map[string]struct{}) *RuntimePlan {
	services := make(map[contract.ServiceAddress]PlannedService, len(manifest.Services))
	routing := RoutingTable{
		commands: make(map[contract.MessageType]contract.ServiceAddress, len(manifest.Routes.Commands)),
		queries:  make(map[contract.MessageType]contract.ServiceAddress, len(manifest.Routes.Queries)),
		events:   make(map[contract.MessageType][]contract.ServiceAddress, len(manifest.Routes.Events)),
		services: make(map[contract.ServiceAddress]struct{}, len(manifest.Services)),
	}
	for _, mount := range manifest.Services {
		services[mount.Address] = PlannedService{
			Address: mount.Address, Component: mount.Component,
			Config:       append([]byte(nil), mount.Config...),
			Dependencies: cloneDependencies(mount.Dependencies),
			Metadata:     contract.CloneStrings(mount.Metadata),
		}
		routing.services[mount.Address] = struct{}{}
	}
	for key, value := range manifest.Routes.Commands {
		routing.commands[key] = value
	}
	for key, value := range manifest.Routes.Queries {
		routing.queries[key] = value
	}
	for key, value := range manifest.Routes.Events {
		routing.events[key] = append([]contract.ServiceAddress(nil), value...)
	}
	knownEffects := make(map[string]struct{}, len(effects))
	for ref := range effects {
		knownEffects[ref] = struct{}{}
	}
	return &RuntimePlan{runtime: manifest.Runtime, services: services, routing: routing, recovery: manifest.Recovery.WithDefaults(), effects: knownEffects}
}

func cloneDefinition(definition ServiceDefinition) ServiceDefinition {
	definition.Consumes = append([]MessageContract(nil), definition.Consumes...)
	definition.Produces = append([]MessageContract(nil), definition.Produces...)
	definition.EffectExecutors = append([]string(nil), definition.EffectExecutors...)
	if len(definition.Dependencies) > 0 {
		dependencies := make([]ServiceDependency, len(definition.Dependencies))
		for index, dependency := range definition.Dependencies {
			dependency.AcceptedTypes = append([]contract.ServiceType(nil), dependency.AcceptedTypes...)
			dependencies[index] = dependency
		}
		definition.Dependencies = dependencies
	}
	return definition
}

func cloneManifest(manifest RuntimeManifest) RuntimeManifest {
	manifest.Services = append([]ServiceMount(nil), manifest.Services...)
	for index := range manifest.Services {
		manifest.Services[index].Config = contract.CloneRaw(manifest.Services[index].Config)
		manifest.Services[index].Dependencies = cloneDependencies(manifest.Services[index].Dependencies)
		manifest.Services[index].Metadata = contract.CloneStrings(manifest.Services[index].Metadata)
	}
	manifest.Routes = cloneRoutes(manifest.Routes)
	return manifest
}

func cloneRoutes(routes RouteManifest) RouteManifest {
	cloned := RouteManifest{
		Commands: make(map[contract.MessageType]contract.ServiceAddress, len(routes.Commands)),
		Queries:  make(map[contract.MessageType]contract.ServiceAddress, len(routes.Queries)),
		Events:   make(map[contract.MessageType][]contract.ServiceAddress, len(routes.Events)),
	}
	for key, value := range routes.Commands {
		cloned.Commands[key] = value
	}
	for key, value := range routes.Queries {
		cloned.Queries[key] = value
	}
	for key, value := range routes.Events {
		cloned.Events[key] = append([]contract.ServiceAddress(nil), value...)
	}
	return cloned
}

func cloneDependencies(values map[string]contract.ServiceAddress) map[string]contract.ServiceAddress {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]contract.ServiceAddress, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func acceptsType(accepted []contract.ServiceType, candidate contract.ServiceType) bool {
	if len(accepted) == 0 {
		return true
	}
	for _, value := range accepted {
		if value == candidate {
			return true
		}
	}
	return false
}

func findCycle(graph map[contract.ServiceAddress][]contract.ServiceAddress) []contract.ServiceAddress {
	const (
		unvisited = iota
		visiting
		visited
	)
	state := make(map[contract.ServiceAddress]int, len(graph))
	var stack []contract.ServiceAddress
	var visit func(contract.ServiceAddress) []contract.ServiceAddress
	visit = func(node contract.ServiceAddress) []contract.ServiceAddress {
		state[node] = visiting
		stack = append(stack, node)
		for _, next := range graph[node] {
			if state[next] == visiting {
				start := 0
				for index, value := range stack {
					if value == next {
						start = index
						break
					}
				}
				cycle := append([]contract.ServiceAddress(nil), stack[start:]...)
				return append(cycle, next)
			}
			if state[next] == unvisited {
				if cycle := visit(next); len(cycle) > 0 {
					return cycle
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[node] = visited
		return nil
	}
	nodes := make([]string, 0, len(graph))
	for node := range graph {
		nodes = append(nodes, string(node))
	}
	sort.Strings(nodes)
	for _, raw := range nodes {
		node := contract.ServiceAddress(raw)
		if state[node] == unvisited {
			if cycle := visit(node); len(cycle) > 0 {
				return cycle
			}
		}
	}
	return nil
}

func appendIssue(issues []ValidationIssue, path, code, message string) []ValidationIssue {
	return append(issues, ValidationIssue{Path: path, Code: code, Message: message})
}

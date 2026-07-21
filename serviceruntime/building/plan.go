package building

import (
	"agent/serviceruntime/contract"
	"fmt"
	"sort"
	"strings"
)

type PlannedService struct {
	Address      contract.ServiceAddress
	Component    contract.ComponentRef
	Config       []byte
	Dependencies map[string]contract.ServiceAddress
	Metadata     map[string]string
}

func (s PlannedService) clone() PlannedService {
	s.Config = append([]byte(nil), s.Config...)
	if len(s.Dependencies) > 0 {
		dependencies := make(map[string]contract.ServiceAddress, len(s.Dependencies))
		for name, address := range s.Dependencies {
			dependencies[name] = address
		}
		s.Dependencies = dependencies
	} else {
		s.Dependencies = nil
	}
	s.Metadata = contract.CloneStrings(s.Metadata)
	return s
}

type RuntimePlan struct {
	runtime  RuntimeSpec
	services map[contract.ServiceAddress]PlannedService
	routing  RoutingTable
	recovery RecoveryPolicy
	payloads InlinePayloadPolicy
	effects  map[string]struct{}
}

func (p *RuntimePlan) Runtime() RuntimeSpec {
	if p == nil {
		return RuntimeSpec{}
	}
	return p.runtime
}

func (p *RuntimePlan) Service(address contract.ServiceAddress) (PlannedService, bool) {
	if p == nil {
		return PlannedService{}, false
	}
	value, ok := p.services[address]
	return value.clone(), ok
}

func (p *RuntimePlan) Services() []PlannedService {
	if p == nil {
		return nil
	}
	addresses := make([]string, 0, len(p.services))
	for address := range p.services {
		addresses = append(addresses, string(address))
	}
	sort.Strings(addresses)
	services := make([]PlannedService, 0, len(addresses))
	for _, address := range addresses {
		services = append(services, p.services[contract.ServiceAddress(address)].clone())
	}
	return services
}

func (p *RuntimePlan) Routing() RoutingTable {
	if p == nil {
		return RoutingTable{}
	}
	return p.routing.clone()
}

func (p *RuntimePlan) Recovery() RecoveryPolicy {
	if p == nil {
		return RecoveryPolicy{}
	}
	return p.recovery
}

func (p *RuntimePlan) Payloads() InlinePayloadPolicy {
	if p == nil {
		return InlinePayloadPolicy{}
	}
	return p.payloads
}

func (p *RuntimePlan) KnowsEffect(ref string) bool {
	if p == nil {
		return false
	}
	_, ok := p.effects[strings.TrimSpace(ref)]
	return ok
}

type RoutingTable struct {
	commands map[contract.MessageType]contract.ServiceAddress
	queries  map[contract.MessageType]contract.ServiceAddress
	events   map[contract.MessageType][]contract.ServiceAddress
	services map[contract.ServiceAddress]struct{}
}

func (t RoutingTable) Resolve(message contract.Message) ([]contract.ServiceAddress, error) {
	switch message.Kind {
	case contract.MessageCommand, contract.MessageQuery:
		if message.To != "" {
			return []contract.ServiceAddress{message.To}, nil
		}
		var target contract.ServiceAddress
		if message.Kind == contract.MessageCommand {
			target = t.commands[message.Type]
		} else {
			target = t.queries[message.Type]
		}
		if target == "" {
			return nil, fmt.Errorf("no %s route for %q", message.Kind, message.Type)
		}
		return []contract.ServiceAddress{target}, nil
	case contract.MessageEvent:
		if message.To != "" {
			return []contract.ServiceAddress{message.To}, nil
		}
		return append([]contract.ServiceAddress(nil), t.events[message.Type]...), nil
	case contract.MessageReply:
		if message.To == "" {
			return nil, fmt.Errorf("reply %q requires a target", message.Type)
		}
		return []contract.ServiceAddress{message.To}, nil
	default:
		return nil, fmt.Errorf("unsupported message kind %q", message.Kind)
	}
}

func (t RoutingTable) clone() RoutingTable {
	cloned := RoutingTable{
		commands: make(map[contract.MessageType]contract.ServiceAddress, len(t.commands)),
		queries:  make(map[contract.MessageType]contract.ServiceAddress, len(t.queries)),
		events:   make(map[contract.MessageType][]contract.ServiceAddress, len(t.events)),
		services: make(map[contract.ServiceAddress]struct{}, len(t.services)),
	}
	for key, value := range t.commands {
		cloned.commands[key] = value
	}
	for key, value := range t.queries {
		cloned.queries[key] = value
	}
	for key, value := range t.events {
		cloned.events[key] = append([]contract.ServiceAddress(nil), value...)
	}
	for key := range t.services {
		cloned.services[key] = struct{}{}
	}
	return cloned
}

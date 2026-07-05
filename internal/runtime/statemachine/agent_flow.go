package statemachine

import (
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/reactor"
	"context"
	"fmt"
	"strings"
)

const AnyAgentPhase AgentPhase = "*"

type AgentFlowMachine interface {
	InitialPhase() AgentPhase
	HandleAgentEvent(ctx context.Context, state TaskState, event eventbus.Event) (AgentFlowResult, error)
}

type AgentFlowResult struct {
	Handled   bool             `json:"handled"`
	NextPhase AgentPhase       `json:"next_phase,omitempty"`
	Effects   []reactor.Effect `json:"effects,omitempty"`
}

func (r AgentFlowResult) Clone() AgentFlowResult {
	cloned := r
	if len(r.Effects) > 0 {
		cloned.Effects = make([]reactor.Effect, 0, len(r.Effects))
		for _, effect := range r.Effects {
			cloned.Effects = append(cloned.Effects, effect.Clone())
		}
	}
	return cloned
}

type AgentFlowRegistry struct {
	flows map[string]AgentFlowMachine
}

func NewAgentFlowRegistry() *AgentFlowRegistry {
	return &AgentFlowRegistry{flows: make(map[string]AgentFlowMachine)}
}

func (r *AgentFlowRegistry) Register(agent string, flow AgentFlowMachine) error {
	if r == nil {
		return fmt.Errorf("agent flow registry is nil")
	}
	agent = strings.TrimSpace(agent)
	if agent == "" {
		return fmt.Errorf("agent name is required")
	}
	if flow == nil {
		return fmt.Errorf("agent flow %q is required", agent)
	}
	if _, exists := r.flows[agent]; exists {
		return fmt.Errorf("agent flow %q already exists", agent)
	}
	r.flows[agent] = flow
	return nil
}

func (r *AgentFlowRegistry) Lookup(agent string) (AgentFlowMachine, bool) {
	if r == nil {
		return nil, false
	}
	flow, ok := r.flows[agent]
	return flow, ok
}

type AgentFlowDefinition struct {
	Initial     AgentPhase
	Transitions []AgentTransition
}

type AgentTransition struct {
	From      AgentPhase
	EventType eventbus.EventType
	To        AgentPhase
	Effects   []EffectTemplate
}

type EffectTemplate struct {
	Type     reactor.EffectType
	Payload  any
	Metadata map[string]string
}

func NewEffectTemplate(effectType reactor.EffectType, payload any) EffectTemplate {
	return EffectTemplate{Type: effectType, Payload: payload}
}

type ConfiguredAgentFlow struct {
	initial     AgentPhase
	transitions []AgentTransition
}

func NewConfiguredAgentFlow(definition AgentFlowDefinition) (*ConfiguredAgentFlow, error) {
	if strings.TrimSpace(string(definition.Initial)) == "" {
		return nil, fmt.Errorf("agent flow initial phase is required")
	}
	flow := &ConfiguredAgentFlow{
		initial:     definition.Initial,
		transitions: make([]AgentTransition, 0, len(definition.Transitions)),
	}
	for _, transition := range definition.Transitions {
		if strings.TrimSpace(string(transition.From)) == "" {
			return nil, fmt.Errorf("agent transition for event %q: from phase is required", transition.EventType)
		}
		if strings.TrimSpace(string(transition.EventType)) == "" {
			return nil, fmt.Errorf("agent transition from %q: event type is required", transition.From)
		}
		if strings.TrimSpace(string(transition.To)) == "" {
			return nil, fmt.Errorf("agent transition %q from %q: to phase is required", transition.EventType, transition.From)
		}
		flow.transitions = append(flow.transitions, transition)
	}
	return flow, nil
}

func (f *ConfiguredAgentFlow) InitialPhase() AgentPhase {
	if f == nil {
		return AgentPhaseUnknown
	}
	return f.initial
}

func (f *ConfiguredAgentFlow) HandleAgentEvent(_ context.Context, state TaskState, event eventbus.Event) (AgentFlowResult, error) {
	if f == nil {
		return AgentFlowResult{}, fmt.Errorf("agent flow is nil")
	}
	current := state.Agent.Phase
	if current == AgentPhaseUnknown {
		current = f.initial
	}

	seenEvent := false
	for _, transition := range f.transitions {
		if transition.EventType != event.Type {
			continue
		}
		seenEvent = true
		if transition.From != current && transition.From != AnyAgentPhase {
			continue
		}
		effects, err := instantiateEffects(state.TaskID, transition.Effects)
		if err != nil {
			return AgentFlowResult{}, err
		}
		return AgentFlowResult{
			Handled:   true,
			NextPhase: transition.To,
			Effects:   effects,
		}, nil
	}
	if seenEvent {
		return AgentFlowResult{}, illegalEvent(state, event, fmt.Sprintf("agent phase %s rejects event", current))
	}
	return AgentFlowResult{Handled: false}, nil
}

func instantiateEffects(taskID string, templates []EffectTemplate) ([]reactor.Effect, error) {
	if len(templates) == 0 {
		return nil, nil
	}
	effects := make([]reactor.Effect, 0, len(templates))
	for _, template := range templates {
		effect, err := reactor.NewEffect(taskID, template.Type, template.Payload, reactor.WithEffectMetadata(template.Metadata))
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}

package agents

import (
	"agent/internal/runtime/reactor"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type AgentExecutor struct {
	registry *Registry
}

func NewAgentExecutor(registry *Registry) (*AgentExecutor, error) {
	if registry == nil {
		return nil, fmt.Errorf("agent executor: registry is required")
	}
	return &AgentExecutor{registry: registry}, nil
}

func (e *AgentExecutor) ExecuteEffect(ctx context.Context, runtime reactor.TaskRuntime, effect reactor.Effect) (reactor.EffectResult, error) {
	if e == nil {
		return reactor.EffectResult{}, fmt.Errorf("agent executor is nil")
	}
	agentName := strings.TrimSpace(runtime.Agent)
	if agentName == "" {
		agentName = strings.TrimSpace(effect.Metadata["agent"])
	}
	if agentName == "" {
		agentName = agentNameFromStartPayload(effect.Payload)
	}
	if agentName == "" {
		return reactor.EffectResult{}, fmt.Errorf("agent executor: agent name is required")
	}
	agent, ok := e.registry.Lookup(agentName)
	if !ok {
		return reactor.EffectResult{}, fmt.Errorf("agent executor: agent %q not found", agentName)
	}

	switch effect.Type {
	case reactor.EffectAgentStart:
		result, err := agent.Start(ctx, startInputFromEffect(effect))
		if err != nil {
			return reactor.EffectResult{}, err
		}
		return reactor.EffectResult{Events: result.Events}, nil
	case reactor.EffectAgentResume:
		result, err := agent.Resume(ctx, AgentResumeInput{
			TaskID:   effect.TaskID,
			Payload:  append(json.RawMessage(nil), effect.Payload...),
			Metadata: cloneStringMap(effect.Metadata),
		})
		if err != nil {
			return reactor.EffectResult{}, err
		}
		return reactor.EffectResult{Events: result.Events}, nil
	default:
		return reactor.EffectResult{}, fmt.Errorf("agent executor: unsupported effect type %q", effect.Type)
	}
}

func startInputFromEffect(effect reactor.Effect) AgentStartInput {
	var payload statemachine.TaskStartPayload
	_ = json.Unmarshal(effect.Payload, &payload)
	metadata := cloneStringMap(payload.Metadata)
	if metadata == nil {
		metadata = cloneStringMap(effect.Metadata)
	}
	return AgentStartInput{
		TaskID:   effect.TaskID,
		Input:    payload.Input,
		Metadata: metadata,
	}
}

func agentNameFromStartPayload(raw json.RawMessage) string {
	var payload statemachine.TaskStartPayload
	if len(raw) == 0 || json.Unmarshal(raw, &payload) != nil {
		return ""
	}
	return payload.Agent
}

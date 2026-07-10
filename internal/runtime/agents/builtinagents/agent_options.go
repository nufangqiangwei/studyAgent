package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"strings"
	"time"
)

type AgentOption func(*agentConfig)

type AnalyzeAgentOption = AgentOption

type agentConfig struct {
	name         string
	model        agents2.ModelClient
	modelName    string
	temperature  float64
	store        agents2.SnapshotStore
	tools        []agents2.ToolSpec
	systemPrompt string
	source       string
	clock        func() time.Time
	maxTurns     int
	hooks        AgentRuntimeHooks
}

func withAgentName(name string) AgentOption {
	return func(config *agentConfig) {
		config.name = strings.TrimSpace(name)
	}
}

func WithModel(model agents2.ModelClient) AgentOption {
	return func(config *agentConfig) {
		config.model = model
	}
}

func WithModelName(model string) AgentOption {
	return func(config *agentConfig) {
		config.modelName = strings.TrimSpace(model)
	}
}

func WithModelTemperature(temperature float64) AgentOption {
	return func(config *agentConfig) {
		config.temperature = temperature
	}
}

func WithSnapshotStore(store agents2.SnapshotStore) AgentOption {
	return func(config *agentConfig) {
		config.store = store
	}
}

func WithTools(tools []agents2.ToolSpec) AgentOption {
	return func(config *agentConfig) {
		config.tools = cloneToolSpecs(tools)
	}
}

func WithSystemPrompt(prompt string) AgentOption {
	return func(config *agentConfig) {
		config.systemPrompt = strings.TrimSpace(prompt)
	}
}

func WithAgentSource(source string) AgentOption {
	return func(config *agentConfig) {
		config.source = source
	}
}

func WithAgentClock(clock func() time.Time) AgentOption {
	return func(config *agentConfig) {
		config.clock = clock
	}
}

func WithMaxTurns(maxTurns int) AgentOption {
	return func(config *agentConfig) {
		config.maxTurns = maxTurns
	}
}

func WithAgentRuntimeHooks(hooks AgentRuntimeHooks) AgentOption {
	return func(config *agentConfig) {
		config.hooks = hooks
	}
}

package builtinagents

import agents2 "agent/internal/runtime/agents"

// NewFactoryRegistry registers the built-in agent modules. Application code
// depends only on the registry and does not need construction branches for
// individual agents.
func NewFactoryRegistry() (*agents2.FactoryRegistry, error) {
	return agents2.NewFactoryRegistry(
		agents2.FactorySpec{Name: AnalyzeAgentName, Factory: newAnalyzeFromFactory},
		agents2.FactorySpec{Name: DefaultAgentName, Factory: newDefaultFromFactory},
		agents2.FactorySpec{Name: ToolsTesterAgentName, Factory: newToolsTesterFromFactory},
	)
}

func DefaultFactoryName() string {
	return AnalyzeAgentName
}

func newAnalyzeFromFactory(config agents2.FactoryConfig) (agents2.Agent, error) {
	return NewAnalyzeAgent(factoryOptions(config)...)
}

func newDefaultFromFactory(config agents2.FactoryConfig) (agents2.Agent, error) {
	return NewDefaultAgent(factoryOptions(config)...)
}

func newToolsTesterFromFactory(config agents2.FactoryConfig) (agents2.Agent, error) {
	return NewToolsTesterAgent(factoryOptions(config)...)
}

func factoryOptions(config agents2.FactoryConfig) []AgentOption {
	options := []AgentOption{
		WithModelName(config.ModelName),
		WithSnapshotStore(config.SnapshotStore),
		WithTools(config.Tools),
		WithAgentSource(config.Source),
	}
	if config.MaxTurns > 0 {
		options = append(options, WithMaxTurns(config.MaxTurns))
	}
	return options
}

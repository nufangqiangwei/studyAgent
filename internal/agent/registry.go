package agent

import (
	"agent/internal/capability/tool"
	"agent/internal/foundation/policy"
	"agent/internal/session"
	"context"
	"fmt"
	systemIO "io"
	"sort"
)

type CreatAgentOptions struct {
	LLM      LLMClient
	Model    string
	Logger   Logger
	MaxSteps int
	WorkDir  string
	In       systemIO.Reader
	Out      systemIO.Writer
	Session  session.Recorder
	Policy   policy.Checker
}

type Agent interface {
	Name() string
	Tools() []tool.Tool
	Run(context.Context, string) error
}
type NewAgent func(context.Context, CreatAgentOptions) (Agent, error)

type Registry struct {
	createAgent map[string]NewAgent
}

func (r *Registry) ListAgentNames() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.createAgent))
	for name := range r.createAgent {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) SelectAgent(name string) (NewAgent, error) {
	if r == nil {
		return nil, fmt.Errorf("agent registry is nil")
	}
	agentFactory, ok := r.createAgent[name]
	if !ok {
		return nil, fmt.Errorf("agent %s not found", name)
	}
	return agentFactory, nil
}

var Catalog *Registry

func RegisteredAgentNames() []string {
	return Catalog.ListAgentNames()
}

func init() {
	Catalog = &Registry{
		createAgent: make(map[string]NewAgent),
	}
	Catalog.createAgent[DefaultAgentName] = NewDefaultAgent
	Catalog.createAgent[AnalyzeAgentName] = NewAnalyzeAgent
	Catalog.createAgent[ToolsTesterAgentName] = NewToolsTesterAgent

}

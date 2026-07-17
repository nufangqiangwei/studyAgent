package command

import (
	"agent/internal/content"
	"context"
	"fmt"
	"strings"
)

type Agent struct{}

func (Agent) Name() string {
	return "agent"
}

func (Agent) Description() string {
	return "show active and available agents"
}

func (Agent) Execute(_ context.Context, env content.Env, _ []string) error {
	useAgentName := activeAgentName(env)
	if _, err := fmt.Fprintf(env.IO.Out, "Agent: %s\n", useAgentName); err != nil {
		return err
	}
	switcher := agentSwitcher(env)
	if switcher == nil {
		return nil
	}
	if _, err := fmt.Fprintln(env.IO.Out, "Available agents:"); err != nil {
		return err
	}

	for _, name := range switcher.ListAgentNames() {
		if _, err := fmt.Fprintf(env.IO.Out, "  %s\n", name); err != nil {
			return err
		}
	}
	return nil
}

type SetAgent struct{}

func (SetAgent) Name() string {
	return "set-agent"
}

func (SetAgent) Description() string {
	return "switch the active agent"
}

func (SetAgent) Execute(ctx context.Context, env content.Env, args []string) error {
	name := strings.TrimSpace(strings.Join(args, " "))
	if name == "" {
		return fmt.Errorf("set-agent requires an agent name")
	}
	if err := switchAgent(ctx, env, name); err != nil {
		return err
	}
	_, err := fmt.Fprintf(env.IO.Out, "Agent switched to: %s\n", activeAgentName(env))
	return err
}

func switchAgent(_ context.Context, env content.Env, agentName string) error {
	switcher := agentSwitcher(env)
	if switcher == nil {
		return fmt.Errorf("set-agent command: agent switcher is not configured")
	}
	return switcher.SelectAgent(agentName)
}

func activeAgentName(env content.Env) string {
	if switcher := agentSwitcher(env); switcher != nil {
		if name := strings.TrimSpace(switcher.ActiveAgentName()); name != "" {
			return name
		}
	}
	return env.Config.AgentName
}

func agentSwitcher(env content.Env) content.AgentSwitcher {
	switcher, ok := env.Agent.(content.AgentSwitcher)
	if !ok {
		return nil
	}
	return switcher
}

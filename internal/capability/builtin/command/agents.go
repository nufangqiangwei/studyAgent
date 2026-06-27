package command

import (
	"agent/internal/content"
	"context"
	"fmt"
	"strings"
)

type agentsCommand struct{}

func (agentsCommand) Name() string {
	return "agent"
}

func (agentsCommand) Description() string {
	return "show active and available agents"
}

func (agentsCommand) Execute(_ context.Context, env content.Env, _ []string) error {
	useAgentName := activeAgentName(env)
	if _, err := fmt.Fprintf(env.IO.Out, "Agent: %s\n", useAgentName); err != nil {
		return err
	}
	selector := agentSelector(env)
	if selector == nil {
		return nil
	}
	if _, err := fmt.Fprintln(env.IO.Out, "Available agents:"); err != nil {
		return err
	}

	for _, name := range selector.ListAgentNames() {
		if _, err := fmt.Fprintf(env.IO.Out, "  %s\n", name); err != nil {
			return err
		}
	}
	return nil
}

type setAgentCommand struct{}

func (setAgentCommand) Name() string {
	return "set-agent"
}

func (setAgentCommand) Description() string {
	return "switch the active agent"
}

func (setAgentCommand) Execute(ctx context.Context, env content.Env, args []string) error {
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
	selector := agentSelector(env)
	if selector == nil {
		return fmt.Errorf("set-agent command: agent selector is not configured")
	}
	return selector.SelectAgent(agentName)
}

func activeAgentName(env content.Env) string {
	if selector := agentSelector(env); selector != nil {
		if name := strings.TrimSpace(selector.ActiveAgentName()); name != "" {
			return name
		}
	}
	return env.Config.AgentName
}

func agentSelector(env content.Env) content.AgentSelector {
	selector, ok := env.Agent.(content.AgentSelector)
	if !ok {
		return nil
	}
	return selector
}

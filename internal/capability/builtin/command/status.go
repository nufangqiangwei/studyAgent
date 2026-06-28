package command

import (
	"agent/internal/content"
	"context"
	"fmt"
)

type Status struct{}

func (Status) Name() string {
	return "status"
}

func (Status) Description() string {
	return "show provider, model, and workspace"
}

func (Status) Execute(_ context.Context, env content.Env, _ []string) error {
	if env.Config.ConfigPath != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Config: %s\n", env.Config.ConfigPath); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Agent: %s\n", activeAgentName(env)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Provider: %s\n", env.Config.Provider); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Model: %s\n", env.Config.Model); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Model URL: %s\n", env.Config.ModelURL); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(env.IO.Out, "API key configured: %t\n", env.Config.APIKeyConfigured); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Debug: %t\n", env.Config.Debug); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Policy mode: %s\n", env.Config.PolicyMode); err != nil {
		return err
	}
	_, err := fmt.Fprintf(env.IO.Out, "Workspace: %s\n", env.Config.WorkDir)
	return err
}

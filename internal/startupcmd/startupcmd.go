package startupcmd

import (
	"agent/internal/command"
	"agent/internal/content"
	"agent/internal/startup"
	"context"
)

func Run(ctx context.Context, cfg startup.Config, registry *command.Registry, env content.Env) error {
	if registry == nil {
		registry = command.Manage
	}
	return registry.Execute(ctx, cfg.Command, env, cfg.CommandArgs)
}

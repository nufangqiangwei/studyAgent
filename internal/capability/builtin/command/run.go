package command

import (
	"agent/internal/content"
	"context"
	"fmt"
	"strings"
)

type Run struct{}

func (Run) Name() string {
	return "run"
}

func (Run) Description() string {
	return "run one agent task"
}

func (Run) Execute(ctx context.Context, env content.Env, args []string) error {
	task := strings.TrimSpace(strings.Join(args, " "))
	if task == "" {
		return fmt.Errorf("run command requires a task, for example: agent run \"summarize this project\"")
	}
	if env.Agent == nil {
		return fmt.Errorf("run command: agent runner is not configured")
	}

	if env.Logger != nil {
		env.Logger.Infof("running task with provider=%s model=%s workdir=%s", env.Config.Provider, env.Config.Model, env.Config.WorkDir)
	}
	return env.Agent.Run(ctx, task)
}

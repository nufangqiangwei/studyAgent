package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"agent/internal/command"
	"agent/internal/content"
)

func Run(ctx context.Context, env content.Env, registry *command.Registry) error {
	if registry == nil {
		registry = command.Manage
	}
	if err := printBanner(env); err != nil {
		return err
	}

	scanner := bufio.NewScanner(env.IO.In)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := fmt.Fprint(env.IO.Out, "agent> "); err != nil {
			return err
		}
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var err error
		if strings.HasPrefix(line, "/") {
			err = executeUserCommand(ctx, env, registry, line)
		} else {
			err = executeDefault(ctx, env, registry, line)
		}
		if err != nil {
			if errors.Is(err, errExit) {
				fmt.Fprintln(env.IO.Out)
				return nil
			}
			fmt.Fprintf(errorWriter(env), "error: %v\n", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read cli input: %w", err)
	}
	return nil
}

var errExit = errors.New("cli exit")

func executeUserCommand(ctx context.Context, env content.Env, registry *command.Registry, line string) error {
	name, arg, _ := strings.Cut(line[1:], " ")
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return executeDefault(ctx, env, registry, line)
	}

	switch name {
	case "exit", "quit":
		return errExit
	case "run":
		task := strings.TrimSpace(arg)
		if task == "" {
			return fmt.Errorf("run requires a task")
		}
		return executeAgentTask(ctx, env, task)
	}

	if _, ok := registry.Lookup(name); !ok {
		if isAgentName(env.Agent, name) {
			return registry.Execute(ctx, "set-agent", env, []string{name})
		}
		return executeDefault(ctx, env, registry, line)
	}

	args := commandArgs(arg)
	return registry.Execute(ctx, name, env, args)
}

func executeDefault(ctx context.Context, env content.Env, registry *command.Registry, line string) error {
	name := strings.ToLower(strings.TrimSpace(line))
	if isAgentName(env.Agent, name) {
		return registry.Execute(ctx, "set-agent", env, []string{name})
	}
	return executeAgentTask(ctx, env, line)
}

func executeAgentTask(ctx context.Context, env content.Env, line string) error {
	if env.Agent == nil {
		return fmt.Errorf("agent runner is not configured")
	}
	if env.Logger != nil {
		env.Logger.Infof("running task with provider=%s model=%s workdir=%s", env.Config.Provider, env.Config.Model, env.Config.WorkDir)
	}
	return env.Agent.Run(ctx, line)
}

func commandArgs(arg string) []string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil
	}
	return []string{arg}
}

func printBanner(env content.Env) error {
	if _, err := fmt.Fprintln(env.IO.Out, "Agent CLI"); err != nil {
		return err
	}
	if env.Config.AgentName != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Agent: %s\n", env.Config.AgentName); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Provider: %s  Model: %s\n", env.Config.Provider, env.Config.Model); err != nil {
		return err
	}
	if env.Config.ConfigPath != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Config: %s\n", env.Config.ConfigPath); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Workspace: %s\n", env.Config.WorkDir); err != nil {
		return err
	}
	_, err := fmt.Fprintln(env.IO.Out, "Type /help for commands, /exit to quit. Other input is sent to the model.")
	return err
}

func errorWriter(env content.Env) io.Writer {
	if env.IO.Err != nil {
		return env.IO.Err
	}
	return env.IO.Out
}

func isAgentName(runner content.AgentRunner, name string) bool {
	selector, ok := runner.(content.AgentSelector)
	if !ok || selector == nil || name == "" {
		return false
	}
	for _, agentName := range selector.ListAgentNames() {
		if strings.EqualFold(agentName, name) {
			return true
		}
	}
	return false
}

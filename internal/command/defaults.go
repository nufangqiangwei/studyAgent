package command

import (
	"agent/internal/content"
	"context"
	"fmt"
	"strings"
)

const Version = "dev"

func RegisterDefaults(registry *Registry) {
	mustRegister(registry, runCommand{})
	mustRegister(registry, agentsCommand{})
	mustRegister(registry, setAgentCommand{})
	mustRegister(registry, helpCommand{})
	mustRegister(registry, statusCommand{})
	mustRegister(registry, versionCommand{})
	mustRegister(registry, modelCommand{})
}

func mustRegister(registry *Registry, cmd Command) {
	if err := registry.Register(cmd); err != nil {
		panic(err)
	}
}

type runCommand struct{}

func (runCommand) Name() string {
	return "run"
}

func (runCommand) Description() string {
	return "run one agent task"
}

func (runCommand) Execute(ctx context.Context, env content.Env, args []string) error {
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

type helpCommand struct{}

func (helpCommand) Name() string {
	return "help"
}

func (helpCommand) Description() string {
	return "show commands and global flags"
}

func (helpCommand) Execute(_ context.Context, env content.Env, _ []string) error {
	fmt.Fprintln(env.IO.Out, "Usage:")
	fmt.Fprintln(env.IO.Out, "  agent [flags] [command] [args]")
	fmt.Fprintln(env.IO.Out, "  agent")
	fmt.Fprintln(env.IO.Out)
	fmt.Fprintln(env.IO.Out, "Commands:")
	registry := env.Registry
	if registry == nil {
		registry = Manage
	}
	for _, cmd := range registry.List() {
		fmt.Fprintf(env.IO.Out, "  %-10s %s\n", cmd.Name(), cmd.Description())
	}
	fmt.Fprintln(env.IO.Out)
	fmt.Fprintln(env.IO.Out, "Global flags:")
	fmt.Fprintln(env.IO.Out, "  --config string    config file path, default: ~/.testAgent/config.json if it exists")
	fmt.Fprintln(env.IO.Out, "  --provider string   deprecated; provider is inferred from --model")
	fmt.Fprintln(env.IO.Out, "  --model string      llm model name, default: mock-native")
	fmt.Fprintln(env.IO.Out, "  --workdir string    workspace directory, default: current directory")
	fmt.Fprintln(env.IO.Out, "  --log-level string  debug, info, warn, error, silent")
	fmt.Fprintln(env.IO.Out, "  --debug             write llm request and response bodies to session llm.jsonl")
	fmt.Fprintln(env.IO.Out, "  --help, -h          show help")
	fmt.Fprintln(env.IO.Out, "  --version, -v       show version")
	fmt.Fprintln(env.IO.Out)
	fmt.Fprintln(env.IO.Out, "Interactive mode:")
	fmt.Fprintln(env.IO.Out, "  Run agent with no command to start interactive CLI mode.")
	fmt.Fprintln(env.IO.Out, "  In CLI mode, prefix registered command names with /, for example /status.")
	fmt.Fprintln(env.IO.Out, "  Plain input is sent directly to the active agent.")
	fmt.Fprintln(env.IO.Out, "  Mistyped slash commands may prompt to run the closest command.")
	fmt.Fprintln(env.IO.Out, "  Unknown slash input without a close command match reports an error.")
	fmt.Fprintln(env.IO.Out, "  Use /exit or /quit to leave CLI mode.")
	fmt.Fprintln(env.IO.Out)
	fmt.Fprintln(env.IO.Out, "Examples:")
	fmt.Fprintln(env.IO.Out, "  agent")
	fmt.Fprintln(env.IO.Out, "  agent run \"summarize this project\"")
	fmt.Fprintln(env.IO.Out, "  agent --model=gpt-4.1 run \"plan the next module\"")
	return nil
}

type statusCommand struct{}

func (statusCommand) Name() string {
	return "status"
}

func (statusCommand) Description() string {
	return "show provider, model, and workspace"
}

func (statusCommand) Execute(_ context.Context, env content.Env, _ []string) error {
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
	_, err := fmt.Fprintf(env.IO.Out, "Workspace: %s\n", env.Config.WorkDir)
	return err
}

type versionCommand struct{}

func (versionCommand) Name() string {
	return "version"
}

func (versionCommand) Description() string {
	return "show version"
}

func (versionCommand) Execute(_ context.Context, env content.Env, _ []string) error {
	_, err := fmt.Fprintf(env.IO.Out, "agent %s\n", Version)
	return err
}

type modelCommand struct{}

func (modelCommand) Name() string {
	return "model"
}
func (modelCommand) Description() string {
	return "show current model"
}
func (modelCommand) Execute(_ context.Context, env content.Env, _ []string) error {
	_, err := fmt.Fprintf(env.IO.Out, " %s\n", env.Config.Model)
	return err
}

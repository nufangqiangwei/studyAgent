package command

import (
	"agent/internal/content"
	"context"
	"fmt"
	"sort"
)

type Command interface {
	content.CommandInfo
	Execute(ctx context.Context, env content.Env, args []string) error
}

type Registry struct {
	commands map[string]Command
}

func NewRegistry() *Registry {
	return &Registry{commands: make(map[string]Command)}
}

func (r *Registry) Register(cmd Command) error {
	if cmd == nil {
		return fmt.Errorf("register command: nil command")
	}
	name := cmd.Name()
	if name == "" {
		return fmt.Errorf("register command: empty name")
	}
	if _, exists := r.commands[name]; exists {
		return fmt.Errorf("register command %q: already exists", name)
	}
	r.commands[name] = cmd
	return nil
}

func (r *Registry) Execute(ctx context.Context, name string, env content.Env, args []string) error {
	if r == nil {
		return fmt.Errorf("command registry is nil")
	}
	cmd, ok := r.commands[name]
	if !ok {
		return fmt.Errorf("unknown command %q", name)
	}
	env = env.WithRegistry(r)
	ctx = content.WithEnv(ctx, &env)
	return cmd.Execute(ctx, env, args)
}

func (r *Registry) Lookup(name string) (Command, bool) {
	cmd, ok := r.commands[name]
	return cmd, ok
}

func (r *Registry) List() []content.CommandInfo {
	names := make([]string, 0, len(r.commands))
	for name := range r.commands {
		names = append(names, name)
	}
	sort.Strings(names)

	commands := make([]content.CommandInfo, 0, len(names))
	for _, name := range names {
		commands = append(commands, r.commands[name])
	}
	return commands
}

var Manage *Registry

func init() {
	Manage = NewRegistry()
	RegisterDefaults(Manage)
}

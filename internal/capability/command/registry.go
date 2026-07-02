package command

import (
	"agent/internal/capability/builtin/command"
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

func (r *Registry) register(cmd Command) {
	if cmd == nil {
		panic("builtin.command 注册命令时报错：传入的Command对象为空")
	}
	name := cmd.Name()
	if name == "" {
		panic("builtin.command 注册命令时报错：传入的Command对象名称为空")
	}
	if _, exists := r.commands[name]; exists {
		panic(fmt.Sprintf("builtin.command 注册命令时报错：传入的Command对象名称：%q已存在", name))
	}
	r.commands[name] = cmd
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
	Manage = &Registry{commands: make(map[string]Command)}
	Manage.register(command.Model{})
	Manage.register(command.Status{})
	Manage.register(command.Run{})
	Manage.register(command.Recover{})
	Manage.register(command.Work{})
	Manage.register(command.Advance{})
	Manage.register(command.Effect{})
	Manage.register(command.Step{})
	Manage.register(command.Result{})
	Manage.register(command.Input{})
	Manage.register(command.Approve{})
	Manage.register(command.Help{})
	Manage.register(command.Agent{})
	Manage.register(command.SetAgent{})
}

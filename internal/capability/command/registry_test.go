package command

import (
	"agent/internal/content"
	"context"
	"testing"
)

func TestRegistryRejectsDuplicateCommand(t *testing.T) {
	registry := NewRegistry()
	cmd := fakeCommand{name: "test"}
	if err := registry.Register(cmd); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := registry.Register(cmd); err == nil {
		t.Fatal("Register duplicate returned nil error")
	}
}

func TestRegistryExecuteBindsEnvToContext(t *testing.T) {
	registry := NewRegistry()
	cmd := &contextEnvCommand{name: "test"}
	if err := registry.Register(cmd); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	err := registry.Execute(context.Background(), "test", content.Env{
		Config: content.Config{
			WorkDir: "workspace",
		},
	}, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if cmd.env.Registry == nil {
		t.Fatal("command env Registry was not set")
	}
	if cmd.contextEnv == nil {
		t.Fatal("context env was not set")
	}
	if cmd.contextEnv.Registry == nil {
		t.Fatal("context env Registry was not set")
	}
	if cmd.contextEnv.Config.WorkDir != "workspace" {
		t.Fatalf("context WorkDir = %q, want workspace", cmd.contextEnv.Config.WorkDir)
	}
}

type fakeCommand struct {
	name string
}

func (c fakeCommand) Name() string {
	return c.name
}

func (fakeCommand) Description() string {
	return "fake"
}

func (fakeCommand) Execute(context.Context, content.Env, []string) error {
	return nil
}

type contextEnvCommand struct {
	name       string
	env        content.Env
	contextEnv *content.Env
}

func (c *contextEnvCommand) Name() string {
	return c.name
}

func (*contextEnvCommand) Description() string {
	return "context env"
}

func (c *contextEnvCommand) Execute(ctx context.Context, env content.Env, _ []string) error {
	c.env = env
	c.contextEnv, _ = content.EnvFromContext(ctx)
	return nil
}

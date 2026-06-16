package startupcmd

import (
	"agent/internal/content"
	"context"
	"strings"
	"testing"

	"agent/internal/command"
	"agent/internal/startup"
)

func TestRunExecutesStartupCommand(t *testing.T) {
	registry := command.NewRegistry()
	cmd := &recordingCommand{name: "test"}
	if err := registry.Register(cmd); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	cfg := startup.Config{
		Command:     "test",
		CommandArgs: []string{"one", "two"},
	}
	err := Run(context.Background(), cfg, registry, content.Env{})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if strings.Join(cmd.args, ",") != "one,two" {
		t.Fatalf("args = %#v, want [one two]", cmd.args)
	}
}

func TestRunReturnsUnknownCommandError(t *testing.T) {
	registry := command.NewRegistry()
	cfg := startup.Config{Command: "missing"}

	err := Run(context.Background(), cfg, registry, content.Env{})
	if err == nil {
		t.Fatal("Run returned nil error")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("error = %q, want unknown command", err.Error())
	}
}

type recordingCommand struct {
	name string
	args []string
}

func (c *recordingCommand) Name() string {
	return c.name
}

func (c *recordingCommand) Description() string {
	return "recording command"
}

func (c *recordingCommand) Execute(_ context.Context, _ content.Env, args []string) error {
	c.args = append([]string(nil), args...)
	return nil
}

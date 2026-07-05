package startupcmd

import (
	"agent/internal/capability/command"
	"agent/internal/content"
	"agent/internal/foundation/startup"
	"context"
	"strings"
	"testing"
)

func TestRunReturnsUnknownCommandError(t *testing.T) {
	registry := command.Manage
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

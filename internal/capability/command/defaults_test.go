package command

import (
	"agent/internal/content"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestRunCommandRunsTask(t *testing.T) {
	var out strings.Builder
	runner := &recordingRunner{output: "done", out: &out}
	env := content.Env{
		IO: content.IO{
			Out: &out,
		},
		Agent: runner,
	}

	err := runCommand{}.Execute(context.Background(), env, []string{"summarize", "this", "project"})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if runner.task != "summarize this project" {
		t.Fatalf("task = %q, want summarize this project", runner.task)
	}
	if out.String() != "done\n" {
		t.Fatalf("output = %q, want done newline", out.String())
	}
}

func TestRunCommandRequiresTask(t *testing.T) {
	err := runCommand{}.Execute(context.Background(), content.Env{}, nil)
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "requires a task") {
		t.Fatalf("error = %q, want requires a task", err.Error())
	}
}

func TestStatusCommandPrintsConfig(t *testing.T) {
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			Out: &out,
		},
		Config: content.Config{
			ConfigPath:       "~/.testAgent/config.json",
			AgentName:        "analyze",
			Provider:         "openai",
			Model:            "gpt-test",
			ModelURL:         "https://example.test/v1",
			APIKeyConfigured: true,
			WorkDir:          "C:\\Code\\GO\\agent",
			PolicyMode:       "validate",
		},
	}

	err := statusCommand{}.Execute(context.Background(), env, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Config: ~/.testAgent/config.json",
		"Agent: analyze",
		"Provider: openai",
		"Model: gpt-test",
		"Model URL: https://example.test/v1",
		"API key configured: true",
		"Policy mode: validate",
		"Workspace: C:\\Code\\GO\\agent",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestAgentsCommandPrintsActiveAgent(t *testing.T) {
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			Out: &out,
		},
		Config: content.Config{
			AgentName: "analyze",
		},
	}

	err := agentsCommand{}.Execute(context.Background(), env, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if out.String() != "Agent: analyze\n" {
		t.Fatalf("output = %q, want active agent", out.String())
	}
}

func TestAgentsCommandPrintsAvailableAgents(t *testing.T) {
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			Out: &out,
		},
		Agent: &recordingAgentSelector{
			active: "analyze",
			names:  []string{"analyze", "review"},
		},
	}

	err := agentsCommand{}.Execute(context.Background(), env, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Agent: analyze",
		"Available agents:",
		"  analyze",
		"  review",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestSetAgentCommandSwitchesByName(t *testing.T) {
	var out strings.Builder
	selector := &recordingAgentSelector{
		active: "analyze",
		names:  []string{"analyze", "review"},
	}
	env := content.Env{
		IO: content.IO{
			Out: &out,
		},
		Agent: selector,
	}

	err := setAgentCommand{}.Execute(context.Background(), env, []string{"review"})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if selector.active != "review" {
		t.Fatalf("active = %q, want review", selector.active)
	}
	if out.String() != "Agent switched to: review\n" {
		t.Fatalf("output = %q, want switched message", out.String())
	}
}

func TestSetAgentCommandRequiresName(t *testing.T) {
	err := setAgentCommand{}.Execute(context.Background(), content.Env{}, nil)
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "requires an agent name") {
		t.Fatalf("error = %q, want requires an agent name", err.Error())
	}
}

func TestRegisterDefaultsExcludesCLI(t *testing.T) {
	registry := NewRegistry()
	RegisterDefaults(registry)

	for _, name := range []string{"run", "help", "status", "version"} {
		if _, ok := registry.Lookup(name); !ok {
			t.Fatalf("default command %q was not registered", name)
		}
	}
	if _, ok := registry.Lookup("cli"); ok {
		t.Fatal("default commands should not register cli")
	}
}

type recordingRunner struct {
	task   string
	output string
	out    io.Writer
}

func (r *recordingRunner) Run(_ context.Context, task string) error {
	r.task = task
	if r.out == nil || r.output == "" {
		return nil
	}
	_, err := fmt.Fprintln(r.out, r.output)
	return err
}

type recordingAgentSelector struct {
	active string
	names  []string
}

func (s *recordingAgentSelector) Run(_ context.Context, _ string) error {
	return nil
}

func (s *recordingAgentSelector) ActiveAgentName() string {
	return s.active
}

func (s *recordingAgentSelector) ListAgentNames() []string {
	return append([]string(nil), s.names...)
}

func (s *recordingAgentSelector) SelectAgent(name string) error {
	for _, agentName := range s.names {
		if agentName == name {
			s.active = name
			return nil
		}
	}
	return nil
}

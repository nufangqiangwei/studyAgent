package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"agent/internal/command"
	"agent/internal/content"
)

func TestRunExecutesPlainTextDirectlyWithAgent(t *testing.T) {
	var out strings.Builder
	runner := &recordingRunner{output: "done", out: &out}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("summarize this project\n/exit\n"),
			Out: &out,
		},
		Agent: runner,
		Config: content.Config{
			Provider: "mock",
			Model:    "mock-native",
			WorkDir:  "C:\\Code\\GO\\agent",
		},
	}

	err := Run(context.Background(), env, defaultRegistry())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if runner.task != "summarize this project" {
		t.Fatalf("task = %q, want summarize this project", runner.task)
	}
	got := out.String()
	for _, want := range []string{"Agent CLI", "agent> ", "done", "Type /help"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunExecutesSlashRunThroughCommand(t *testing.T) {
	var out strings.Builder
	runner := &recordingRunner{output: "ran", out: &out}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/run fix tests\n/quit\n"),
			Out: &out,
		},
		Agent: runner,
	}

	err := Run(context.Background(), env, defaultRegistry())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if runner.task != "fix tests" {
		t.Fatalf("task = %q, want fix tests", runner.task)
	}
	if !strings.Contains(out.String(), "ran") {
		t.Fatalf("output missing agent response:\n%s", out.String())
	}
}

func TestRunExecutesSlashStatusThroughCommand(t *testing.T) {
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/status\n/exit\n"),
			Out: &out,
		},
		Config: content.Config{
			Provider:         "openai",
			Model:            "gpt-test",
			ModelURL:         "https://example.test/v1",
			APIKeyConfigured: true,
			WorkDir:          "C:\\Code\\GO\\agent",
		},
	}

	err := Run(context.Background(), env, defaultRegistry())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Provider: openai",
		"Model: gpt-test",
		"Model URL: https://example.test/v1",
		"API key configured: true",
		"Workspace: C:\\Code\\GO\\agent",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunUnknownSlashInputGoesToModel(t *testing.T) {
	var out strings.Builder
	runner := &recordingRunner{output: "sent", out: &out}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/missing\n/exit\n"),
			Out: &out,
		},
		Agent: runner,
	}

	err := Run(context.Background(), env, defaultRegistry())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if runner.task != "/missing" {
		t.Fatalf("task = %q, want /missing", runner.task)
	}
	if !strings.Contains(out.String(), "sent") {
		t.Fatalf("output missing model response:\n%s", out.String())
	}
}

func TestRunSwitchesAgentWithBareAgentName(t *testing.T) {
	var out strings.Builder
	selector := &recordingAgentSelector{
		active: "analyze",
		names:  []string{"analyze", "review"},
	}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("review\n/exit\n"),
			Out: &out,
		},
		Agent: selector,
	}

	err := Run(context.Background(), env, defaultRegistry())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if selector.active != "review" {
		t.Fatalf("active = %q, want review", selector.active)
	}
	if !strings.Contains(out.String(), "Agent switched to: review") {
		t.Fatalf("output missing switch message:\n%s", out.String())
	}
}

func TestRunSwitchesAgentWithSlashAgentName(t *testing.T) {
	var out strings.Builder
	selector := &recordingAgentSelector{
		active: "analyze",
		names:  []string{"analyze", "review"},
	}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/review\n/exit\n"),
			Out: &out,
		},
		Agent: selector,
	}

	err := Run(context.Background(), env, defaultRegistry())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if selector.active != "review" {
		t.Fatalf("active = %q, want review", selector.active)
	}
	if !strings.Contains(out.String(), "Agent switched to: review") {
		t.Fatalf("output missing switch message:\n%s", out.String())
	}
}

func TestRunSlashOnlyGoesToModel(t *testing.T) {
	var out strings.Builder
	runner := &recordingRunner{output: "sent", out: &out}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/\n/exit\n"),
			Out: &out,
		},
		Agent: runner,
	}

	err := Run(context.Background(), env, defaultRegistry())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if runner.task != "/" {
		t.Fatalf("task = %q, want /", runner.task)
	}
	if !strings.Contains(out.String(), "sent") {
		t.Fatalf("output missing model response:\n%s", out.String())
	}
}

func TestRunCommandErrorContinues(t *testing.T) {
	var out strings.Builder
	var errOut strings.Builder
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/run\n/status\n/exit\n"),
			Out: &out,
			Err: &errOut,
		},
		Config: content.Config{
			Provider: "mock",
			Model:    "mock-native",
			WorkDir:  "C:\\Code\\GO\\agent",
		},
	}

	err := Run(context.Background(), env, defaultRegistry())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(errOut.String(), "requires a task") {
		t.Fatalf("stderr missing command error:\n%s", errOut.String())
	}
	if !strings.Contains(out.String(), "Workspace: C:\\Code\\GO\\agent") {
		t.Fatalf("output missing later status command:\n%s", out.String())
	}
}

func defaultRegistry() *command.Registry {
	registry := command.NewRegistry()
	command.RegisterDefaults(registry)
	return registry
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
		if strings.EqualFold(agentName, name) {
			s.active = agentName
			return nil
		}
	}
	return nil
}

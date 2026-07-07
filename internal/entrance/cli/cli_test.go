package cli

import (
	command2 "agent/internal/capability/command"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"agent/internal/content"
)

func TestRunDelegatesPlainTextToHandler(t *testing.T) {
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

	err := Run(context.Background(), env, defaultRegistry(), runWithAgent(runner))
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

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
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

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
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

func TestRunUnknownSlashInputReportsError(t *testing.T) {
	var out strings.Builder
	var errOut strings.Builder
	runner := &recordingRunner{output: "sent", out: &out}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/missing\n/exit\n"),
			Out: &out,
			Err: &errOut,
		},
		Agent: runner,
	}

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if runner.task != "" {
		t.Fatalf("task = %q, want no agent task", runner.task)
	}
	if !strings.Contains(errOut.String(), "unknown command \"/missing\"") {
		t.Fatalf("stderr missing unknown command error:\n%s", errOut.String())
	}
}

func TestRunSuggestsMistypedSlashCommandAndExecutesWhenConfirmed(t *testing.T) {
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/statu\ny\n/exit\n"),
			Out: &out,
		},
		Config: content.Config{
			Provider: "openai",
			Model:    "gpt-test",
			WorkDir:  "C:\\Code\\GO\\agent",
		},
	}

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Unknown command \"/statu\"",
		"Did you mean \"/status\"?",
		"Provider: openai",
		"Model: gpt-test",
		"Workspace: C:\\Code\\GO\\agent",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunDoesNotExecuteSuggestedCommandWhenDeclined(t *testing.T) {
	var out strings.Builder
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/stats\nn\n/exit\n"),
			Out: &out,
		},
	}

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Unknown command \"/stats\"",
		"Did you mean \"/status\"?",
		"Command not executed.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "agent dev") {
		t.Fatalf("declined suggestion executed status command:\n%s", got)
	}
}

func TestRunSuggestedRunCommandKeepsArgument(t *testing.T) {
	var out strings.Builder
	runner := &recordingRunner{output: "ran", out: &out}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/rn fix tests\ny\n/exit\n"),
			Out: &out,
		},
		Agent: runner,
	}

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
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

func TestRunDelegatesBareAgentNameToPlainInputHandler(t *testing.T) {
	var out strings.Builder
	switcher := &recordingAgentSwitcher{
		active: "analyze",
		names:  []string{"analyze", "review"},
	}
	var handled string
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("review\n/exit\n"),
			Out: &out,
		},
		Agent: switcher,
	}

	err := Run(context.Background(), env, defaultRegistry(), func(_ context.Context, _ content.Env, line string) error {
		handled = line
		return nil
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if handled != "review" {
		t.Fatalf("handled = %q, want review", handled)
	}
	if switcher.active != "analyze" {
		t.Fatalf("active = %q, want analyze", switcher.active)
	}
	if strings.Contains(out.String(), "Agent switched to: review") {
		t.Fatalf("bare plain input switched agent unexpectedly:\n%s", out.String())
	}
}

func TestRunSwitchesAgentWithSlashAgentName(t *testing.T) {
	var out strings.Builder
	switcher := &recordingAgentSwitcher{
		active: "analyze",
		names:  []string{"analyze", "review"},
	}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/review\n/exit\n"),
			Out: &out,
		},
		Agent: switcher,
	}

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if switcher.active != "review" {
		t.Fatalf("active = %q, want review", switcher.active)
	}
	if !strings.Contains(out.String(), "Agent switched to: review") {
		t.Fatalf("output missing switch message:\n%s", out.String())
	}
}

func TestRunSlashOnlyReportsError(t *testing.T) {
	var out strings.Builder
	var errOut strings.Builder
	runner := &recordingRunner{output: "sent", out: &out}
	env := content.Env{
		IO: content.IO{
			In:  strings.NewReader("/\n/exit\n"),
			Out: &out,
			Err: &errOut,
		},
		Agent: runner,
	}

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if runner.task != "" {
		t.Fatalf("task = %q, want no agent task", runner.task)
	}
	if !strings.Contains(errOut.String(), "unknown command \"/\"") {
		t.Fatalf("stderr missing unknown command error:\n%s", errOut.String())
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

	err := Run(context.Background(), env, defaultRegistry(), noopPlainInput)
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

func defaultRegistry() *command2.Registry {
	return command2.Manage
}

func noopPlainInput(context.Context, content.Env, string) error {
	return nil
}

func runWithAgent(runner content.AgentRunner) PlainInputHandler {
	return func(ctx context.Context, _ content.Env, line string) error {
		return runner.Run(ctx, line)
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

type recordingAgentSwitcher struct {
	active string
	names  []string
}

func (s *recordingAgentSwitcher) Run(_ context.Context, _ string) error {
	return nil
}

func (s *recordingAgentSwitcher) ActiveAgentName() string {
	return s.active
}

func (s *recordingAgentSwitcher) ListAgentNames() []string {
	return append([]string(nil), s.names...)
}

func (s *recordingAgentSwitcher) SelectAgent(name string) error {
	for _, agentName := range s.names {
		if strings.EqualFold(agentName, name) {
			s.active = agentName
			return nil
		}
	}
	return nil
}

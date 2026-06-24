package startup

import "testing"

func TestParseDefaultsToCLI(t *testing.T) {
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if cfg.Command != "cli" {
		t.Fatalf("Command = %q, want cli", cfg.Command)
	}
	if cfg.Provider != "mock" {
		t.Fatalf("Provider = %q, want mock", cfg.Provider)
	}
	if cfg.PolicyMode != "read" {
		t.Fatalf("PolicyMode = %q, want read", cfg.PolicyMode)
	}
}

func TestParseRunCommand(t *testing.T) {
	cfg, err := Parse([]string{"--config=custom.json", "--provider=openai", "--model=gpt-test", "--policy-mode=modify", "--debug", "run", "hello"})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if cfg.ConfigPath != "custom.json" {
		t.Fatalf("ConfigPath = %q, want custom.json", cfg.ConfigPath)
	}
	if !cfg.IsFlagSet("model") {
		t.Fatal("model flag should be marked as set")
	}
	if cfg.Command != "run" {
		t.Fatalf("Command = %q, want run", cfg.Command)
	}
	if cfg.Provider != "openai" {
		t.Fatalf("Provider = %q, want openai", cfg.Provider)
	}
	if cfg.Model != "gpt-test" {
		t.Fatalf("Model = %q, want gpt-test", cfg.Model)
	}
	if cfg.PolicyMode != "modify" {
		t.Fatalf("PolicyMode = %q, want modify", cfg.PolicyMode)
	}
	if !cfg.IsFlagSet("policy-mode") {
		t.Fatal("policy-mode flag should be marked as set")
	}
	if !cfg.Debug {
		t.Fatal("Debug = false, want true")
	}
	if !cfg.IsFlagSet("debug") {
		t.Fatal("debug flag should be marked as set")
	}
	if len(cfg.CommandArgs) != 1 || cfg.CommandArgs[0] != "hello" {
		t.Fatalf("CommandArgs = %#v, want [hello]", cfg.CommandArgs)
	}
}

func TestParseResumeRunCommand(t *testing.T) {
	cfg, err := Parse([]string{"--resume", "session-1", "--resume-agent", "agent-1", "run"})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if cfg.Command != "run" || cfg.ResumeSessionID != "session-1" || cfg.ResumeAgentID != "agent-1" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestParseResumeRejectsUnsupportedCommand(t *testing.T) {
	_, err := Parse([]string{"--resume", "session-1", "status"})
	if err == nil {
		t.Fatal("Parse returned nil error")
	}
}

func TestParseResumeRunRejectsNewTask(t *testing.T) {
	_, err := Parse([]string{"--resume", "session-1", "run", "new task"})
	if err == nil {
		t.Fatal("Parse returned nil error")
	}
}

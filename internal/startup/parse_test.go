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
}

func TestParseRunCommand(t *testing.T) {
	cfg, err := Parse([]string{"--config=custom.json", "--provider=openai", "--model=gpt-test", "--debug", "run", "hello"})
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

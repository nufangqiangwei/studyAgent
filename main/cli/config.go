package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const defaultSystemPrompt = "Answer the user's request accurately and concisely. This CLI configuration has no external capabilities, so do not claim to read files, run commands, or change the workspace."

type cliConfig struct {
	Provider     string
	Model        string
	BaseURL      string
	APIKey       string
	DataDir      string
	UserID       string
	RuntimeID    string
	SystemPrompt string
	ModelTimeout time.Duration
	MaxTurns     int
	MaxTokens    int
}

func parseConfig(args []string, stderr io.Writer, getenv func(string) string) (cliConfig, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	config := cliConfig{}
	flags := flag.NewFlagSet("agent-cli", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&config.Provider, "provider", envOr(getenv, "AGENT_PROVIDER", "deepseek"), "model provider family")
	flags.StringVar(&config.Model, "model", strings.TrimSpace(getenv("AGENT_MODEL")), "model name (or AGENT_MODEL)")
	flags.StringVar(&config.BaseURL, "base-url", envOr(getenv, "AGENT_BASE_URL", "https://api.deepseek.com"), "OpenAI-compatible model API base URL")
	flags.StringVar(&config.DataDir, "data-dir", envOr(getenv, "AGENT_DATA_DIR", ".agent/runtime"), "SQLite and Artifact data directory")
	flags.StringVar(&config.UserID, "user", envOr(getenv, "AGENT_USER_ID", "local-user"), "stable user identity")
	flags.StringVar(&config.RuntimeID, "runtime-id", envOr(getenv, "AGENT_RUNTIME_ID", "agent-cli"), "stable Runtime identity")
	flags.StringVar(&config.SystemPrompt, "system-prompt", envOr(getenv, "AGENT_SYSTEM_PROMPT", defaultSystemPrompt), "Agent system instruction")
	flags.DurationVar(&config.ModelTimeout, "model-timeout", 2*time.Minute, "timeout for one model provider call")
	flags.IntVar(&config.MaxTurns, "max-turns", 8, "maximum Agent model turns per request")
	flags.IntVar(&config.MaxTokens, "max-tokens", 0, "maximum model output tokens; zero uses provider default")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: go run ./main/cli [options]")
		fmt.Fprintln(stderr, "\nRequired configuration: -model (or AGENT_MODEL).")
		fmt.Fprintln(stderr, "DeepSeek's OpenAI-compatible endpoint is the default base URL.")
		fmt.Fprintln(stderr, "The API key is read only from AGENT_API_KEY.")
		fmt.Fprintln(stderr, "\nOptions:")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return cliConfig{}, err
	}
	if flags.NArg() != 0 {
		return cliConfig{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	config.Provider = strings.ToLower(strings.TrimSpace(config.Provider))
	config.Model = strings.TrimSpace(config.Model)
	config.BaseURL = strings.TrimSpace(config.BaseURL)
	config.DataDir = strings.TrimSpace(config.DataDir)
	config.UserID = strings.TrimSpace(config.UserID)
	config.RuntimeID = strings.TrimSpace(config.RuntimeID)
	config.SystemPrompt = strings.TrimSpace(config.SystemPrompt)
	config.APIKey = strings.TrimSpace(getenv("AGENT_API_KEY"))
	if config.Provider == "" || config.Model == "" || config.BaseURL == "" {
		return cliConfig{}, fmt.Errorf("provider, model, and base URL are required")
	}
	if config.DataDir == "" || config.UserID == "" || config.RuntimeID == "" || config.SystemPrompt == "" {
		return cliConfig{}, fmt.Errorf("data directory, user, runtime id, and system prompt are required")
	}
	if config.ModelTimeout <= 0 || config.MaxTurns <= 0 || config.MaxTokens < 0 {
		return cliConfig{}, fmt.Errorf("model timeout and max turns must be positive, and max tokens cannot be negative")
	}
	return config, nil
}

func envOr(getenv func(string) string, key, fallback string) string {
	if value := strings.TrimSpace(getenv(key)); value != "" {
		return value
	}
	return fallback
}

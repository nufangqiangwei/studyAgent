package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultServerAgentSystemPrompt = "Answer the user's request accurately and concisely. Use the available capabilities when needed, and report results truthfully."

type serverConfig struct {
	Address           string
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	DataDir           string
	RuntimeID         string
	Provider          string
	Model             string
	BaseURL           string
	APIKey            string
	ModelTimeout      time.Duration
	AgentSystemPrompt string
	AgentMaxTurns     int
	AgentMaxTokens    int
}

func parseServerConfig(args []string, stderr io.Writer, getenv func(string) string) (serverConfig, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	readHeaderTimeout, err := durationEnvOrDefault(getenv, "AGENT_SERVER_READ_HEADER_TIMEOUT", 5*time.Second)
	if err != nil {
		return serverConfig{}, err
	}
	idleTimeout, err := durationEnvOrDefault(getenv, "AGENT_SERVER_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return serverConfig{}, err
	}
	shutdownTimeout, err := durationEnvOrDefault(getenv, "AGENT_SERVER_SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return serverConfig{}, err
	}
	modelTimeout, err := durationEnvOrDefault(getenv, "AGENT_MODEL_TIMEOUT", 2*time.Minute)
	if err != nil {
		return serverConfig{}, err
	}
	agentMaxTurns, err := intEnvOrDefault(getenv, "AGENT_MAX_TURNS", 8)
	if err != nil {
		return serverConfig{}, err
	}
	agentMaxTokens, err := intEnvOrDefault(getenv, "AGENT_MAX_TOKENS", 0)
	if err != nil {
		return serverConfig{}, err
	}

	config := serverConfig{
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ShutdownTimeout:   shutdownTimeout,
		ModelTimeout:      modelTimeout,
		AgentMaxTurns:     agentMaxTurns,
		AgentMaxTokens:    agentMaxTokens,
	}
	flags := flag.NewFlagSet("agent-server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&config.Address, "address", envOrDefault(getenv, "AGENT_SERVER_ADDRESS", "127.0.0.1:8080"), "HTTP listen address")
	flags.DurationVar(&config.ReadHeaderTimeout, "read-header-timeout", config.ReadHeaderTimeout, "maximum time to read request headers")
	flags.DurationVar(&config.IdleTimeout, "idle-timeout", config.IdleTimeout, "HTTP keep-alive idle timeout")
	flags.DurationVar(&config.ShutdownTimeout, "shutdown-timeout", config.ShutdownTimeout, "graceful shutdown timeout")
	flags.StringVar(&config.DataDir, "data-dir", envOrDefault(getenv, "AGENT_DATA_DIR", ".agent/runtime"), "SQLite and Artifact data directory")
	flags.StringVar(&config.RuntimeID, "runtime-id", envOrDefault(getenv, "AGENT_RUNTIME_ID", "agent-server"), "stable Runtime identity")
	flags.StringVar(&config.Provider, "provider", envOrDefault(getenv, "AGENT_PROVIDER", "deepseek"), "model provider family")
	flags.StringVar(&config.Model, "model", strings.TrimSpace(getenv("AGENT_MODEL")), "model name (or AGENT_MODEL)")
	flags.StringVar(&config.BaseURL, "base-url", envOrDefault(getenv, "AGENT_BASE_URL", "https://api.deepseek.com"), "OpenAI-compatible model API base URL")
	flags.DurationVar(&config.ModelTimeout, "model-timeout", config.ModelTimeout, "timeout for one model provider call")
	flags.StringVar(&config.AgentSystemPrompt, "system-prompt", envOrDefault(getenv, "AGENT_SYSTEM_PROMPT", defaultServerAgentSystemPrompt), "Agent system instruction")
	flags.IntVar(&config.AgentMaxTurns, "max-turns", config.AgentMaxTurns, "maximum Agent model turns per request")
	flags.IntVar(&config.AgentMaxTokens, "max-tokens", config.AgentMaxTokens, "maximum model output tokens; zero uses provider default")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: go run ./main/server [options]")
		fmt.Fprintln(stderr, "\nThis entry starts the Web API with a local, persistent service Runtime.")
		fmt.Fprintln(stderr, "Required configuration: -model (or AGENT_MODEL).")
		fmt.Fprintln(stderr, "The API key is read only from AGENT_API_KEY.")
		fmt.Fprintln(stderr, "\nOptions:")
		flags.PrintDefaults()
	}
	if err = flags.Parse(args); err != nil {
		return serverConfig{}, err
	}
	if flags.NArg() != 0 {
		return serverConfig{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	config.Address = strings.TrimSpace(config.Address)
	config.DataDir = strings.TrimSpace(config.DataDir)
	config.RuntimeID = strings.TrimSpace(config.RuntimeID)
	config.Provider = strings.ToLower(strings.TrimSpace(config.Provider))
	config.Model = strings.TrimSpace(config.Model)
	config.BaseURL = strings.TrimSpace(config.BaseURL)
	config.AgentSystemPrompt = strings.TrimSpace(config.AgentSystemPrompt)
	config.APIKey = strings.TrimSpace(getenv("AGENT_API_KEY"))
	if config.Address == "" {
		return serverConfig{}, fmt.Errorf("HTTP listen address is required")
	}
	if config.DataDir == "" || config.RuntimeID == "" {
		return serverConfig{}, fmt.Errorf("data directory and runtime id are required")
	}
	if config.Provider == "" || config.Model == "" || config.BaseURL == "" {
		return serverConfig{}, fmt.Errorf("provider, model, and base URL are required")
	}
	if config.AgentSystemPrompt == "" {
		return serverConfig{}, fmt.Errorf("agent system prompt is required")
	}
	if config.ReadHeaderTimeout <= 0 || config.IdleTimeout <= 0 || config.ShutdownTimeout <= 0 {
		return serverConfig{}, fmt.Errorf("server timeouts must be positive")
	}
	if config.ModelTimeout <= 0 || config.AgentMaxTurns <= 0 || config.AgentMaxTokens < 0 {
		return serverConfig{}, fmt.Errorf("model timeout and agent max turns must be positive, and agent max tokens cannot be negative")
	}
	return config, nil
}

func envOrDefault(getenv func(string) string, key, fallback string) string {
	if value := strings.TrimSpace(getenv(key)); value != "" {
		return value
	}
	return fallback
}

func durationEnvOrDefault(getenv func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

func intEnvOrDefault(getenv func(string) string, key string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}

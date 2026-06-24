package startup

import (
	"flag"
	"fmt"
	"io"
)

type Config struct {
	Command         string
	CommandArgs     []string
	ConfigPath      string
	Provider        string
	ModelURL        string
	Model           string
	APIKey          string
	LogLevel        string
	WorkDir         string
	Debug           bool
	PolicyMode      string
	ResumeSessionID string
	ResumeAgentID   string

	setFlags map[string]bool
}

func (c Config) IsFlagSet(name string) bool {
	return c.setFlags[name]
}

func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	configPath := fs.String("config", "", "config file path")
	provider := fs.String("provider", "mock", "deprecated; llm provider is inferred from model name")
	model := fs.String("model", "mock-native", "llm model name")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error, silent")
	workDir := fs.String("workdir", "", "workspace directory")
	debug := fs.Bool("debug", false, "write llm request and response bodies to session debug jsonl")
	policyMode := fs.String("policy-mode", "read", "tool permission policy mode: read, validate, or modify")
	resume := fs.String("resume", "", "resume an interrupted session by session id")
	resumeAgent := fs.String("resume-agent", "", "agent id to resume within the session")
	help := fs.Bool("help", false, "show help")
	version := fs.Bool("version", false, "show version")
	fs.BoolVar(help, "h", false, "show help")
	fs.BoolVar(version, "v", false, "show version")

	if err := fs.Parse(args); err != nil {
		return Config{}, fmt.Errorf("parse startup args: %w", err)
	}

	setFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true
	})

	cfg := Config{
		ConfigPath:      *configPath,
		Provider:        *provider,
		Model:           *model,
		LogLevel:        *logLevel,
		WorkDir:         *workDir,
		Debug:           *debug,
		PolicyMode:      *policyMode,
		ResumeSessionID: *resume,
		ResumeAgentID:   *resumeAgent,
		setFlags:        setFlags,
	}

	if *version {
		cfg.Command = "version"
		return cfg, nil
	}
	if *help {
		cfg.Command = "help"
		return cfg, nil
	}

	remaining := fs.Args()
	if len(remaining) == 0 {
		cfg.Command = "cli"
		if err := validateResumeConfig(cfg); err != nil {
			return Config{}, err
		}
		return cfg, nil
	}

	cfg.Command = remaining[0]
	cfg.CommandArgs = remaining[1:]
	if err := validateResumeConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateResumeConfig(cfg Config) error {
	if cfg.ResumeAgentID != "" && cfg.ResumeSessionID == "" {
		return fmt.Errorf("--resume-agent requires --resume")
	}
	if cfg.ResumeSessionID == "" {
		return nil
	}
	if cfg.Command != "cli" && cfg.Command != "run" {
		return fmt.Errorf("--resume only supports cli mode or run command, got %q", cfg.Command)
	}
	if cfg.Command == "run" && len(cfg.CommandArgs) > 0 {
		return fmt.Errorf("--resume run resumes the saved task and does not accept a new task")
	}
	return nil
}

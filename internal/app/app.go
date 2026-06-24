package app

import (
	"agent/internal/content"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"agent/internal/agent"
	"agent/internal/cli"
	"agent/internal/command"
	appconfig "agent/internal/config"
	"agent/internal/llm/provider"
	"agent/internal/logging"
	"agent/internal/policy"
	"agent/internal/session"
	"agent/internal/startup"
	"agent/internal/startupcmd"
)

// Run wires application dependencies and executes the requested command.
func Run(ctx context.Context, args []string, in io.Reader, out io.Writer, errOut io.Writer) error {
	cfg, err := startup.Parse(args)
	if err != nil {
		return err
	}

	level, err := logging.ParseLevel(cfg.LogLevel)
	if err != nil {
		return err
	}
	logger := logging.New(errOut, level)

	workDir := cfg.WorkDir
	if workDir == "" {
		workDir, err = os.Getwd()
		if err != nil {
			return fmt.Errorf("get working directory: %w", err)
		}
	}

	cfg, err = applyFileConfig(cfg)
	if err != nil {
		return err
	}
	policyMode, err := policy.ParseMode(cfg.PolicyMode)
	if err != nil {
		return err
	}
	cfg.PolicyMode = string(policyMode)

	sessionDir, err := session.DefaultDir()
	if err != nil {
		return err
	}

	var sessionStore *session.FileStore
	var resumeCheckpoint *session.ResumeCheckpoint
	if cfg.ResumeSessionID != "" {
		resumeStore, checkpoint, err := session.OpenResumeFileStore(ctx, sessionDir, cfg.ResumeSessionID, cfg.ResumeAgentID)
		if err != nil {
			return err
		}
		if cfg.Command == "cli" {
			confirmed, err := confirmResume(in, out, checkpoint)
			if err != nil {
				return err
			}
			if confirmed {
				sessionStore = resumeStore
				resumeCheckpoint = &checkpoint
			}
		} else {
			sessionStore = resumeStore
			resumeCheckpoint = &checkpoint
		}
	}
	if sessionStore == nil {
		sessionStore, err = session.NewFileStore(sessionDir)
		if err != nil {
			return fmt.Errorf("create session: %w", err)
		}
	}
	if resumeCheckpoint != nil && resumeCheckpoint.WorkDir != "" {
		workDir = resumeCheckpoint.WorkDir
	}

	var debugRecorder provider.BodyDebugRecorder
	if cfg.Debug {
		debugRecorder, err = provider.NewJSONLDebugRecorder(filepath.Join(sessionStore.SessionDir(), "llm.jsonl"))
		if err != nil {
			return fmt.Errorf("create debug recorder: %w", err)
		}
	}

	modelClient, err := provider.New(provider.Options{
		Model:         cfg.Model,
		ModelURL:      cfg.ModelURL,
		APIKey:        cfg.APIKey,
		DebugRecorder: debugRecorder,
	})
	if err != nil {
		return err
	}
	resolvedProvider, err := provider.NameForModel(cfg.Model)
	if err != nil {
		return err
	}
	cfg.Provider = resolvedProvider
	if result, lookupErr := agent.ResolveAndCacheContextWindowTokens(ctx, agent.ContextWindowLookupOptions{
		Provider: cfg.Provider,
		Model:    cfg.Model,
		ModelURL: cfg.ModelURL,
		APIKey:   cfg.APIKey,
	}); lookupErr != nil {
		logger.Warnf("context window metadata lookup failed for provider=%s model=%s: %v", cfg.Provider, cfg.Model, lookupErr)
	} else {
		logger.Debugf("context window tokens resolved for provider=%s model=%s tokens=%d source=%s", cfg.Provider, result.Model, result.Tokens, result.Source)
	}

	initialAgentName := agent.AnalyzeAgentName
	if resumeCheckpoint != nil && strings.TrimSpace(resumeCheckpoint.AgentName) != "" {
		initialAgentName = resumeCheckpoint.AgentName
	}
	agentSelector, err := newAgentSelector(ctx, agent.Catalog, initialAgentName, agent.CreatAgentOptions{
		LLM:      modelClient,
		Model:    cfg.Model,
		Logger:   logger,
		MaxSteps: 20,
		WorkDir:  workDir,
		In:       in,
		Out:      out,
		Session:  sessionStore,
		Policy:   policy.New(policyMode),
	})
	if err != nil {
		return err
	}

	var runModel string
	if cfg.Command == "cli" {
		runModel = cfg.Model
	} else {
		runModel = "cmd"
	}
	env := content.Env{
		IO: content.IO{
			In:  in,
			Out: out,
			Err: errOut,
		},
		Agent:   agentSelector,
		Logger:  logger,
		Session: sessionStore,
		Config: content.Config{
			ConfigPath:       cfg.ConfigPath,
			Provider:         cfg.Provider,
			Model:            cfg.Model,
			ModelURL:         cfg.ModelURL,
			APIKeyConfigured: cfg.APIKey != "",
			AgentName:        agentSelector.ActiveAgentName(),
			WorkDir:          workDir,
			Debug:            cfg.Debug,
			PolicyMode:       cfg.PolicyMode,
		},
		RunModel: runModel,
	}
	runCtx := content.WithEnv(ctx, &env)

	if resumeCheckpoint != nil {
		if cfg.Command == "run" {
			return agentSelector.Resume(runCtx, *resumeCheckpoint)
		}
		if err := agentSelector.Resume(runCtx, *resumeCheckpoint); err != nil {
			return err
		}
	}
	if cfg.Command == "cli" {
		return cli.Run(runCtx, env, command.Manage)
	}
	return startupcmd.Run(runCtx, cfg, command.Manage, env)
}

func confirmResume(in io.Reader, out io.Writer, checkpoint session.ResumeCheckpoint) (bool, error) {
	if out != nil {
		if _, err := fmt.Fprintf(out, "Interrupted session found:\n  Session: %s\n  Agent: %s\n  Task: %s\n  Step: %d\nContinue from interrupted point? [y/N] ",
			checkpoint.SessionID, checkpoint.AgentName, checkpoint.Task, checkpoint.StepIndex); err != nil {
			return false, err
		}
	}
	if in == nil {
		return false, nil
	}
	answer, err := readLineUnbuffered(in)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true, nil
	default:
		if out != nil {
			_, err := fmt.Fprintln(out, "Starting a new session.")
			return false, err
		}
		return false, nil
	}
}

func readLineUnbuffered(in io.Reader) (string, error) {
	var b strings.Builder
	buf := make([]byte, 1)
	for {
		n, err := in.Read(buf)
		if n > 0 {
			switch buf[0] {
			case '\n':
				return b.String(), nil
			case '\r':
			default:
				b.WriteByte(buf[0])
			}
		}
		if err != nil {
			if err == io.EOF {
				return b.String(), nil
			}
			return "", fmt.Errorf("read resume confirmation: %w", err)
		}
	}
}

func applyFileConfig(cfg startup.Config) (startup.Config, error) {
	configPath := cfg.ConfigPath
	explicitConfig := configPath != ""
	if configPath == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return startup.Config{}, fmt.Errorf("resolve home directory: %w", err)
		}
		configPath = filepath.Join(homeDir, ".testAgent", "config.json")
	} else if !filepath.IsAbs(configPath) {
		absPath, err := filepath.Abs(configPath)
		if err != nil {
			return startup.Config{}, fmt.Errorf("resolve config path %s: %w", configPath, err)
		}
		configPath = absPath
	}

	fileConfig, found, err := appconfig.LoadOptional(configPath)
	if err != nil {
		return startup.Config{}, err
	}
	if !found {
		if explicitConfig {
			return startup.Config{}, fmt.Errorf("config file %s does not exist", configPath)
		}
		return cfg, nil
	}

	cfg.ConfigPath = configPath
	cfg.ModelURL = fileConfig.ModelURL
	cfg.APIKey = fileConfig.APIKey
	if fileConfig.ModelName != "" && !cfg.IsFlagSet("model") {
		cfg.Model = fileConfig.ModelName
	}
	if fileConfig.PolicyMode != "" && !cfg.IsFlagSet("policy-mode") {
		cfg.PolicyMode = fileConfig.PolicyMode
	}

	return cfg, nil
}

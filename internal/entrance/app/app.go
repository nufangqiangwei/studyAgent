package app

import (
	"agent/internal/capability/command"
	"agent/internal/content"
	"agent/internal/entrance/cli"
	"agent/internal/entrance/startupcmd"
	appconfig "agent/internal/foundation/config"
	provider2 "agent/internal/foundation/llmClient/provider"
	"agent/internal/foundation/logging"
	"agent/internal/foundation/policy"
	"agent/internal/foundation/startup"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"agent/internal/agent"
	"agent/internal/session"
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
	sessionStore, err := session.NewFileStore(sessionDir)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}

	var debugRecorder provider2.BodyDebugRecorder
	if cfg.Debug {
		debugRecorder, err = provider2.NewJSONLDebugRecorder(filepath.Join(sessionStore.SessionDir(), "llm.jsonl"))
		if err != nil {
			return fmt.Errorf("create debug recorder: %w", err)
		}
	}

	modelClient, err := provider2.New(provider2.Options{
		Model:         cfg.Model,
		ModelURL:      cfg.ModelURL,
		APIKey:        cfg.APIKey,
		DebugRecorder: debugRecorder,
	})
	if err != nil {
		return err
	}
	resolvedProvider, err := provider2.NameForModel(cfg.Model)
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

	agentSelector, err := newAgentSelector(ctx, agent.Catalog, agent.AnalyzeAgentName, agent.CreatAgentOptions{
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

	if cfg.Command == "cli" {
		return cli.Run(runCtx, env, command.Manage)
	}
	return startupcmd.Run(runCtx, cfg, command.Manage, env)
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

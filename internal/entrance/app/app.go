package app

import (
	"agent/internal/capability/command"
	"agent/internal/content"
	"agent/internal/entrance/runtimecli"
	"agent/internal/entrance/startupcmd"
	appconfig "agent/internal/foundation/config"
	"agent/internal/foundation/identity"
	provider2 "agent/internal/foundation/llmClient/provider"
	"agent/internal/foundation/logging"
	"agent/internal/foundation/policy"
	"agent/internal/foundation/startup"
	"agent/internal/runtime/agents/builtinagents"
	"agent/internal/runtime/contextmgr"
	"agent/internal/runtime/runservice"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	policyChecker := policy.New(policyMode)

	dataDir, err := defaultDataDir()
	if err != nil {
		return err
	}
	runtimeStoreRoot := filepath.Join(dataDir, "runtime")

	var debugRecorder provider2.BodyDebugRecorder
	if cfg.Debug {
		debugID, err := identity.New("debug")
		if err != nil {
			return err
		}
		debugRecorder, err = provider2.NewJSONLDebugRecorder(filepath.Join(dataDir, "debug", debugID, "llm.jsonl"))
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
	if result, lookupErr := contextmgr.ResolveAndCacheContextWindowTokens(ctx, contextmgr.ContextWindowLookupOptions{
		Provider: cfg.Provider,
		Model:    cfg.Model,
		ModelURL: cfg.ModelURL,
		APIKey:   cfg.APIKey,
	}); lookupErr != nil {
		logger.Warnf("context window metadata lookup failed for provider=%s model=%s: %v", cfg.Provider, cfg.Model, lookupErr)
	} else {
		logger.Debugf("context window tokens resolved for provider=%s model=%s tokens=%d source=%s", cfg.Provider, result.Model, result.Tokens, result.Source)
	}

	agentFactories, err := builtinagents.NewFactoryRegistry()
	if err != nil {
		return err
	}
	runtimeBuilder, err := newRuntimeSetupBuilder(runtimeSetupOptions{
		LLM:              modelClient,
		Model:            cfg.Model,
		MaxSteps:         20,
		WorkDir:          workDir,
		In:               in,
		Out:              out,
		RuntimeStoreRoot: runtimeStoreRoot,
		Policy:           policyChecker,
		Agents:           agentFactories,
	})
	if err != nil {
		return err
	}
	agentService, err := runservice.New(runservice.Options{
		Builder:      runtimeBuilder,
		Agents:       agentFactories,
		InitialAgent: builtinagents.DefaultFactoryName(),
		MaxSteps:     20,
		Out:          out,
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
		Agent:  agentService,
		Logger: logger,
		Config: content.Config{
			ConfigPath:       cfg.ConfigPath,
			Provider:         cfg.Provider,
			Model:            cfg.Model,
			ModelURL:         cfg.ModelURL,
			APIKeyConfigured: cfg.APIKey != "",
			AgentName:        agentService.ActiveAgentName(),
			WorkDir:          workDir,
			Debug:            cfg.Debug,
			PolicyMode:       cfg.PolicyMode,
		},
		RunModel: runModel,
	}
	runCtx := content.WithEnv(ctx, &env)

	if cfg.Command == "cli" {
		return runtimecli.Run(runCtx, env, command.Manage, runtimecli.Options{
			LLM:    modelClient,
			Policy: policyChecker,
			Agents: agentFactories,
		})
	}
	return startupcmd.Run(runCtx, cfg, command.Manage, env)
}

func defaultDataDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(homeDir, ".testAgent"), nil
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

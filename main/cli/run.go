package main

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/artifact"
	artifactlocal "agent/serviceruntime/artifact/local"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	persistencesqlite "agent/serviceruntime/persistence/sqlite"
	agentservice "agent/services/agent"
	"agent/services/approval"
	"agent/services/capability"
	"agent/services/interaction"
	"agent/services/llmClient"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

const (
	artifactStoreName                         = "agent-cli-local"
	cliSourceAddress  contract.ServiceAddress = "cli.external"
	maxCLIInputBytes  int                     = 16 << 20
)

type runOptions struct {
	modelClient llmClient.Client
	getenv      func(string) string
}

type application struct {
	runtime   *serviceruntime.Runtime
	storage   *persistencesqlite.Store
	artifacts *artifactlocal.Store
	ids       contract.IDGenerator
}

func Run(ctx context.Context, args []string, input io.Reader, output, stderr io.Writer) error {
	return runWithOptions(ctx, args, input, output, stderr, runOptions{})
}

func runWithOptions(ctx context.Context, args []string, input io.Reader, output, stderr io.Writer, options runOptions) error {
	config, err := parseConfig(args, stderr, options.getenv)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	presenter := newTerminalPresenter(output)
	app, err := buildApplication(ctx, config, presenter, options)
	if err != nil {
		return err
	}
	defer app.close()
	if _, err := app.runtime.Start(ctx); err != nil {
		return fmt.Errorf("start Runtime: %w", err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- app.runtime.Serve(ctx) }()

	if err := presenter.printf("Agent CLI (%s / %s)\n", config.Provider, config.Model); err != nil {
		return fmt.Errorf("write CLI banner: %w", err)
	}
	if err := presenter.println("Type /help for commands. Each input starts an independent Agent run."); err != nil {
		return fmt.Errorf("write CLI help: %w", err)
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), maxCLIInputBytes)
	for {
		if err := presenter.printf("\nyou> "); err != nil {
			return fmt.Errorf("write CLI prompt: %w", err)
		}
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read terminal input: %w", err)
			}
			_ = presenter.println()
			return nil
		}
		line := scanner.Text()
		command := strings.ToLower(strings.TrimSpace(line))
		switch command {
		case "":
			continue
		case "/exit", "/quit":
			return nil
		case "/help":
			if err := presenter.println("Commands: /help, /exit, /quit"); err != nil {
				return fmt.Errorf("write CLI commands: %w", err)
			}
			continue
		}
		requestID, err := app.ids.New("cli-request")
		if err != nil {
			return fmt.Errorf("create request id: %w", err)
		}
		submit := interaction.SubmitRequest{RequestID: requestID, Input: line}
		if int64(len(line)) > interaction.MaxInlineInputBytes {
			ref, err := app.runtime.WriteArtifact(ctx, artifact.WriteRequest{
				Key: "cli/inputs/" + requestID + ".txt", ContentType: "text/plain; charset=utf-8",
			}, strings.NewReader(line))
			if err != nil {
				return fmt.Errorf("store CLI input artifact: %w", err)
			}
			submit.Input, submit.InputArtifact = "", &ref
		}
		payload, err := json.Marshal(submit)
		if err != nil {
			return fmt.Errorf("encode interaction request: %w", err)
		}
		if _, err := app.runtime.Publish(ctx, contract.Message{
			Kind: contract.MessageCommand, Type: interaction.SubmitMessageType, Version: interaction.ProtocolVersion,
			From: cliSourceAddress, To: interaction.DefaultAddress,
			UserID: config.UserID, RunID: requestID, CorrelationID: requestID, Payload: payload,
		}); err != nil {
			return fmt.Errorf("publish interaction request: %w", err)
		}
		if err := presenter.wait(ctx, requestID, serveErrors); err != nil {
			return err
		}
	}
}

func buildApplication(ctx context.Context, config cliConfig, presenter interaction.Presenter, options runOptions) (*application, error) {
	storage, err := persistencesqlite.Open(ctx, filepath.Join(config.DataDir, "runtime.db"), persistencesqlite.Options{})
	if err != nil {
		return nil, fmt.Errorf("open Runtime storage: %w", err)
	}
	artifacts, err := artifactlocal.Open(filepath.Join(config.DataDir, "artifacts"), artifactlocal.Options{Name: artifactStoreName})
	if err != nil {
		_ = storage.Close()
		return nil, fmt.Errorf("open Artifact store: %w", err)
	}
	cleanup := func() {
		_ = artifacts.Close()
		_ = storage.Close()
	}

	modelOptions := []llmClient.ModuleOption{}
	if options.modelClient != nil {
		modelOptions = append(modelOptions, llmClient.WithClient(options.modelClient))
	}
	modelModule, err := llmClient.NewModule(llmClient.Config{
		BaseURL: config.BaseURL, APIKey: config.APIKey, Provider: config.Provider,
		ModelName: config.Model, Timeout: config.ModelTimeout,
	}, modelOptions...)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("configure LLM Client: %w", err)
	}
	approvalModule, err := approval.NewModule(approval.ModuleOptions{
		TrustedRequesters: []contract.ServiceAddress{capability.DefaultAddress},
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("configure Approval Service: %w", err)
	}
	capabilityModule, err := capability.NewModule(capability.ModuleOptions{
		Evaluator: capability.AuthorizationEvaluatorFunc(func(capability.AuthorizationInput) (capability.AuthorizationDecision, error) {
			return capability.AuthorizationDecision{
				Decision: capability.AuthorizationDeny, RuleRef: "cli-empty-capability-catalog@v1",
				ReasonCode: "capabilities_not_configured",
			}, nil
		}),
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("configure Capability Service: %w", err)
	}
	agentModule, err := agentservice.NewModule(agentservice.AgentSpec{
		Ref: "cli-agent", Version: "v1", SystemPrompt: config.SystemPrompt,
		MaxTurns: config.MaxTurns, MaxTokens: config.MaxTokens,
	}, serviceruntime.SystemClock{})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("configure Agent Service: %w", err)
	}
	interactionModule, err := interaction.NewModule(interaction.ModuleOptions{Presenter: presenter})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("configure Interaction Service: %w", err)
	}
	ids := serviceruntime.StableIDs{}
	builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
		Storage: storage, Artifacts: artifacts, IDs: ids,
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("create Runtime builder: %w", err)
	}
	registrations := []struct {
		name string
		run  func() error
	}{
		{"LLM Client", func() error { return modelModule.Register(builder) }},
		{"Approval Service", func() error { return approvalModule.Register(builder) }},
		{"Capability Service", func() error { return capabilityModule.Register(builder) }},
		{"Agent Service", func() error { return agentModule.Register(builder) }},
		{"Interaction Service", func() error { return interactionModule.Register(builder) }},
	}
	for _, registration := range registrations {
		if err := registration.run(); err != nil {
			cleanup()
			return nil, fmt.Errorf("register %s: %w", registration.name, err)
		}
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: contract.RuntimeID(config.RuntimeID), Revision: planRevision(config)},
		Services: []building.ServiceMount{
			modelModule.Mount(llmClient.DefaultAddress),
			approvalModule.Mount(approval.DefaultAddress, interaction.DefaultAddress, ""),
			capabilityModule.Mount(capability.DefaultAddress, approval.DefaultAddress, ""),
			agentModule.Mount(agentservice.DefaultAddress, llmClient.DefaultAddress, capability.DefaultAddress),
			interactionModule.Mount(interaction.DefaultAddress, agentservice.DefaultAddress),
		},
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("build Runtime: %w", err)
	}
	return &application{runtime: runtime, storage: storage, artifacts: artifacts, ids: ids}, nil
}

func planRevision(config cliConfig) contract.PlanRevision {
	payload, _ := json.Marshal(struct {
		Version      int    `json:"version"`
		SystemPrompt string `json:"system_prompt"`
		MaxTurns     int    `json:"max_turns"`
		MaxTokens    int    `json:"max_tokens"`
	}{1, config.SystemPrompt, config.MaxTurns, config.MaxTokens})
	sum := sha256.Sum256(payload)
	return contract.PlanRevision(fmt.Sprintf("cli-v1-%x", sum[:8]))
}

func (a *application) close() error {
	if a == nil {
		return nil
	}
	var first error
	if a.runtime != nil {
		first = a.runtime.Close()
	}
	if a.artifacts != nil {
		if err := a.artifacts.Close(); first == nil {
			first = err
		}
	}
	if a.storage != nil {
		if err := a.storage.Close(); first == nil {
			first = err
		}
	}
	return first
}

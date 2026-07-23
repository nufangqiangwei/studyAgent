package main

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/artifact"
	artifactlocal "agent/serviceruntime/artifact/local"
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/persistence"
	persistencesqlite "agent/serviceruntime/persistence/sqlite"
	agentservice "agent/services/agent"
	"agent/services/approval"
	"agent/services/capability"
	"agent/services/interaction"
	"agent/services/llmClient"
	"agent/services/task"
	"agent/services/webgateway"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	webArtifactStoreName       = "agent-web-local"
	webCompositionVersion      = "web-runtime-application@v1"
	webAgentRef                = "web-agent"
	webAgentVersion            = "v1"
	webCapabilityCatalogRef    = "web-empty-capability-catalog@v1"
	webAuthorizationRuleRef    = "web-empty-capability-catalog-deny@v1"
	webAuthorizationReasonCode = "capabilities_not_configured"
	webWorkspaceGuard          = "This Web runtime has no workspace capabilities. Do not claim to read, inspect, modify, or otherwise access the workspace."
)

type runtimeApplicationBuilder interface {
	RegisterService(building.ServiceDefinition) error
	RegisterEffect(effect.Spec) error
	RegisterPlanValidator(building.PlanValidator) error
	RegisterRuntimeBinder(assembly.RuntimeBinder) error
	Build(context.Context, building.RuntimeManifest) (*serviceruntime.Runtime, error)
}

type applicationOptions struct {
	modelClient llmClient.Client
	clock       contract.Clock
	ids         contract.IDGenerator

	openStorage  func(context.Context, string, persistencesqlite.Options) (persistence.RuntimeStorage, error)
	openArtifact func(string, artifactlocal.Options) (artifact.Store, error)
	newBuilder   func(serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error)
}

// application owns the process-local resources for one Web Runtime object
// graph. The Runtime is built but remains in RuntimeCreated until a later
// lifecycle integration explicitly starts it.
type application struct {
	runtime     *serviceruntime.Runtime
	runtimePort RuntimePort

	runtimeResource closeResource
	adapter         *RuntimeAdapter
	artifacts       artifact.Store
	storage         persistence.RuntimeStorage

	closeOnce sync.Once
	closeErr  error
}

type closeResource interface {
	Close() error
}

func buildApplication(ctx context.Context, config serverConfig, options applicationOptions) (*application, error) {
	options = options.withDefaults()
	storage, err := options.openStorage(
		ctx,
		filepath.Join(config.DataDir, "runtime.db"),
		persistencesqlite.Options{Clock: options.clock},
	)
	if err != nil {
		return nil, fmt.Errorf("open Runtime storage: %w", err)
	}
	artifacts, err := options.openArtifact(
		filepath.Join(config.DataDir, "artifacts"),
		artifactlocal.Options{Name: webArtifactStoreName},
	)
	if err != nil {
		_ = storage.Close()
		return nil, fmt.Errorf("open Artifact store: %w", err)
	}
	cleanup := func(adapter *RuntimeAdapter, runtime closeResource) {
		if runtime != nil {
			_ = runtime.Close()
		}
		if adapter != nil {
			_ = adapter.Close()
		}
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
		cleanup(nil, nil)
		return nil, fmt.Errorf("configure LLM Client: %w", err)
	}
	approvalModule, err := approval.NewModule(approval.ModuleOptions{
		Clock: options.clock, TrustedRequesters: []contract.ServiceAddress{capability.DefaultAddress},
	})
	if err != nil {
		cleanup(nil, nil)
		return nil, fmt.Errorf("configure Approval Service: %w", err)
	}
	capabilityModule, err := capability.NewModule(capability.ModuleOptions{
		Clock:     options.clock,
		Evaluator: capability.AuthorizationEvaluatorFunc(denyWebCapability),
	})
	if err != nil {
		cleanup(nil, nil)
		return nil, fmt.Errorf("configure Capability Service: %w", err)
	}
	agentModule, err := agentservice.NewModule(agentservice.AgentSpec{
		Ref: webAgentRef, Version: webAgentVersion,
		SystemPrompt: webAgentSystemPrompt(config.AgentSystemPrompt),
		Capabilities: nil, MaxTurns: config.AgentMaxTurns, MaxTokens: config.AgentMaxTokens,
	}, options.clock)
	if err != nil {
		cleanup(nil, nil)
		return nil, fmt.Errorf("configure Agent Service: %w", err)
	}
	agentMount := agentModule.Mount(agentservice.DefaultAddress, llmClient.DefaultAddress, capability.DefaultAddress)
	revision, err := webPlanRevision(config, agentMount.Config)
	if err != nil {
		cleanup(nil, nil)
		return nil, fmt.Errorf("create Web Runtime plan revision: %w", err)
	}
	adapterBinding := &runtimeAdapterBinding{
		expectedRuntimeID: contract.RuntimeID(config.RuntimeID),
		expectedRevision:  revision,
	}
	interactionModule, err := interaction.NewModule(interaction.ModuleOptions{
		Presenter: adapterBinding.interactionPresenter(), Clock: options.clock,
	})
	if err != nil {
		cleanup(nil, nil)
		return nil, fmt.Errorf("configure Interaction Service: %w", err)
	}
	webGatewayModule, err := webgateway.NewModule(webgateway.ModuleOptions{
		Presenter: adapterBinding.webPresenter(), Clock: options.clock,
	})
	if err != nil {
		cleanup(nil, nil)
		return nil, fmt.Errorf("configure Web Gateway Service: %w", err)
	}
	taskModule := task.NewModule(options.clock)

	builder, err := options.newBuilder(serviceruntime.BuilderOptions{
		Storage: storage, Artifacts: artifacts, Clock: options.clock, IDs: options.ids,
	})
	if err != nil {
		cleanup(nil, nil)
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
		{"Task Service", func() error { return taskModule.Register(builder) }},
		{"Web Gateway Service", func() error { return webGatewayModule.Register(builder) }},
	}
	for _, registration := range registrations {
		if err := registration.run(); err != nil {
			cleanup(adapterBinding.adapterSnapshot(), nil)
			return nil, fmt.Errorf("register %s: %w", registration.name, err)
		}
	}
	if err := builder.RegisterRuntimeBinder(adapterBinding); err != nil {
		cleanup(adapterBinding.adapterSnapshot(), nil)
		return nil, fmt.Errorf("register Web Runtime adapter: %w", err)
	}

	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: contract.RuntimeID(config.RuntimeID), Revision: revision},
		Services: []building.ServiceMount{
			modelModule.Mount(llmClient.DefaultAddress),
			approvalModule.Mount(approval.DefaultAddress, interaction.DefaultAddress, ""),
			capabilityModule.Mount(capability.DefaultAddress, approval.DefaultAddress, ""),
			agentMount,
			interactionModule.Mount(interaction.DefaultAddress, agentservice.DefaultAddress),
			webGatewayModule.Mount(webgateway.DefaultAddress),
		},
	})
	if err != nil {
		cleanup(adapterBinding.adapterSnapshot(), runtime)
		return nil, fmt.Errorf("build Runtime: %w", err)
	}
	adapter := adapterBinding.adapterSnapshot()
	if adapter == nil {
		cleanup(nil, runtime)
		return nil, fmt.Errorf("build Runtime: Web Runtime adapter was not bound")
	}
	return &application{
		runtime: runtime, runtimePort: adapter,
		runtimeResource: runtime, adapter: adapter, artifacts: artifacts, storage: storage,
	}, nil
}

func (o applicationOptions) withDefaults() applicationOptions {
	if o.clock == nil {
		o.clock = serviceruntime.SystemClock{}
	}
	if o.ids == nil {
		o.ids = serviceruntime.StableIDs{}
	}
	if o.openStorage == nil {
		o.openStorage = func(ctx context.Context, path string, options persistencesqlite.Options) (persistence.RuntimeStorage, error) {
			return persistencesqlite.Open(ctx, path, options)
		}
	}
	if o.openArtifact == nil {
		o.openArtifact = func(path string, options artifactlocal.Options) (artifact.Store, error) {
			return artifactlocal.Open(path, options)
		}
	}
	if o.newBuilder == nil {
		o.newBuilder = func(options serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error) {
			return serviceruntime.NewBuilder(options)
		}
	}
	return o
}

func webAgentSystemPrompt(configured string) string {
	configured = strings.TrimSpace(configured)
	if configured == "" {
		return webWorkspaceGuard
	}
	return configured + "\n\n" + webWorkspaceGuard
}

func denyWebCapability(capability.AuthorizationInput) (capability.AuthorizationDecision, error) {
	return capability.AuthorizationDecision{
		Decision: capability.AuthorizationDeny, RuleRef: webAuthorizationRuleRef,
		ReasonCode: webAuthorizationReasonCode,
	}, nil
}

func webPlanRevision(config serverConfig, agentConfig json.RawMessage) (contract.PlanRevision, error) {
	timeout := config.ModelTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	payload, err := json.Marshal(struct {
		CompositionVersion string          `json:"composition_version"`
		AgentConfig        json.RawMessage `json:"agent_config"`
		Model              struct {
			Provider string        `json:"provider"`
			Name     string        `json:"name"`
			BaseURL  string        `json:"base_url"`
			Timeout  time.Duration `json:"timeout"`
		} `json:"model"`
		CapabilityCatalogRef string `json:"capability_catalog_ref"`
		AuthorizationRuleRef string `json:"authorization_rule_ref"`
		WebGatewayProtocol   int    `json:"web_gateway_protocol"`
	}{
		CompositionVersion: webCompositionVersion,
		AgentConfig:        contract.CloneRaw(agentConfig),
		Model: struct {
			Provider string        `json:"provider"`
			Name     string        `json:"name"`
			BaseURL  string        `json:"base_url"`
			Timeout  time.Duration `json:"timeout"`
		}{
			Provider: strings.ToLower(strings.TrimSpace(config.Provider)),
			Name:     strings.TrimSpace(config.Model),
			BaseURL:  strings.TrimRight(strings.TrimSpace(config.BaseURL), "/"),
			Timeout:  timeout,
		},
		CapabilityCatalogRef: webCapabilityCatalogRef,
		AuthorizationRuleRef: webAuthorizationRuleRef,
		WebGatewayProtocol:   webgateway.ProtocolVersion,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return contract.PlanRevision(fmt.Sprintf("web-v1-%x", sum[:12])), nil
}

// Close is idempotent. Runtime is closed before the process-local adapter,
// Artifact Store, and SQLite storage; the externally durable resources retain
// the required Runtime -> Artifact -> SQLite shutdown order.
func (a *application) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		resources := []closeResource{a.runtimeResource, a.adapter, a.artifacts, a.storage}
		for _, resource := range resources {
			if resource == nil {
				continue
			}
			if err := resource.Close(); a.closeErr == nil {
				a.closeErr = err
			}
		}
	})
	return a.closeErr
}

// runtimeAdapterBinding is the only bridge between the composition root and
// the adapter. Builder supplies generic RuntimePorts after the durable bus is
// assembled; the binding passes only ingress, identity, and ID generation to
// RuntimeAdapter.
type runtimeAdapterBinding struct {
	expectedRuntimeID contract.RuntimeID
	expectedRevision  contract.PlanRevision

	mu      sync.RWMutex
	adapter *RuntimeAdapter
}

func (b *runtimeAdapterBinding) BindRuntime(ports assembly.RuntimePorts) error {
	if b == nil {
		return fmt.Errorf("Web Runtime adapter binding is nil")
	}
	if ports.RuntimeID != b.expectedRuntimeID || ports.PlanRevision != b.expectedRevision {
		return fmt.Errorf("Web Runtime adapter received unexpected runtime identity")
	}
	if ports.Ingress == nil || ports.IDs == nil {
		return fmt.Errorf("Web Runtime adapter requires durable ingress and id generator")
	}
	adapter, err := NewRuntimeAdapter(RuntimeAdapterOptions{
		Ingress: ports.Ingress, RuntimeID: ports.RuntimeID,
		PlanRevision: ports.PlanRevision, IDs: ports.IDs,
	})
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.adapter != nil {
		_ = adapter.Close()
		return fmt.Errorf("Web Runtime adapter is already bound")
	}
	b.adapter = adapter
	return nil
}

func (b *runtimeAdapterBinding) adapterSnapshot() *RuntimeAdapter {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.adapter
}

func (b *runtimeAdapterBinding) webPresenter() webgateway.Presenter {
	return webgateway.PresenterFunc(func(ctx context.Context, presentation webgateway.Presentation) error {
		adapter := b.adapterSnapshot()
		if adapter == nil {
			return ErrRuntimeUnavailable
		}
		return adapter.Present(ctx, presentation)
	})
}

func (b *runtimeAdapterBinding) interactionPresenter() interaction.Presenter {
	return interaction.PresenterFunc(func(ctx context.Context, presentation interaction.Presentation) error {
		adapter := b.adapterSnapshot()
		if adapter == nil {
			return ErrRuntimeUnavailable
		}
		return adapter.PresentInteraction(ctx, presentation)
	})
}

var _ assembly.RuntimeBinder = (*runtimeAdapterBinding)(nil)

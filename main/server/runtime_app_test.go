package main

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/artifact"
	artifactlocal "agent/serviceruntime/artifact/local"
	artifactmemory "agent/serviceruntime/artifact/memory"
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/persistence"
	persistencememory "agent/serviceruntime/persistence/memory"
	persistencesqlite "agent/serviceruntime/persistence/sqlite"
	agentservice "agent/services/agent"
	"agent/services/approval"
	"agent/services/capability"
	"agent/services/interaction"
	"agent/services/llmClient"
	"agent/services/task"
	"agent/services/webgateway"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type runtimeAppTestClock struct {
	now time.Time
}

func (c runtimeAppTestClock) Now() time.Time { return c.now }

type runtimeAppTestIDs struct {
	mu   sync.Mutex
	next int
}

func (g *runtimeAppTestIDs) New(kind string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return fmt.Sprintf("%s-%d", kind, g.next), nil
}

func (*runtimeAppTestIDs) Derive(kind string, parts ...string) string {
	return kind + "-" + strings.Join(parts, "-")
}

type noNetworkModelClient struct{}

func (noNetworkModelClient) Complete(context.Context, llmClient.ClientRequest, string) (llmClient.Completion, error) {
	return llmClient.Completion{}, errors.New("test model must not be called")
}

type recordingApplicationBuilder struct {
	delegate    runtimeApplicationBuilder
	definitions []building.ServiceDefinition
	manifest    building.RuntimeManifest
}

func (b *recordingApplicationBuilder) RegisterService(definition building.ServiceDefinition) error {
	b.definitions = append(b.definitions, definition)
	return b.delegate.RegisterService(definition)
}

func (b *recordingApplicationBuilder) RegisterEffect(spec effect.Spec) error {
	return b.delegate.RegisterEffect(spec)
}

func (b *recordingApplicationBuilder) RegisterPlanValidator(validator building.PlanValidator) error {
	return b.delegate.RegisterPlanValidator(validator)
}

func (b *recordingApplicationBuilder) RegisterRuntimeBinder(binder assembly.RuntimeBinder) error {
	return b.delegate.RegisterRuntimeBinder(binder)
}

func (b *recordingApplicationBuilder) Build(ctx context.Context, manifest building.RuntimeManifest) (*serviceruntime.Runtime, error) {
	b.manifest = manifest
	return b.delegate.Build(ctx, manifest)
}

func TestBuildApplicationComposesUnstartedWebRuntime(t *testing.T) {
	ctx := context.Background()
	clock := runtimeAppTestClock{now: time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC)}
	ids := &runtimeAppTestIDs{}
	storage := persistencememory.New(clock)
	artifacts, err := artifactmemory.New(webArtifactStoreName)
	if err != nil {
		t.Fatal(err)
	}
	var storagePath string
	var artifactPath string
	var recorder *recordingApplicationBuilder
	config := validRuntimeApplicationConfig(t.TempDir())

	app, err := buildApplication(ctx, config, applicationOptions{
		modelClient: noNetworkModelClient{},
		clock:       clock,
		ids:         ids,
		openStorage: func(_ context.Context, path string, options persistencesqlite.Options) (persistence.RuntimeStorage, error) {
			storagePath = path
			if options.Clock != clock {
				t.Fatalf("storage clock was not injected")
			}
			return storage, nil
		},
		openArtifact: func(path string, options artifactlocal.Options) (artifact.Store, error) {
			artifactPath = path
			if options.Name != webArtifactStoreName {
				t.Fatalf("artifact store name=%q", options.Name)
			}
			return artifacts, nil
		},
		newBuilder: func(options serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error) {
			if options.Storage != storage || options.Artifacts != artifacts || options.Clock != clock || options.IDs != ids {
				t.Fatalf("builder did not receive the injected Runtime resources")
			}
			delegate, buildErr := serviceruntime.NewBuilder(options)
			if buildErr != nil {
				return nil, buildErr
			}
			recorder = &recordingApplicationBuilder{delegate: delegate}
			return recorder, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if storagePath != filepath.Join(config.DataDir, "runtime.db") {
		t.Fatalf("storage path=%q", storagePath)
	}
	if artifactPath != filepath.Join(config.DataDir, "artifacts") {
		t.Fatalf("artifact path=%q", artifactPath)
	}
	if app.runtime == nil || app.runtimePort == nil || app.adapter == nil {
		t.Fatal("application did not return Runtime and real RuntimePort")
	}
	if app.runtimePort != app.adapter {
		t.Fatal("application RuntimePort is not the bound RuntimeAdapter")
	}
	if status := app.runtime.Status(); status != serviceruntime.RuntimeCreated {
		t.Fatalf("runtime status=%q, want %q", status, serviceruntime.RuntimeCreated)
	}

	expectedMounts := map[contract.ServiceAddress]contract.ComponentRef{
		llmClient.DefaultAddress:    llmClient.Component,
		approval.DefaultAddress:     approval.Component,
		capability.DefaultAddress:   capability.Component,
		agentservice.DefaultAddress: agentservice.Component,
		interaction.DefaultAddress:  interaction.Component,
		webgateway.DefaultAddress:   webgateway.Component,
	}
	plan := app.runtime.Plan()
	for address, component := range expectedMounts {
		planned, found := plan.Service(address)
		if !found || planned.Component != component {
			t.Fatalf("plan service %q=%#v, found=%v", address, planned.Component, found)
		}
	}
	for _, planned := range plan.Services() {
		if planned.Component == task.Component {
			t.Fatalf("virtual Task component was statically mounted at %q", planned.Address)
		}
	}

	taskDefinition := definitionFor(t, recorder.definitions, task.Component)
	if taskDefinition.Scope != building.ScopeVirtual {
		t.Fatalf("task scope=%q", taskDefinition.Scope)
	}
	webDefinition := definitionFor(t, recorder.definitions, webgateway.Component)
	if !containsString(webDefinition.SystemOperations, assembly.SystemOperationDeclareInstance) {
		t.Fatalf("web gateway system operations=%v", webDefinition.SystemOperations)
	}
	capabilityDefinition := definitionFor(t, recorder.definitions, capability.Component)
	if len(capabilityDefinition.EffectExecutors) != 0 {
		t.Fatalf("empty capability catalog registered executors=%v", capabilityDefinition.EffectExecutors)
	}

	agentPlan, _ := plan.Service(agentservice.DefaultAddress)
	var spec agentservice.AgentSpec
	if err := json.Unmarshal(agentPlan.Config, &spec); err != nil {
		t.Fatalf("decode agent spec: %v", err)
	}
	if len(spec.Capabilities) != 0 {
		t.Fatalf("agent capabilities=%v", spec.Capabilities)
	}
	if !strings.Contains(spec.SystemPrompt, webWorkspaceGuard) {
		t.Fatalf("agent system prompt lacks workspace guard: %q", spec.SystemPrompt)
	}
	if _, err := app.runtime.DeclareInstance(ctx, serviceruntime.InstanceDeclaration{
		Address: "task.test", Component: task.Component,
	}); err != nil {
		t.Fatalf("declare virtual Task instance: %v", err)
	}

	decision, err := denyWebCapability(capability.AuthorizationInput{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Decision != capability.AuthorizationDeny ||
		decision.RuleRef != webAuthorizationRuleRef ||
		!strings.Contains(decision.RuleRef, "@v1") {
		t.Fatalf("authorization decision=%#v", decision)
	}
}

func TestBuildApplicationUsesSQLiteAndLocalArtifactStore(t *testing.T) {
	dataDir := t.TempDir()
	app, err := buildApplication(
		context.Background(),
		validRuntimeApplicationConfig(dataDir),
		applicationOptions{modelClient: noNetworkModelClient{}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := app.storage.(*persistencesqlite.Store); !ok {
		t.Fatalf("storage type=%T", app.storage)
	}
	if _, ok := app.artifacts.(*artifactlocal.Store); !ok {
		t.Fatalf("artifact type=%T", app.artifacts)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "runtime.db")); err != nil {
		t.Fatalf("stat runtime.db: %v", err)
	}
	for _, directory := range []string{
		filepath.Join(dataDir, "artifacts", "staging"),
		filepath.Join(dataDir, "artifacts", "objects"),
	} {
		if info, err := os.Stat(directory); err != nil || !info.IsDir() {
			t.Fatalf("artifact directory %q: info=%v err=%v", directory, info, err)
		}
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := app.runtimePort.CreateTask(context.Background(), Actor{UserID: "user-1"}, CreateTaskInput{Input: "test"}); !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("closed RuntimePort error=%v", err)
	}
}

func TestWebPlanRevisionIsDeterministicVersionedAndSecretFree(t *testing.T) {
	config := validRuntimeApplicationConfig(`C:\machine-a\private-runtime`)
	config.APIKey = "super-secret-api-key"
	agentConfig := json.RawMessage(`{"ref":"web-agent","version":"v1","system_prompt":"guarded","capabilities":[],"max_turns":8}`)

	first, err := webPlanRevision(config, agentConfig)
	if err != nil {
		t.Fatal(err)
	}
	second, err := webPlanRevision(config, agentConfig)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || !strings.HasPrefix(string(first), "web-v1-") {
		t.Fatalf("plan revisions=%q, %q", first, second)
	}
	localChange := config
	localChange.APIKey = "different-secret"
	localChange.DataDir = `D:\machine-b\other-runtime`
	localRevision, err := webPlanRevision(localChange, agentConfig)
	if err != nil {
		t.Fatal(err)
	}
	if localRevision != first {
		t.Fatalf("machine-local or secret configuration changed revision: %q != %q", localRevision, first)
	}
	modelChange := config
	modelChange.Model = "different-model"
	changedRevision, err := webPlanRevision(modelChange, agentConfig)
	if err != nil {
		t.Fatal(err)
	}
	if changedRevision == first {
		t.Fatal("model configuration did not change plan revision")
	}
	agentChange := append(json.RawMessage(nil), agentConfig...)
	agentChange = json.RawMessage(strings.Replace(string(agentChange), `"max_turns":8`, `"max_turns":9`, 1))
	changedRevision, err = webPlanRevision(config, agentChange)
	if err != nil {
		t.Fatal(err)
	}
	if changedRevision == first {
		t.Fatal("AgentSpec behavior did not change plan revision")
	}
}

func TestBuildApplicationManifestExcludesSecretsAndMachineLocalPaths(t *testing.T) {
	config := validRuntimeApplicationConfig(`C:\machine-private\runtime-data`)
	config.APIKey = "api-key-must-not-be-persisted"
	var captured building.RuntimeManifest

	app, err := buildApplication(context.Background(), config, injectedRuntimeApplicationOptions(t, func(options serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error) {
		delegate, buildErr := serviceruntime.NewBuilder(options)
		if buildErr != nil {
			return nil, buildErr
		}
		return &manifestCapturingBuilder{runtimeApplicationBuilder: delegate, capture: &captured}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	data, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(data)
	if strings.Contains(encoded, config.APIKey) || strings.Contains(encoded, config.DataDir) {
		t.Fatalf("manifest contains secret or machine-local path: %s", encoded)
	}
	if captured.Runtime.Revision == "" {
		t.Fatal("manifest has no plan revision")
	}
}

func TestBuildApplicationClosesResourcesOnEveryFailure(t *testing.T) {
	buildFailure := errors.New("build failed")
	registerFailure := errors.New("register failed")
	builderFailure := errors.New("builder failed")
	moduleFailureConfig := validRuntimeApplicationConfig("runtime-data")
	moduleFailureConfig.BaseURL = "not-an-absolute-url"

	tests := []struct {
		name    string
		config  serverConfig
		builder func(serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error)
		wantErr error
	}{
		{
			name: "module configuration", config: moduleFailureConfig,
			builder: func(serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error) {
				t.Fatal("builder should not be created")
				return nil, nil
			},
		},
		{
			name: "builder creation", config: validRuntimeApplicationConfig("runtime-data"),
			builder: func(serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error) {
				return nil, builderFailure
			},
			wantErr: builderFailure,
		},
		{
			name: "module registration", config: validRuntimeApplicationConfig("runtime-data"),
			builder: func(serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error) {
				return &failingApplicationBuilder{registerErr: registerFailure}, nil
			},
			wantErr: registerFailure,
		},
		{
			name: "runtime build", config: validRuntimeApplicationConfig("runtime-data"),
			builder: func(serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error) {
				return &failingApplicationBuilder{buildErr: buildFailure}, nil
			},
			wantErr: buildFailure,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := &closeLog{}
			storage := &trackedStorage{RuntimeStorage: persistencememory.New(runtimeAppTestClock{now: time.Now().UTC()}), name: "sqlite", log: log}
			memoryArtifacts, err := artifactmemory.New(webArtifactStoreName)
			if err != nil {
				t.Fatal(err)
			}
			artifacts := &trackedArtifacts{Store: memoryArtifacts, name: "artifact", log: log}
			_, err = buildApplication(context.Background(), test.config, applicationOptions{
				modelClient: noNetworkModelClient{},
				clock:       runtimeAppTestClock{now: time.Now().UTC()},
				ids:         &runtimeAppTestIDs{},
				openStorage: func(context.Context, string, persistencesqlite.Options) (persistence.RuntimeStorage, error) {
					return storage, nil
				},
				openArtifact: func(string, artifactlocal.Options) (artifact.Store, error) {
					return artifacts, nil
				},
				newBuilder: test.builder,
			})
			if err == nil {
				t.Fatal("build unexpectedly succeeded")
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("error=%v, want %v", err, test.wantErr)
			}
			if got := log.snapshot(); fmt.Sprint(got) != fmt.Sprint([]string{"artifact", "sqlite"}) {
				t.Fatalf("close order=%v", got)
			}
		})
	}
}

func TestBuildApplicationArtifactOpenFailureClosesSQLite(t *testing.T) {
	log := &closeLog{}
	storage := &trackedStorage{RuntimeStorage: persistencememory.New(runtimeAppTestClock{now: time.Now().UTC()}), name: "sqlite", log: log}
	artifactFailure := errors.New("artifact open failed")
	_, err := buildApplication(context.Background(), validRuntimeApplicationConfig("runtime-data"), applicationOptions{
		modelClient: noNetworkModelClient{},
		openStorage: func(context.Context, string, persistencesqlite.Options) (persistence.RuntimeStorage, error) {
			return storage, nil
		},
		openArtifact: func(string, artifactlocal.Options) (artifact.Store, error) {
			return nil, artifactFailure
		},
	})
	if !errors.Is(err, artifactFailure) {
		t.Fatalf("error=%v", err)
	}
	if got := log.snapshot(); fmt.Sprint(got) != fmt.Sprint([]string{"sqlite"}) {
		t.Fatalf("close order=%v", got)
	}
}

func TestApplicationCloseIsIdempotentAndOrdered(t *testing.T) {
	log := &closeLog{}
	storage := &trackedStorage{RuntimeStorage: persistencememory.New(runtimeAppTestClock{now: time.Now().UTC()}), name: "sqlite", log: log}
	memoryArtifacts, err := artifactmemory.New(webArtifactStoreName)
	if err != nil {
		t.Fatal(err)
	}
	artifacts := &trackedArtifacts{Store: memoryArtifacts, name: "artifact", log: log}
	app := &application{
		runtimeResource: &trackedCloser{name: "runtime", log: log},
		artifacts:       artifacts,
		storage:         storage,
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	if err := app.Close(); err != nil {
		t.Fatal(err)
	}
	if got := log.snapshot(); fmt.Sprint(got) != fmt.Sprint([]string{"runtime", "artifact", "sqlite"}) {
		t.Fatalf("close order=%v", got)
	}
}

type manifestCapturingBuilder struct {
	runtimeApplicationBuilder
	capture *building.RuntimeManifest
}

func (b *manifestCapturingBuilder) Build(ctx context.Context, manifest building.RuntimeManifest) (*serviceruntime.Runtime, error) {
	*b.capture = manifest
	return b.runtimeApplicationBuilder.Build(ctx, manifest)
}

func injectedRuntimeApplicationOptions(
	t *testing.T,
	builderFactory func(serviceruntime.BuilderOptions) (runtimeApplicationBuilder, error),
) applicationOptions {
	t.Helper()
	clock := runtimeAppTestClock{now: time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC)}
	storage := persistencememory.New(clock)
	artifacts, err := artifactmemory.New(webArtifactStoreName)
	if err != nil {
		t.Fatal(err)
	}
	return applicationOptions{
		modelClient: noNetworkModelClient{}, clock: clock, ids: &runtimeAppTestIDs{},
		openStorage: func(context.Context, string, persistencesqlite.Options) (persistence.RuntimeStorage, error) {
			return storage, nil
		},
		openArtifact: func(string, artifactlocal.Options) (artifact.Store, error) {
			return artifacts, nil
		},
		newBuilder: builderFactory,
	}
}

func validRuntimeApplicationConfig(dataDir string) serverConfig {
	return serverConfig{
		DataDir: dataDir, RuntimeID: "agent-server-test",
		Provider: llmClient.ProviderOpenAI, Model: "test-model", BaseURL: "https://model.example/v1",
		APIKey: "test-only-secret", ModelTimeout: time.Minute,
		AgentSystemPrompt: "Answer accurately.", AgentMaxTurns: 8, AgentMaxTokens: 256,
	}
}

func definitionFor(t *testing.T, definitions []building.ServiceDefinition, component contract.ComponentRef) building.ServiceDefinition {
	t.Helper()
	for _, definition := range definitions {
		if definition.Component == component {
			return definition
		}
	}
	t.Fatalf("definition %q was not registered", component.String())
	return building.ServiceDefinition{}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type failingApplicationBuilder struct {
	registerErr error
	buildErr    error
}

func (b *failingApplicationBuilder) RegisterService(building.ServiceDefinition) error {
	if b.registerErr != nil {
		err := b.registerErr
		b.registerErr = nil
		return err
	}
	return nil
}

func (*failingApplicationBuilder) RegisterEffect(effect.Spec) error { return nil }

func (*failingApplicationBuilder) RegisterPlanValidator(building.PlanValidator) error { return nil }

func (*failingApplicationBuilder) RegisterRuntimeBinder(assembly.RuntimeBinder) error { return nil }

func (b *failingApplicationBuilder) Build(context.Context, building.RuntimeManifest) (*serviceruntime.Runtime, error) {
	return nil, b.buildErr
}

type closeLog struct {
	mu     sync.Mutex
	values []string
}

func (l *closeLog) record(value string) {
	l.mu.Lock()
	l.values = append(l.values, value)
	l.mu.Unlock()
}

func (l *closeLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.values...)
}

type trackedCloser struct {
	name string
	log  *closeLog
}

func (c *trackedCloser) Close() error {
	c.log.record(c.name)
	return nil
}

type trackedStorage struct {
	persistence.RuntimeStorage
	name string
	log  *closeLog
}

func (s *trackedStorage) Close() error {
	s.log.record(s.name)
	return s.RuntimeStorage.Close()
}

type trackedArtifacts struct {
	artifact.Store
	name string
	log  *closeLog
}

func (s *trackedArtifacts) Close() error {
	s.log.record(s.name)
	return s.Store.Close()
}

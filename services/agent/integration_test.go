package agent

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/artifact"
	artifactmemory "agent/serviceruntime/artifact/memory"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/persistence"
	persistencememory "agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/service"
	"agent/services/approval"
	"agent/services/capability"
	"agent/services/llmClient"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	ownerComponent       = contract.ComponentRef{Type: "test.agent-owner", Version: "v1"}
	interactionComponent = contract.ComponentRef{Type: "test.agent-interaction", Version: "v1"}
)

type scriptedModel struct {
	mu       sync.Mutex
	outputs  []string
	requests []llmClient.ClientRequest
}

func (m *scriptedModel) Complete(_ context.Context, request llmClient.ClientRequest, _ string) (llmClient.Completion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, request)
	if len(m.outputs) == 0 {
		return llmClient.Completion{}, fmt.Errorf("model script is exhausted")
	}
	value := m.outputs[0]
	m.outputs = m.outputs[1:]
	return llmClient.Completion{Content: value}, nil
}

func (m *scriptedModel) snapshot() []llmClient.ClientRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]llmClient.ClientRequest(nil), m.requests...)
}

type echoExecutor struct{}

func (echoExecutor) ExecuteEffect(_ context.Context, record persistence.EffectRecord) (effect.ExecutionResult, error) {
	return effect.ExecutionResult{Payload: contract.CloneRaw(record.Payload)}, nil
}

func (echoExecutor) ReconcileEffect(_ context.Context, record persistence.EffectRecord) (effect.ReconciliationResult, error) {
	return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: contract.CloneRaw(record.Payload)}, nil
}

type ownerService struct{ results chan ExecuteResult }

func (*ownerService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: ownerComponent}
}

func (*ownerService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (o *ownerService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	var result ExecuteResult
	if err := json.Unmarshal(message.Payload, &result); err != nil {
		return service.Decision{}, err
	}
	o.results <- result
	return service.Decision{}, nil
}

func (*ownerService) Apply(service.State, contract.StoredEvent) (service.State, error) {
	return service.State{}, fmt.Errorf("owner does not persist events")
}

type interactionService struct{}

func (*interactionService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: interactionComponent}
}

func (*interactionService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (*interactionService) Handle(context.Context, service.State, contract.Message) (service.Decision, error) {
	return service.Decision{}, nil
}

func (*interactionService) Apply(service.State, contract.StoredEvent) (service.State, error) {
	return service.State{}, fmt.Errorf("interaction does not persist events")
}

type recordingObserver struct {
	mu     sync.Mutex
	events []contract.RuntimeEvent
}

func (o *recordingObserver) RecordRuntimeEvent(_ context.Context, event contract.RuntimeEvent) error {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
	return nil
}

func (o *recordingObserver) snapshot() []contract.RuntimeEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]contract.RuntimeEvent(nil), o.events...)
}

func TestSingleAgentRunsModelCapabilityModelToCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	artifacts, err := artifactmemory.New("agent-integration-artifacts")
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	model := &scriptedModel{outputs: []string{
		`{"action":"capability","capability_ref":"test.echo","capability_version":"v1","arguments":{"value":"hello"}}`,
		`{"action":"finish","answer":"task completed"}`,
	}}
	modelModule, err := llmClient.NewModule(llmClient.Config{
		BaseURL: "https://model.example/v1", Provider: llmClient.ProviderOpenAI, ModelName: "test-model",
	}, llmClient.WithClient(model))
	if err != nil {
		t.Fatal(err)
	}
	approvalModule, err := approval.NewModule(approval.ModuleOptions{TrustedRequesters: []contract.ServiceAddress{capability.DefaultAddress}})
	if err != nil {
		t.Fatal(err)
	}
	capabilityModule, err := capability.NewModule(capability.ModuleOptions{
		Evaluator: capability.AuthorizationEvaluatorFunc(func(capability.AuthorizationInput) (capability.AuthorizationDecision, error) {
			return capability.AuthorizationDecision{
				Decision: capability.AuthorizationAllow, RuleRef: "allow-test@v1", ReasonCode: "test_allowed",
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := capability.CapabilityDescriptor{
		Ref: "test.echo", Version: "v1", ProviderRef: "test-provider",
		ExecutionKind: capability.ExecutionEffect, ExecutorRef: "test.echo@v1", EffectType: "test.echo",
		DescriptorRevision: "echo-descriptor-1",
	}
	if err := capabilityModule.RegisterProvider(capability.ProviderFunc{
		ProviderRef: "test-provider", Descriptors: []capability.CapabilityDescriptor{descriptor},
		PlanFunc: func(_ context.Context, input capability.CapabilityInvocation) (capability.CapabilityExecutionPlan, error) {
			return capability.CapabilityExecutionPlan{
				Kind: capability.ExecutionEffect, ExecutionKey: "echo",
				Effect: &capability.EffectPlan{
					Type: "test.echo", Version: 1, ExecutorRef: "test.echo@v1", Payload: contract.CloneRaw(input.Arguments.Inline),
				},
			}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := capabilityModule.RegisterExecutor(effect.Spec{
		Ref: "test.echo@v1", Type: "test.echo", Executor: echoExecutor{}, Reconciler: echoExecutor{},
	}); err != nil {
		t.Fatal(err)
	}
	agentModule, err := NewModule(AgentSpec{
		Ref: "coding-agent", Version: "v1", SystemPrompt: "Complete the user's coding task and report the actual result.",
		Capabilities: []CapabilityPrompt{{
			Ref: "test.echo", Version: "v1", Description: "Echo structured test input.",
			ArgumentsSchema: json.RawMessage(`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}`),
		}},
	}, serviceruntime.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
		Artifacts: artifacts, IDs: serviceruntime.StableIDs{}, OwnerID: "agent-integration-owner", Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, register := range []func() error{
		func() error { return modelModule.Register(builder) },
		func() error { return approvalModule.Register(builder) },
		func() error { return capabilityModule.Register(builder) },
		func() error { return agentModule.Register(builder) },
	} {
		if err := register(); err != nil {
			t.Fatal(err)
		}
	}
	results := make(chan ExecuteResult, 1)
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: ownerComponent,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &ownerService{results: results}, nil
		}),
		Scope:    building.ScopeMounted,
		Consumes: []building.MessageContract{{Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: interactionComponent,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &interactionService{}, nil
		}),
		Scope:    building.ScopeMounted,
		Consumes: []building.MessageContract{{Kind: contract.MessageEvent, Type: approval.RequestedEventType, Version: approval.ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "agent-integration", Revision: "v1"},
		Services: []building.ServiceMount{
			modelModule.Mount(llmClient.DefaultAddress),
			approvalModule.Mount(approval.DefaultAddress, "interaction.main", ""),
			capabilityModule.Mount(capability.DefaultAddress, approval.DefaultAddress, ""),
			agentModule.Mount(DefaultAddress, llmClient.DefaultAddress, capability.DefaultAddress),
			{Address: "owner.main", Component: ownerComponent},
			{Address: "interaction.main", Component: interactionComponent},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- runtime.Serve(ctx) }()
	payload, _ := json.Marshal(ExecuteRequest{RunID: "run-integration", Input: "Use the echo capability, then finish."})
	if _, err := runtime.Publish(ctx, contract.Message{
		ID: "execute-integration", Kind: contract.MessageCommand, Type: ExecuteMessageType, Version: ProtocolVersion,
		From: "owner.main", To: DefaultAddress, ReplyTo: "owner.main", UserID: "user-1", RunID: "run-integration",
		Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	var result ExecuteResult
	select {
	case result = <-results:
	case err := <-serveErrors:
		t.Fatalf("runtime serve stopped before completion: %v", err)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for agent completion; events=%#v model_requests=%#v", observer.snapshot(), model.snapshot())
	}
	if result.Phase != PhaseCompleted || result.Output == nil || result.Turns != 2 {
		t.Fatalf("result=%#v", result)
	}
	reader, _, err := runtime.OpenArtifact(ctx, *result.Output)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(content) != "task completed" {
		t.Fatalf("output=%q err=%v", content, err)
	}
	requests := model.snapshot()
	if len(requests) != 2 {
		t.Fatalf("model requests=%d", len(requests))
	}
	secondPrompt := ""
	for _, message := range requests[1].Messages {
		secondPrompt += message.Content
	}
	if !strings.Contains(secondPrompt, `"value":"hello"`) || !strings.Contains(secondPrompt, "Capability result") {
		t.Fatalf("second prompt did not contain capability result:\n%s", secondPrompt)
	}
}

type recoveryCapabilityService struct{}

func (*recoveryCapabilityService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: capability.Component}
}

func (*recoveryCapabilityService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (*recoveryCapabilityService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Kind != contract.MessageQuery || message.Type != capability.ListMessageType {
		return service.Decision{}, fmt.Errorf("unexpected recovery capability message %q", message.Type)
	}
	payload, _ := json.Marshal(capability.ListResponse{})
	return service.Decision{Reply: &service.Reply{
		Key: "recovery-capabilities", Type: capability.ResultMessageType,
		Version: capability.ProtocolVersion, Payload: payload,
	}}, nil
}

func (*recoveryCapabilityService) Apply(service.State, contract.StoredEvent) (service.State, error) {
	return service.State{}, fmt.Errorf("recovery capability does not persist events")
}

type recoveryModelService struct{ response contract.ArtifactRef }

func (*recoveryModelService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: llmClient.Component}
}

func (*recoveryModelService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (s *recoveryModelService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	var request llmClient.CompletionRequest
	if err := json.Unmarshal(message.Payload, &request); err != nil {
		return service.Decision{}, err
	}
	payload, _ := json.Marshal(llmClient.CompletionReply{
		RequestID: request.RequestID, ArtifactKey: s.response.Key, Artifact: s.response,
		Provider: "recovery", ModelName: "recovery-model",
	})
	return service.Decision{Reply: &service.Reply{
		Key: "recovery-model/" + request.RequestID, Type: llmClient.CompletedMessageType,
		Version: llmClient.ProtocolVersion, Payload: payload,
	}}, nil
}

func (*recoveryModelService) Apply(service.State, contract.StoredEvent) (service.State, error) {
	return service.State{}, fmt.Errorf("recovery model does not persist events")
}

func buildRecoveryRuntime(t *testing.T, ctx context.Context, storage persistence.RuntimeStorage, artifacts artifact.Store, response contract.ArtifactRef, results chan ExecuteResult, ownerID string) *serviceruntime.Runtime {
	t.Helper()
	agents, err := NewModule(AgentSpec{
		Ref: "recovery-agent", Version: "v1", SystemPrompt: "Complete the task.", MaxTurns: 2,
	}, serviceruntime.SystemClock{})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
		Storage: storage, Artifacts: artifacts, IDs: serviceruntime.StableIDs{}, OwnerID: ownerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agents.Register(builder); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: capability.Component,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &recoveryCapabilityService{}, nil
		}),
		Scope:    building.ScopeRuntimeSingleton,
		Consumes: []building.MessageContract{{Kind: contract.MessageQuery, Type: capability.ListMessageType, Version: capability.ProtocolVersion}},
		Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: capability.ResultMessageType, Version: capability.ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: llmClient.Component,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &recoveryModelService{response: response}, nil
		}),
		Scope:    building.ScopeRuntimeSingleton,
		Consumes: []building.MessageContract{{Kind: contract.MessageCommand, Type: llmClient.CompleteMessageType, Version: llmClient.ProtocolVersion}},
		Produces: []building.MessageContract{{Kind: contract.MessageReply, Type: llmClient.CompletedMessageType, Version: llmClient.ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: ownerComponent,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &ownerService{results: results}, nil
		}),
		Scope:    building.ScopeMounted,
		Consumes: []building.MessageContract{{Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "agent-recovery", Revision: "v1"},
		Services: []building.ServiceMount{
			agents.Mount(DefaultAddress, llmClient.DefaultAddress, capability.DefaultAddress),
			{Address: llmClient.DefaultAddress, Component: llmClient.Component},
			{Address: capability.DefaultAddress, Component: capability.Component},
			{Address: "owner.main", Component: ownerComponent},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(ctx); err != nil {
		runtime.Close()
		t.Fatal(err)
	}
	return runtime
}

func TestPendingPromptEffectAndRunStateRecoverAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	artifacts, err := artifactmemory.New("agent-recovery-artifacts")
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	response, err := artifact.WriteAll(ctx, artifacts, artifact.WriteRequest{
		Key: "tests/recovery-model-response.json", ContentType: "application/json",
	}, strings.NewReader(`{"action":"finish","answer":"recovered completion"}`))
	if err != nil {
		t.Fatal(err)
	}
	storage := persistencememory.New(serviceruntime.SystemClock{})
	defer storage.Close()
	results := make(chan ExecuteResult, 1)
	first := buildRecoveryRuntime(t, ctx, storage, artifacts, response, results, "agent-recovery-owner-1")
	payload, _ := json.Marshal(ExecuteRequest{RunID: "run-recovery", Input: "finish after restart"})
	if _, err := first.Publish(ctx, contract.Message{
		ID: "execute-recovery", Kind: contract.MessageCommand, Type: ExecuteMessageType, Version: ProtocolVersion,
		From: "owner.main", To: DefaultAddress, ReplyTo: "owner.main", RunID: "run-recovery", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if handled, err := first.HandleNext(ctx, DefaultAddress); err != nil || handled.Status != "committed" {
		t.Fatalf("handle execute: result=%#v err=%v", handled, err)
	}
	if _, err := first.DispatchNextOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	if handled, err := first.HandleNext(ctx, capability.DefaultAddress); err != nil || handled.Status != "committed" {
		t.Fatalf("handle capability list: result=%#v err=%v", handled, err)
	}
	if _, err := first.DispatchNextOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	handled, err := first.HandleNext(ctx, DefaultAddress)
	if err != nil || handled.Status != "committed" || len(handled.EffectIDs) != 1 {
		t.Fatalf("persist prompt effect: result=%#v err=%v", handled, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := buildRecoveryRuntime(t, ctx, storage, artifacts, response, results, "agent-recovery-owner-2")
	defer second.Close()
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- second.Serve(ctx) }()
	select {
	case result := <-results:
		if result.Phase != PhaseCompleted || result.Output == nil || result.Turns != 1 {
			t.Fatalf("recovered result=%#v", result)
		}
		reader, _, err := second.OpenArtifact(ctx, *result.Output)
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil || string(content) != "recovered completion" {
			t.Fatalf("recovered output=%q err=%v", content, err)
		}
	case err := <-serveErrors:
		t.Fatalf("recovered runtime stopped before completion: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for recovered agent completion")
	}
}

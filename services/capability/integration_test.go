package capability

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/persistence"
	persistencememory "agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"agent/services/approval"
)

var (
	integrationAgentComponent = contract.ComponentRef{Type: "test.capability-agent", Version: "v1"}
	integrationUIComponent    = contract.ComponentRef{Type: "test.approval-ui", Version: "v1"}
)

type capturedMessage struct{ message contract.Message }

type captureService struct{ messages chan capturedMessage }

func (c *captureService) Descriptor() service.Descriptor { return service.Descriptor{} }

func (c *captureService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (c *captureService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	c.messages <- capturedMessage{message: message.Clone()}
	return service.Decision{}, nil
}

func (*captureService) Apply(service.State, contract.StoredEvent) (service.State, error) {
	return service.State{}, fmt.Errorf("capture service does not persist events")
}

type integrationExecutor struct{ calls int }

func (e *integrationExecutor) ExecuteEffect(_ context.Context, _ persistence.EffectRecord) (effect.ExecutionResult, error) {
	e.calls++
	return effect.ExecutionResult{Payload: json.RawMessage(`{"result":{"echo":"ok"}}`)}, nil
}

func (*integrationExecutor) ReconcileEffect(_ context.Context, _ persistence.EffectRecord) (effect.ReconciliationResult, error) {
	return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: json.RawMessage(`{"result":{"echo":"ok"}}`)}, nil
}

type integrationRuntime struct {
	runtime  *serviceruntime.Runtime
	agent    chan capturedMessage
	ui       chan capturedMessage
	executor *integrationExecutor
}

func buildIntegrationRuntime(t *testing.T, ctx context.Context, storage persistence.RuntimeStorage, clock contract.Clock, ownerID string) integrationRuntime {
	t.Helper()
	agentMessages := make(chan capturedMessage, 8)
	uiMessages := make(chan capturedMessage, 8)
	executor := &integrationExecutor{}
	approvalModule, err := approval.NewModule(approval.ModuleOptions{
		Clock: clock, TrustedRequesters: []contract.ServiceAddress{"capability.main"},
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilityModule, err := NewModule(ModuleOptions{
		Clock: clock,
		Evaluator: AuthorizationEvaluatorFunc(func(AuthorizationInput) (AuthorizationDecision, error) {
			return AuthorizationDecision{
				Decision: AuthorizationAsk, RuleRef: "integration-ask@v1", ReasonCode: "confirm",
				RiskSummary: "runs the integration executor", ApprovalScope: "call",
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := &testProvider{
		descriptor: CapabilityDescriptor{
			Ref: "test.echo", Version: "v1", ProviderRef: "test-provider",
			ExecutionKind: ExecutionEffect, ExecutorRef: "test.echo@v1", EffectType: "test.echo",
			DescriptorRevision: "descriptor-1",
		},
		plan: CapabilityExecutionPlan{
			Kind: ExecutionEffect, ExecutionKey: "echo",
			Effect: &EffectPlan{Type: "test.echo", Version: 1, ExecutorRef: "test.echo@v1", Payload: json.RawMessage(`{"value":"hello"}`)},
		},
	}
	if err := capabilityModule.RegisterProvider(provider); err != nil {
		t.Fatal(err)
	}
	if err := capabilityModule.RegisterExecutor(effect.Spec{
		Ref: "test.echo@v1", Type: "test.echo", Executor: executor, Reconciler: executor,
	}); err != nil {
		t.Fatal(err)
	}
	builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
		Storage: storage, Clock: clock, IDs: serviceruntime.StableIDs{}, OwnerID: ownerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := approvalModule.Register(builder); err != nil {
		t.Fatal(err)
	}
	if err := capabilityModule.Register(builder); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: integrationAgentComponent,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			value := &captureService{messages: agentMessages}
			return &descriptorCaptureService{captureService: value, component: integrationAgentComponent}, nil
		}),
		Scope:    building.ScopeMounted,
		Consumes: []building.MessageContract{{Kind: contract.MessageReply, Type: ResultMessageType, Version: ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: integrationUIComponent,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			value := &captureService{messages: uiMessages}
			return &descriptorCaptureService{captureService: value, component: integrationUIComponent}, nil
		}),
		Scope:    building.ScopeMounted,
		Consumes: []building.MessageContract{{Kind: contract.MessageEvent, Type: approval.RequestedEventType, Version: approval.ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	manifest := building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "capability-integration", Revision: "v1"},
		Services: []building.ServiceMount{
			capabilityModule.Mount("capability.main", "approval.main", ""),
			approvalModule.Mount("approval.main", "ui.main", ""),
			{Address: "agent.main", Component: integrationAgentComponent},
			{Address: "ui.main", Component: integrationUIComponent},
		},
	}
	runtime, err := builder.Build(ctx, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Start(ctx); err != nil {
		runtime.Close()
		t.Fatal(err)
	}
	return integrationRuntime{runtime: runtime, agent: agentMessages, ui: uiMessages, executor: executor}
}

type descriptorCaptureService struct {
	*captureService
	component contract.ComponentRef
}

func (s *descriptorCaptureService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: s.component}
}

func drainIntegrationOutbox(t *testing.T, ctx context.Context, runtime *serviceruntime.Runtime) {
	t.Helper()
	for attempts := 0; attempts < 32; attempts++ {
		result, err := runtime.DispatchNextOutbox(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if result.Idle {
			return
		}
	}
	t.Fatal("outbox did not become idle")
}

func TestAskFlowRecoversPendingApprovalAndCompletesEffect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := &capabilityTestClock{now: time.Date(2026, 7, 21, 15, 0, 0, 0, time.UTC)}
	storage := persistencememory.New(clock)
	defer storage.Close()
	first := buildIntegrationRuntime(t, ctx, storage, clock, "capability-integration-owner-1")
	deadline := clock.now.Add(time.Hour)
	message := invokeMessage(t, "call-integration", `{"input":42}`, deadline)
	message.ID = "integration-invoke"
	message.RuntimeID, message.PlanRevision = "", ""
	if _, err := first.runtime.Publish(ctx, message); err != nil {
		t.Fatal(err)
	}
	if handled, err := first.runtime.HandleNext(ctx, "capability.main"); err != nil || handled.Status != "committed" || len(handled.EffectIDs) != 0 {
		t.Fatalf("handle invoke: result=%#v err=%v", handled, err)
	}
	drainIntegrationOutbox(t, ctx, first.runtime)
	if handled, err := first.runtime.HandleNext(ctx, "approval.main"); err != nil || handled.Status != "committed" {
		t.Fatalf("handle approval request: result=%#v err=%v", handled, err)
	}
	drainIntegrationOutbox(t, ctx, first.runtime)
	if handled, err := first.runtime.HandleNext(ctx, "ui.main"); err != nil || handled.Status != "committed" {
		t.Fatalf("handle UI notification: result=%#v err=%v", handled, err)
	}
	select {
	case captured := <-first.ui:
		if captured.message.Type != approval.RequestedEventType {
			t.Fatalf("UI message=%#v", captured.message)
		}
	default:
		t.Fatal("approval request was not delivered to UI")
	}
	if handled, err := first.runtime.HandleNext(ctx, "capability.main"); err != nil || handled.Status != "committed" {
		t.Fatalf("handle approval acknowledgement: result=%#v err=%v", handled, err)
	}
	if err := first.runtime.Close(); err != nil {
		t.Fatal(err)
	}

	second := buildIntegrationRuntime(t, ctx, storage, clock, "capability-integration-owner-2")
	defer second.runtime.Close()
	resolvePayload, _ := json.Marshal(approval.ResolveRequest{
		ApprovalID: stableID("approval", "call-integration", "descriptor-1", "integration-ask@v1"),
		CallID:     "call-integration", Decision: approval.DecisionApprove, ReasonCode: "user_confirmed",
	})
	if _, err := second.runtime.Publish(ctx, contract.Message{
		ID: "integration-resolve", Kind: contract.MessageCommand,
		Type: approval.ResolveMessageType, Version: approval.ProtocolVersion,
		From: "ui.main", To: "approval.main", UserID: "user-1", Payload: resolvePayload,
	}); err != nil {
		t.Fatal(err)
	}
	if handled, err := second.runtime.HandleNext(ctx, "approval.main"); err != nil || handled.Status != "committed" {
		t.Fatalf("handle approval resolution: result=%#v err=%v", handled, err)
	}
	drainIntegrationOutbox(t, ctx, second.runtime)
	if handled, err := second.runtime.HandleNext(ctx, "capability.main"); err != nil || handled.Status != "committed" || len(handled.EffectIDs) != 1 {
		t.Fatalf("handle approved call: result=%#v err=%v", handled, err)
	}
	worked, err := second.runtime.DispatchNextEffect(ctx)
	if err != nil || worked.Status != persistence.EffectSucceeded || second.executor.calls != 1 {
		t.Fatalf("dispatch effect: result=%#v calls=%d err=%v", worked, second.executor.calls, err)
	}
	if handled, err := second.runtime.HandleNext(ctx, "capability.main"); err != nil || handled.Status != "committed" {
		t.Fatalf("handle effect result: result=%#v err=%v", handled, err)
	}
	drainIntegrationOutbox(t, ctx, second.runtime)
	if handled, err := second.runtime.HandleNext(ctx, "agent.main"); err != nil || handled.Status != "committed" {
		t.Fatalf("handle final capability result: result=%#v err=%v", handled, err)
	}
	select {
	case captured := <-second.agent:
		var result Result
		if err := json.Unmarshal(captured.message.Payload, &result); err != nil {
			t.Fatal(err)
		}
		if result.CallID != "call-integration" || result.Phase != PhaseSucceeded || string(result.Result) != `{"echo":"ok"}` {
			t.Fatalf("final result=%#v", result)
		}
	default:
		t.Fatal("final capability result was not delivered to agent")
	}
}

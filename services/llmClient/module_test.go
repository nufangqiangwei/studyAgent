package llmClient

import (
	serviceruntime "agent/serviceruntime"
	"agent/serviceruntime/artifact/memory"
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	persistencememory "agent/serviceruntime/persistence/memory"
	"agent/serviceruntime/service"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClient struct {
	mu      sync.Mutex
	calls   int
	content string
	request ClientRequest
	key     string
}

func (f *fakeClient) Complete(_ context.Context, request ClientRequest, idempotencyKey string) (Completion, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.request = request
	f.key = idempotencyKey
	return Completion{Content: f.content}, nil
}

func (f *fakeClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var replyOwnerComponent = contract.ComponentRef{Type: "test.model-reply-owner", Version: "v1"}

type replyOwner struct{ replies chan CompletionReply }

func (*replyOwner) Descriptor() service.Descriptor {
	return service.Descriptor{Component: replyOwnerComponent}
}

func (*replyOwner) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1}, nil
}

func (o *replyOwner) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	var reply CompletionReply
	if err := json.Unmarshal(message.Payload, &reply); err != nil {
		return service.Decision{}, err
	}
	o.replies <- reply
	return service.Decision{}, nil
}

func (*replyOwner) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

func TestModuleCompletesThroughEffectAndReturnsArtifactKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	artifacts, err := memory.New("model-test-artifacts")
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	storage := persistencememory.New(serviceruntime.SystemClock{})
	defer storage.Close()
	client := &fakeClient{content: "the model answer"}
	module, err := NewModule(Config{
		BaseURL: "https://provider.example/v1", APIKey: "super-secret",
		Provider: ProviderOpenAI, ModelName: "test-model",
	}, WithClient(client))
	if err != nil {
		t.Fatal(err)
	}
	builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
		Storage: storage, Artifacts: artifacts, IDs: serviceruntime.StableIDs{}, OwnerID: "model-test-owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.Register(builder); err != nil {
		t.Fatal(err)
	}
	replies := make(chan CompletionReply, 1)
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: replyOwnerComponent,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return &replyOwner{replies: replies}, nil
		}),
		Scope:    building.ScopeMounted,
		Consumes: []building.MessageContract{{Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion}},
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime: building.RuntimeSpec{ID: "model-test-runtime", Revision: "v1"},
		Services: []building.ServiceMount{
			module.Mount(DefaultAddress),
			{Address: "requester", Component: replyOwnerComponent},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(CompletionRequest{RequestID: "request-42", Prompt: "answer me"})
	if err != nil {
		t.Fatal(err)
	}
	messageID := "model-command-1"
	if _, err := runtime.Publish(ctx, contract.Message{
		ID: messageID, Kind: contract.MessageCommand, Type: CompleteMessageType, Version: ProtocolVersion,
		From: "requester", To: DefaultAddress, ReplyTo: "requester", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	handled, err := runtime.HandleNext(ctx, DefaultAddress)
	if err != nil || handled.Status != "committed" || len(handled.EffectIDs) != 1 {
		t.Fatalf("handle model request: result=%#v err=%v", handled, err)
	}
	if client.callCount() != 0 {
		t.Fatal("Service.Handle called the provider before Effect dispatch")
	}
	unfinished, err := storage.Effects().ListUnfinished(ctx, "model-test-runtime")
	if err != nil || len(unfinished) != 1 {
		t.Fatalf("unfinished effects=%#v err=%v", unfinished, err)
	}
	if bytes.Contains(unfinished[0].Payload, []byte("super-secret")) {
		t.Fatal("API key leaked into persisted Effect payload")
	}
	worked, err := runtime.DispatchNextEffect(ctx)
	if err != nil || worked.Status != persistence.EffectSucceeded {
		t.Fatalf("dispatch model effect: result=%#v err=%v", worked, err)
	}
	if client.callCount() != 1 || client.key != messageID {
		t.Fatalf("provider calls=%d idempotency_key=%q", client.callCount(), client.key)
	}
	if handled, err := runtime.HandleNext(ctx, "requester"); err != nil || handled.Status != "committed" {
		t.Fatalf("handle completion reply: result=%#v err=%v", handled, err)
	}
	var reply CompletionReply
	select {
	case reply = <-replies:
	case <-ctx.Done():
		t.Fatal("timed out waiting for completion reply")
	}
	if reply.RequestID != "request-42" || reply.ArtifactKey == "" || reply.Artifact.Key != reply.ArtifactKey {
		t.Fatalf("completion reply=%#v", reply)
	}
	reader, _, err := runtime.OpenArtifact(ctx, reply.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(data) != "the model answer" {
		t.Fatalf("artifact content=%q err=%v", data, err)
	}
}

type flakyIngress struct {
	calls    int
	messages []contract.Message
}

func (f *flakyIngress) Send(_ context.Context, message contract.Message) error {
	f.calls++
	if f.calls == 1 {
		return fmt.Errorf("simulated reply delivery failure")
	}
	f.messages = append(f.messages, message.Clone())
	return nil
}

func TestEffectRetryReusesCommittedArtifactWithoutCallingModelAgain(t *testing.T) {
	ctx := context.Background()
	artifacts, err := memory.New("retry-artifacts")
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	client := &fakeClient{content: "stable answer"}
	module, err := NewModule(Config{
		BaseURL: "https://provider.example/v1", Provider: ProviderOpenAI, ModelName: "test-model",
	}, WithClient(client))
	if err != nil {
		t.Fatal(err)
	}
	ingress := &flakyIngress{}
	if err := module.BindRuntime(assembly.RuntimePorts{
		RuntimeID: "retry-runtime", Ingress: ingress, Artifacts: artifacts, IDs: serviceruntime.StableIDs{},
	}); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(completionEffectPayload{
		Request: CompletionRequest{RequestID: "request-retry", Prompt: "hello"},
		ReplyTo: "requester", SourceAddress: DefaultAddress,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := persistence.EffectRecord{
		EffectID: "effect-retry-1", RuntimeID: "retry-runtime", PlanRevision: "v1",
		SourceMessageID: "source-1", IdempotencyKey: "source-1", Payload: payload,
	}
	if _, err := module.executeEffect(ctx, record); err == nil || !strings.Contains(err.Error(), "delivery failure") {
		t.Fatalf("first execute error=%v", err)
	}
	if client.callCount() != 1 {
		t.Fatalf("first provider call count=%d", client.callCount())
	}
	if _, err := module.executeEffect(ctx, record); err != nil {
		t.Fatal(err)
	}
	if client.callCount() != 1 {
		t.Fatalf("retry called provider %d times", client.callCount())
	}
	if len(ingress.messages) != 1 || ingress.messages[0].ID == "" {
		t.Fatalf("retried replies=%#v", ingress.messages)
	}
}

func TestTerminalModelFailurePublishesDurableErrorReply(t *testing.T) {
	artifacts, err := memory.New("terminal-model-artifacts")
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	module, err := NewModule(Config{
		BaseURL: "https://provider.example/v1", Provider: ProviderOpenAI, ModelName: "test-model",
	}, WithClient(&fakeClient{content: "unused"}))
	if err != nil {
		t.Fatal(err)
	}
	ingress := &flakyIngress{calls: 1}
	if err := module.BindRuntime(assembly.RuntimePorts{
		RuntimeID: "terminal-model-runtime", Ingress: ingress, Artifacts: artifacts, IDs: serviceruntime.StableIDs{},
	}); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(completionEffectPayload{
		Request: CompletionRequest{RequestID: "request-terminal", Prompt: "hello"},
		ReplyTo: "agent.main", SourceAddress: DefaultAddress, CorrelationID: "model-turn-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	record := persistence.EffectRecord{
		EffectID: "effect-terminal-model", RuntimeID: "terminal-model-runtime", PlanRevision: "v1",
		SourceMessageID: "source-terminal", IdempotencyKey: "source-terminal", Payload: payload,
	}
	if err := module.notifyTerminalFailure(context.Background(), record, fmt.Errorf("provider failed")); err != nil {
		t.Fatal(err)
	}
	if len(ingress.messages) != 1 {
		t.Fatalf("messages=%#v", ingress.messages)
	}
	message := ingress.messages[0]
	if message.Kind != contract.MessageReply || message.Type != CompletedMessageType || message.To != "agent.main" ||
		message.CorrelationID != "model-turn-1" || message.Metadata[contract.MetadataReplyError] != "true" {
		t.Fatalf("terminal reply=%#v", message)
	}
	var replyError service.ReplyError
	if err := json.Unmarshal(message.Payload, &replyError); err != nil || replyError.Code != "model_completion_failed" {
		t.Fatalf("reply error=%#v err=%v", replyError, err)
	}
}

func TestConfigRejectsCredentialsInBaseURL(t *testing.T) {
	_, err := NewModule(Config{
		BaseURL: "https://user:password@provider.example/v1", Provider: ProviderOpenAI, ModelName: "test-model",
	})
	if err == nil {
		t.Fatal("expected base URL credentials to be rejected")
	}
}

func TestInvalidRequestReturnsErrorReplyWithoutEffect(t *testing.T) {
	model := &modelService{}
	decision, err := model.Handle(context.Background(), service.State{SchemaVersion: 1}, contract.Message{
		ID: "invalid-request", Kind: contract.MessageCommand, Type: CompleteMessageType, Version: ProtocolVersion,
		ReplyTo: "requester", Payload: json.RawMessage(`{"prompt":""}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Effects) != 0 || decision.Reply == nil || decision.Reply.Error == nil || decision.Reply.Error.Code != "invalid_request" {
		t.Fatalf("invalid request decision=%#v", decision)
	}
}

func TestOpenAICompatibleHTTPAdapter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" {
			t.Errorf("request path=%q", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer test-key" || request.Header.Get("Idempotency-Key") != "stable-id" {
			t.Errorf("request headers=%v", request.Header)
		}
		var body struct {
			Model    string        `json:"model"`
			Messages []ChatMessage `json:"messages"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Model != "provider-model" || len(body.Messages) != 1 || body.Messages[0].Content != "hello" {
			t.Errorf("request body=%#v", body)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":"world"}}]}`))
	}))
	defer server.Close()
	module, err := NewModule(Config{
		BaseURL: server.URL + "/v1", APIKey: "test-key", Provider: ProviderOpenAI,
		ModelName: "provider-model", HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	completion, err := module.client.Complete(context.Background(), ClientRequest{
		Provider: ProviderOpenAI, ModelName: "provider-model",
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
	}, "stable-id")
	if err != nil || completion.Content != "world" {
		t.Fatalf("completion=%#v err=%v", completion, err)
	}
}

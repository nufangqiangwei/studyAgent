package serviceruntime

import (
	"agent/serviceruntime/artifact"
	artifactmemory "agent/serviceruntime/artifact/memory"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

var artifactReaderComponent = contract.ComponentRef{Type: "test.artifact-reader", Version: "v1"}

const artifactReadMessage contract.MessageType = "test.artifact.read"

type artifactReadingService struct {
	reader   artifact.Reader
	observed *string
}

func (s *artifactReadingService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: artifactReaderComponent}
}

func (s *artifactReadingService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (s *artifactReadingService) Handle(ctx context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	var input struct {
		Content contract.ArtifactRef `json:"content"`
	}
	if err := json.Unmarshal(message.Payload, &input); err != nil {
		return service.Decision{}, err
	}
	reader, _, err := s.reader.Open(ctx, input.Content)
	if err != nil {
		return service.Decision{}, err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return service.Decision{}, err
	}
	*s.observed = string(data)
	return service.Decision{}, nil
}

func (s *artifactReadingService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state.Clone(), nil
}

func TestRuntimeArtifactDataPlaneAndFactoryReader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := artifactmemory.New("runtime-artifacts")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var observed string
	builder, err := NewBuilder(BuilderOptions{Artifacts: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.RegisterService(building.ServiceDefinition{
		Component: artifactReaderComponent,
		Factory: service.FactoryFunc(func(_ context.Context, request service.CreateRequest) (service.Service, error) {
			if request.Artifacts == nil {
				t.Fatal("factory did not receive artifact reader")
			}
			return &artifactReadingService{reader: request.Artifacts, observed: &observed}, nil
		}),
		Consumes: []building.MessageContract{{Kind: contract.MessageCommand, Type: artifactReadMessage, Version: 1}},
		Scope:    building.ScopeMounted,
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := builder.Build(ctx, building.RuntimeManifest{
		Runtime:  building.RuntimeSpec{ID: "artifact-runtime", Revision: "v1"},
		Services: []building.ServiceMount{{Address: "artifact-reader", Component: artifactReaderComponent}},
		Routes: building.RouteManifest{Commands: map[contract.MessageType]contract.ServiceAddress{
			artifactReadMessage: "artifact-reader",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	ref, err := runtime.WriteArtifact(ctx, artifact.WriteRequest{Key: "llm/responses/one", ContentType: "text/plain"}, strings.NewReader("model output"))
	if err != nil {
		t.Fatalf("WriteArtifact: %v", err)
	}
	if _, err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(struct {
		Content contract.ArtifactRef `json:"content"`
	}{Content: ref})
	if _, err := runtime.Publish(ctx, contract.Message{Kind: contract.MessageCommand, Type: artifactReadMessage, Version: 1, Payload: payload}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	result, err := runtime.HandleNext(ctx, "artifact-reader")
	if err != nil {
		t.Fatalf("HandleNext: %v", err)
	}
	if result.Status != "committed" || observed != "model output" {
		t.Fatalf("result = %#v, observed = %q", result, observed)
	}
}

func TestRuntimeArtifactAPIUnavailableWithoutConfiguredStore(t *testing.T) {
	t.Parallel()
	var runtime *Runtime
	if _, err := runtime.WriteArtifact(context.Background(), artifact.WriteRequest{Key: "x"}, strings.NewReader("x")); err != artifact.ErrUnavailable {
		t.Fatalf("WriteArtifact error = %v", err)
	}
}

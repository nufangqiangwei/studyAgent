package interaction

import (
	"agent/serviceruntime/artifact"
	artifactmemory "agent/serviceruntime/artifact/memory"
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestPresentationEffectReadsArtifactBeforeCallingPresenter(t *testing.T) {
	ctx := context.Background()
	artifacts, err := artifactmemory.New("interaction-test")
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	ref, err := artifact.WriteAll(ctx, artifacts, artifact.WriteRequest{
		Key: "answers/request.txt", ContentType: "text/plain",
	}, strings.NewReader("artifact answer"))
	if err != nil {
		t.Fatal(err)
	}
	presented := make(chan Presentation, 1)
	module, err := NewModule(ModuleOptions{Presenter: PresenterFunc(func(_ context.Context, value Presentation) error {
		presented <- value
		return nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.BindRuntime(assembly.RuntimePorts{RuntimeID: "runtime-1", Artifacts: artifacts}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(Presentation{
		ID: "request/request-1/completed", Kind: PresentationAnswer,
		RequestID: "request-1", RunID: "request-1", Output: &ref,
	})
	result, err := module.executePresentation(ctx, persistence.EffectRecord{RuntimeID: "runtime-1", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Payload) == 0 {
		t.Fatal("presentation effect did not return a compact result")
	}
	value := <-presented
	if value.Content != "artifact answer" || value.Output == nil || value.Output.Key != ref.Key {
		t.Fatalf("presented=%#v", value)
	}
}

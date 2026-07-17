package building

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"testing"
)

type noopService struct{ component contract.ComponentRef }

func (s noopService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: s.component}
}
func (noopService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1}, nil
}
func (noopService) Handle(context.Context, service.State, contract.Message) (service.Decision, error) {
	return service.Decision{}, nil
}
func (noopService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state, nil
}

func definition(ref contract.ComponentRef, consumes ...MessageContract) ServiceDefinition {
	return ServiceDefinition{
		Component: ref, Scope: ScopeMounted, Consumes: consumes,
		Factory: service.FactoryFunc(func(context.Context, service.CreateRequest) (service.Service, error) {
			return noopService{component: ref}, nil
		}),
	}
}

func TestCompileCreatesImmutablePlan(t *testing.T) {
	register := NewRegister(nil)
	ref := contract.ComponentRef{Type: "counter", Version: "v1"}
	if err := register.RegisterService(definition(ref, MessageContract{Kind: contract.MessageCommand, Type: "counter.increment", Version: 1})); err != nil {
		t.Fatal(err)
	}
	config := []byte(`{"step":1}`)
	metadata := map[string]string{"owner": "test"}
	manifest := RuntimeManifest{
		Runtime:  RuntimeSpec{ID: "test", Revision: "v1"},
		Services: []ServiceMount{{Address: "counter.main", Component: ref, Config: config, Metadata: metadata}},
		Routes:   RouteManifest{Commands: map[contract.MessageType]contract.ServiceAddress{"counter.increment": "counter.main"}},
	}
	plan, err := register.Compile(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	config[8] = '9'
	metadata["owner"] = "changed"
	mounted, ok := plan.Service("counter.main")
	if !ok {
		t.Fatal("planned service not found")
	}
	if string(mounted.Config) != `{"step":1}` || mounted.Metadata["owner"] != "test" {
		t.Fatalf("plan changed through manifest aliases: config=%s metadata=%v", mounted.Config, mounted.Metadata)
	}
	mounted.Config[8] = '7'
	mounted.Metadata["owner"] = "mutated"
	again, _ := plan.Service("counter.main")
	if string(again.Config) != `{"step":1}` || again.Metadata["owner"] != "test" {
		t.Fatalf("plan getter exposed mutable state: config=%s metadata=%v", again.Config, again.Metadata)
	}
}

func TestCompileRejectsDependencyCycle(t *testing.T) {
	register := NewRegister(nil)
	ref := contract.ComponentRef{Type: "node", Version: "v1"}
	def := definition(ref)
	def.Dependencies = []ServiceDependency{{Name: "peer", Required: true, AcceptedTypes: []contract.ServiceType{"node"}}}
	if err := register.RegisterService(def); err != nil {
		t.Fatal(err)
	}
	_, err := register.Compile(context.Background(), RuntimeManifest{
		Runtime: RuntimeSpec{ID: "test", Revision: "v1"},
		Services: []ServiceMount{
			{Address: "node.a", Component: ref, Dependencies: map[string]contract.ServiceAddress{"peer": "node.b"}},
			{Address: "node.b", Component: ref, Dependencies: map[string]contract.ServiceAddress{"peer": "node.a"}},
		},
	})
	if err == nil {
		t.Fatal("expected dependency cycle error")
	}
	compileErr, ok := err.(*CompileError)
	if !ok {
		t.Fatalf("error type = %T, want *CompileError", err)
	}
	found := false
	for _, issue := range compileErr.Issues {
		found = found || issue.Code == "cycle"
	}
	if !found {
		t.Fatalf("issues = %#v, want cycle", compileErr.Issues)
	}
}

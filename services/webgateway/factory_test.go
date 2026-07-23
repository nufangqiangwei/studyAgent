package webgateway

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestServiceFactoryReadsDefaultAgentFromCreateRequestConfig(t *testing.T) {
	factory := ServiceFactory{clock: fixedClock{fixedTime()}}
	raw, err := json.Marshal(serviceConfig{
		Version: serviceConfigVersion, DefaultAgent: "agent.from-old-plan",
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := factory.Create(context.Background(), service.CreateRequest{
		InstanceID: "web-gateway-1",
		Address:    DefaultAddress,
		Component:  Component,
		Config:     raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	gateway, ok := created.(*webGatewayService)
	if !ok {
		t.Fatalf("service type=%T", created)
	}
	if gateway.defaultAgent != "agent.from-old-plan" {
		t.Fatalf("default agent=%q", gateway.defaultAgent)
	}
}

func TestServiceFactoryUsesFallbackOnlyForLegacyEmptyMount(t *testing.T) {
	factory := ServiceFactory{legacyDefaultAgent: "agent.legacy"}
	legacy, err := factory.Create(context.Background(), service.CreateRequest{
		InstanceID: "web-gateway-legacy",
		Address:    DefaultAddress,
		Component:  Component,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := legacy.(*webGatewayService).defaultAgent; got != "agent.legacy" {
		t.Fatalf("legacy default agent=%q", got)
	}

	versionedConfig, err := json.Marshal(serviceConfig{
		Version: serviceConfigVersion, DefaultAgent: "agent.versioned",
	})
	if err != nil {
		t.Fatal(err)
	}
	versioned, err := factory.Create(context.Background(), service.CreateRequest{
		InstanceID: "web-gateway-versioned",
		Address:    DefaultAddress,
		Component:  Component,
		Config:     versionedConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := versioned.(*webGatewayService).defaultAgent; got != "agent.versioned" {
		t.Fatalf("versioned mount fell back to legacy agent: %q", got)
	}

	for _, malformed := range []json.RawMessage{
		json.RawMessage(` `),
		json.RawMessage(`{"version":99,"default_agent":"agent.versioned"}`),
		json.RawMessage(`{"version":1,"default_agent":`),
	} {
		if _, err := factory.Create(context.Background(), service.CreateRequest{
			InstanceID: "web-gateway-invalid",
			Address:    DefaultAddress,
			Component:  Component,
			Config:     malformed,
		}); err == nil {
			t.Fatalf("non-empty invalid config %q used the legacy fallback", malformed)
		}
	}
}

func TestServiceFactoryRejectsInvalidMountConfig(t *testing.T) {
	factory := ServiceFactory{}
	tests := []struct {
		name   string
		config json.RawMessage
	}{
		{name: "missing", config: nil},
		{name: "malformed", config: json.RawMessage(`{"version":1`)},
		{name: "missing version", config: json.RawMessage(`{"default_agent":"agent.test"}`)},
		{name: "old version", config: json.RawMessage(`{"version":0,"default_agent":"agent.test"}`)},
		{name: "future version", config: json.RawMessage(`{"version":2,"default_agent":"agent.test"}`)},
		{name: "missing agent", config: json.RawMessage(`{"version":1}`)},
		{name: "blank agent", config: json.RawMessage(`{"version":1,"default_agent":"  "}`)},
		{name: "noncanonical agent", config: json.RawMessage(`{"version":1,"default_agent":" agent.test "}`)},
		{name: "unknown field", config: json.RawMessage(`{"version":1,"default_agent":"agent.test","extra":true}`)},
		{name: "multiple values", config: json.RawMessage(`{"version":1,"default_agent":"agent.test"} {}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := factory.Create(context.Background(), service.CreateRequest{
				InstanceID: "web-gateway-1",
				Address:    DefaultAddress,
				Component:  Component,
				Config:     test.config,
			})
			if err == nil || !strings.Contains(err.Error(), "web gateway service config") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestModuleMountConfigIsVersionedAndImmutable(t *testing.T) {
	module, err := NewModule(ModuleOptions{
		Presenter:          PresenterFunc(func(context.Context, Presentation) error { return nil }),
		DefaultAgent:       "agent.test",
		LegacyDefaultAgent: "agent.legacy",
	})
	if err != nil {
		t.Fatal(err)
	}
	first := module.Mount(DefaultAddress)
	var config serviceConfig
	if err := json.Unmarshal(first.Config, &config); err != nil {
		t.Fatal(err)
	}
	if config.Version != serviceConfigVersion || config.DefaultAgent != "agent.test" {
		t.Fatalf("mount config=%#v", config)
	}

	for index := range first.Config {
		first.Config[index] = 'x'
	}
	second := module.Mount(DefaultAddress)
	if err := json.Unmarshal(second.Config, &config); err != nil {
		t.Fatalf("module config changed through mount alias: %v", err)
	}
	if config.Version != serviceConfigVersion || config.DefaultAgent != contract.ServiceAddress("agent.test") {
		t.Fatalf("second mount config=%#v", config)
	}
}

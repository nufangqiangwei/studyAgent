package webgateway

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	runtimesystem "agent/serviceruntime/system"
	"agent/services/task"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type ServiceFactory struct {
	clock              contract.Clock
	legacyDefaultAgent contract.ServiceAddress
}

func (f ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	if request.Component != Component {
		return nil, fmt.Errorf("web gateway factory received component %q", request.Component.String())
	}
	if request.InstanceID == "" || request.Address == "" {
		return nil, fmt.Errorf("web gateway service requires instance id and address")
	}
	defaultAgent := f.legacyDefaultAgent
	if len(request.Config) > 0 {
		config, err := decodeServiceConfig(request.Config)
		if err != nil {
			return nil, err
		}
		defaultAgent = config.DefaultAgent
	} else if defaultAgent == "" {
		return nil, fmt.Errorf("legacy web gateway service config requires a default agent fallback")
	}
	return &webGatewayService{
		address: request.Address, instanceID: request.InstanceID,
		clock: f.clock, defaultAgent: defaultAgent,
	}, nil
}

func decodeServiceConfig(raw json.RawMessage) (serviceConfig, error) {
	var config serviceConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return serviceConfig{}, fmt.Errorf("decode web gateway service config: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return serviceConfig{}, fmt.Errorf("decode web gateway service config: %w", err)
	}
	if config.Version != serviceConfigVersion {
		return serviceConfig{}, fmt.Errorf(
			"web gateway service config version %d is unsupported", config.Version,
		)
	}
	if strings.TrimSpace(string(config.DefaultAgent)) == "" {
		return serviceConfig{}, fmt.Errorf("web gateway service config requires a default agent address")
	}
	if string(config.DefaultAgent) != strings.TrimSpace(string(config.DefaultAgent)) {
		return serviceConfig{}, fmt.Errorf("web gateway service config default agent address must be canonical")
	}
	return config, nil
}

func Definition(factory service.Factory) building.ServiceDefinition {
	return building.ServiceDefinition{
		Component: Component, Factory: factory, Scope: building.ScopeRuntimeSingleton, StateSchema: StateSchema,
		Consumes: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: CreateTaskMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageCommand, Type: GetTaskMessageType, Version: ProtocolVersion},
			{Kind: contract.MessageReply, Type: runtimesystem.ResultMessageType, Version: runtimesystem.CallVersion},
			{Kind: contract.MessageReply, Type: task.StatusMessageType, Version: task.ProtocolVersion},
			// The Gateway is Task Owner. Status-change events are acknowledged
			// without building the future Projection; terminal events can also
			// advance an in-flight create through an authoritative task.get.
			{Kind: contract.MessageEvent, Type: task.StatusChangedEventType, Version: task.ProtocolVersion},
			{Kind: contract.MessageEvent, Type: task.CompletedEventType, Version: task.ProtocolVersion},
			{Kind: contract.MessageEvent, Type: task.FailedEventType, Version: task.ProtocolVersion},
			{Kind: contract.MessageEvent, Type: task.CancelledEventType, Version: task.ProtocolVersion},
		},
		Produces: []building.MessageContract{
			{Kind: contract.MessageCommand, Type: runtimesystem.CallMessageType, Version: runtimesystem.CallVersion},
			{Kind: contract.MessageCommand, Type: task.CreateMessageType, Version: task.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: task.MarkReadyMessageType, Version: task.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: task.AssignMessageType, Version: task.ProtocolVersion},
			{Kind: contract.MessageCommand, Type: task.StartMessageType, Version: task.ProtocolVersion},
			{Kind: contract.MessageQuery, Type: task.GetMessageType, Version: task.ProtocolVersion},
		},
		EffectExecutors:  []string{PresentationExecutorRef},
		SystemOperations: []string{assembly.SystemOperationDeclareInstance},
	}
}

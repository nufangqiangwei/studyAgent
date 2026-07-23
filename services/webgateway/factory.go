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
	clock contract.Clock
}

func (f ServiceFactory) Create(_ context.Context, request service.CreateRequest) (service.Service, error) {
	if request.Component != Component {
		return nil, fmt.Errorf("web gateway factory received component %q", request.Component.String())
	}
	if request.InstanceID == "" || request.Address == "" {
		return nil, fmt.Errorf("web gateway service requires instance id and address")
	}
	config, err := decodeServiceConfig(request.Config)
	if err != nil {
		return nil, err
	}
	return &webGatewayService{
		address: request.Address, instanceID: request.InstanceID,
		clock: f.clock, defaultAgent: config.DefaultAgent,
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
			// The Gateway is Task Owner. Task Service sends status change and terminal
			// events to the owner address. The Gateway acknowledges them (no-op) so
			// they do not become Dead Letter; future Projection consumers will use
			// these to maintain task list / timeline views.
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

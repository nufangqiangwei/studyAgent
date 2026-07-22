package llmClient

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type modelService struct{}

func (*modelService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: Component, StateSchema: StateSchema}
}

func (*modelService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: StateSchema.Version, Data: json.RawMessage(`{}`)}, nil
}

func (*modelService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Kind != contract.MessageCommand || message.Type != CompleteMessageType || message.Version != ProtocolVersion {
		return service.Decision{}, fmt.Errorf("unsupported model message %s %q v%d", message.Kind, message.Type, message.Version)
	}
	if strings.TrimSpace(string(message.ReplyTo)) == "" {
		return service.Decision{}, fmt.Errorf("model completion command requires reply_to")
	}
	var request CompletionRequest
	if err := json.Unmarshal(message.Payload, &request); err != nil {
		return completionRejection("invalid_payload", "model completion payload is not valid JSON"), nil
	}
	if err := request.validate(); err != nil {
		return completionRejection("invalid_request", err.Error()), nil
	}
	if request.RequestID == "" {
		request.RequestID = message.ID
	}
	payload, err := json.Marshal(completionEffectPayload{
		Request: request.clone(), ReplyTo: message.ReplyTo, SourceAddress: message.To,
		CorrelationID: message.CorrelationID, UserID: message.UserID, GoalID: message.GoalID,
		RunID: message.RunID, StreamID: message.StreamID,
	})
	if err != nil {
		return service.Decision{}, fmt.Errorf("encode model completion effect: %w", err)
	}
	return service.Decision{Effects: []service.PlannedEffect{{
		Key: "complete-model", Type: CompleteEffectType, Version: ProtocolVersion,
		ExecutorRef: CompleteExecutorRef, IdempotencyKey: message.ID,
		Payload: payload, Deadline: message.Deadline,
	}}}, nil
}

func completionRejection(code, message string) service.Decision {
	return service.Decision{Reply: &service.Reply{
		Key: "reject-model-completion", Type: CompletedMessageType, Version: ProtocolVersion,
		Error: &service.ReplyError{Code: code, Message: message},
	}}
}

func (*modelService) Apply(_ service.State, event contract.StoredEvent) (service.State, error) {
	return service.State{}, fmt.Errorf("model service does not persist event %q v%d", event.EventType, event.EventVersion)
}

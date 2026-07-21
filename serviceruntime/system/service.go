package system

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type runtimeService struct{}

func (s *runtimeService) Descriptor() service.Descriptor {
	return service.Descriptor{Component: Component}
}

func (s *runtimeService) InitialState(context.Context, service.Init) (service.State, error) {
	return service.State{SchemaVersion: 1, Data: json.RawMessage(`{}`)}, nil
}

func (s *runtimeService) Handle(_ context.Context, _ service.State, message contract.Message) (service.Decision, error) {
	if message.Kind != contract.MessageCommand || message.Type != CallMessageType || message.Version != CallVersion {
		return service.Decision{}, fmt.Errorf("unsupported system message %s %q version %d", message.Kind, message.Type, message.Version)
	}
	if message.ReplyTo == "" {
		return service.Decision{}, fmt.Errorf("system call requires reply_to")
	}
	var call Call
	if err := json.Unmarshal(message.Payload, &call); err != nil {
		return rejectedDecision(Call{}, "invalid_call", "decode system call: "+err.Error()), nil
	}
	call.CallID = strings.TrimSpace(call.CallID)
	call.Operation = strings.TrimSpace(call.Operation)
	if call.CallID == "" || call.Operation == "" || call.OperationVersion <= 0 {
		return rejectedDecision(call, "invalid_call", "call_id, operation and positive operation_version are required"), nil
	}

	switch call.Operation {
	case DeclareInstanceOperation:
		if call.OperationVersion != 1 {
			return rejectedDecision(call, "unsupported_version", fmt.Sprintf("operation %q version %d is unsupported", call.Operation, call.OperationVersion)), nil
		}
		var request DeclareInstanceRequest
		if err := json.Unmarshal(call.Payload, &request); err != nil {
			return rejectedDecision(call, "invalid_payload", "decode instance declaration: "+err.Error()), nil
		}
	default:
		return rejectedDecision(call, "unsupported_operation", fmt.Sprintf("system operation %q is not registered", call.Operation)), nil
	}

	payload, err := json.Marshal(systemEffectPayload{
		Call: call, Caller: message.From, ReplyTo: message.ReplyTo,
		CorrelationID: message.CorrelationID, UserID: message.UserID,
		GoalID: message.GoalID, RunID: message.RunID, StreamID: message.StreamID,
	})
	if err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Effects: []service.PlannedEffect{{
		Key: "system-call", Type: EffectType, Version: 1,
		ExecutorRef: ExecutorRef, IdempotencyKey: call.CallID, Payload: payload,
	}}}, nil
}

func (s *runtimeService) Apply(state service.State, _ contract.StoredEvent) (service.State, error) {
	return state.Clone(), nil
}

func rejectedDecision(call Call, code, message string) service.Decision {
	return service.Decision{Reply: &service.Reply{
		Key: "system-result", Type: ResultMessageType, Version: CallVersion,
		Error:    &service.ReplyError{Code: code, Message: message},
		Metadata: callMetadata(call),
	}}
}

func callMetadata(call Call) map[string]string {
	metadata := make(map[string]string, 2)
	if call.CallID != "" {
		metadata[MetadataCallID] = call.CallID
	}
	if call.Operation != "" {
		metadata[MetadataOperation] = call.Operation
	}
	return metadata
}

package system

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/service"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type runtimeBinding struct {
	control assembly.InstanceControl
	ingress assembly.MessageIngress
	ids     contract.IDGenerator
}

type systemEffectPayload struct {
	Call          Call                    `json:"call"`
	Caller        contract.ServiceAddress `json:"caller"`
	ReplyTo       contract.ServiceAddress `json:"reply_to"`
	CorrelationID string                  `json:"correlation_id,omitempty"`
	UserID        string                  `json:"user_id,omitempty"`
	GoalID        string                  `json:"goal_id,omitempty"`
	RunID         string                  `json:"run_id,omitempty"`
	StreamID      contract.StreamID       `json:"stream_id,omitempty"`
}

func (m *Module) executeEffect(ctx context.Context, record persistence.EffectRecord) (effect.ExecutionResult, error) {
	binding, found := m.resolve(record.RuntimeID, record.PlanRevision)
	if !found {
		return effect.ExecutionResult{}, fmt.Errorf("system runtime control is not bound for %q revision %q", record.RuntimeID, record.PlanRevision)
	}
	var input systemEffectPayload
	if err := json.Unmarshal(record.Payload, &input); err != nil {
		return effect.ExecutionResult{}, fmt.Errorf("decode system call effect: %w", err)
	}
	resultPayload, metadata, err := executeSystemCall(ctx, binding, input)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	correlationID := input.CorrelationID
	if correlationID == "" {
		correlationID = record.SourceMessageID
	}
	message := contract.Message{
		ID:   binding.ids.Derive("system-result", record.EffectID),
		Kind: contract.MessageReply, Type: ResultMessageType, Version: CallVersion,
		From: Address, To: input.ReplyTo,
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		UserID: input.UserID, GoalID: input.GoalID, RunID: input.RunID,
		CorrelationID: correlationID, CausationID: record.SourceMessageID,
		StreamID: input.StreamID, Payload: resultPayload, Metadata: metadata,
	}
	if err := binding.ingress.Send(ctx, message); err != nil {
		return effect.ExecutionResult{}, fmt.Errorf("publish system result: %w", err)
	}
	return effect.ExecutionResult{Payload: contract.CloneRaw(resultPayload), Metadata: contract.CloneStrings(metadata)}, nil
}

func (m *Module) reconcileEffect(ctx context.Context, record persistence.EffectRecord) (effect.ReconciliationResult, error) {
	result, err := m.executeEffect(ctx, record)
	if err != nil {
		return effect.ReconciliationResult{Action: effect.ReconcileRetry, Reason: err.Error()}, nil
	}
	return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: result.Payload}, nil
}

func executeSystemCall(ctx context.Context, binding runtimeBinding, input systemEffectPayload) (json.RawMessage, map[string]string, error) {
	call := input.Call
	metadata := callMetadata(call)
	switch call.Operation {
	case DeclareInstanceOperation:
		var request DeclareInstanceRequest
		if err := json.Unmarshal(call.Payload, &request); err != nil {
			return systemErrorPayload(call, "invalid_payload", "decode instance declaration: "+err.Error()), errorMetadata(metadata), nil
		}
		record, err := binding.control.Declare(ctx, input.Caller, request)
		if err != nil {
			var rejection *assembly.ControlRejection
			if !errors.As(err, &rejection) {
				return nil, nil, err
			}
			code := rejection.Code
			if code == "" {
				code = "operation_rejected"
			}
			return systemErrorPayload(call, code, err.Error()), errorMetadata(metadata), nil
		}
		declared, err := json.Marshal(DeclareInstanceResult{Instance: record})
		if err != nil {
			return nil, nil, err
		}
		payload, err := json.Marshal(Result{
			CallID: call.CallID, Operation: call.Operation,
			OperationVersion: call.OperationVersion, Result: declared,
		})
		return payload, metadata, err
	default:
		return systemErrorPayload(call, "unsupported_operation", fmt.Sprintf("system operation %q is not registered", call.Operation)), errorMetadata(metadata), nil
	}
}

func systemErrorPayload(call Call, code, message string) json.RawMessage {
	payload, _ := json.Marshal(service.ReplyError{
		Code: code, Message: message,
		Details: map[string]string{MetadataCallID: call.CallID, MetadataOperation: call.Operation},
	})
	return payload
}

func errorMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata[contract.MetadataReplyError] = "true"
	return metadata
}

package connection

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"fmt"
)

const (
	OpenEffectType  contract.EffectType = "connection.open"
	SendEffectType  contract.EffectType = "connection.send"
	CloseEffectType contract.EffectType = "connection.close"

	OpenExecutorRef  = "connection.open@v1"
	SendExecutorRef  = "connection.send@v1"
	CloseExecutorRef = "connection.close@v1"
)

type sendEffectPayload struct {
	ConnectionID string            `json:"connection_id"`
	Generation   uint64            `json:"generation"`
	Data         []byte            `json:"data,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type operationExecutor struct {
	module    *Module
	operation contract.EffectType
}

func (m *Module) effectSpecs() []effect.Spec {
	return []effect.Spec{
		{Ref: OpenExecutorRef, Type: OpenEffectType, Executor: operationExecutor{module: m, operation: OpenEffectType}, Reconciler: operationReconciler{module: m, operation: OpenEffectType}},
		{Ref: SendExecutorRef, Type: SendEffectType, Executor: operationExecutor{module: m, operation: SendEffectType}, Reconciler: operationReconciler{module: m, operation: SendEffectType}},
		{Ref: CloseExecutorRef, Type: CloseEffectType, Executor: operationExecutor{module: m, operation: CloseEffectType}, Reconciler: operationReconciler{module: m, operation: CloseEffectType}},
	}
}

func (e operationExecutor) ExecuteEffect(ctx context.Context, record persistence.EffectRecord) (effect.ExecutionResult, error) {
	supervisor, found := e.module.resources.Resolve(record.InstanceID)
	if !found {
		return effect.ExecutionResult{}, fmt.Errorf("connection resources for service instance %q are not active", record.InstanceID)
	}
	switch e.operation {
	case OpenEffectType:
		var input Record
		if err := json.Unmarshal(record.Payload, &input); err != nil {
			return effect.ExecutionResult{}, fmt.Errorf("decode open connection effect: %w", err)
		}
		if err := supervisor.Open(ctx, input, record.EffectID); err != nil {
			return effect.ExecutionResult{}, err
		}
		return executionResult(infoFromRecord(input))
	case SendEffectType:
		var input sendEffectPayload
		if err := json.Unmarshal(record.Payload, &input); err != nil {
			return effect.ExecutionResult{}, fmt.Errorf("decode send connection effect: %w", err)
		}
		if err := supervisor.Send(ctx, input, record.EffectID); err != nil {
			return effect.ExecutionResult{}, err
		}
		return executionResult(map[string]string{"connection_id": input.ConnectionID})
	case CloseEffectType:
		var input Record
		if err := json.Unmarshal(record.Payload, &input); err != nil {
			return effect.ExecutionResult{}, fmt.Errorf("decode close connection effect: %w", err)
		}
		if err := supervisor.Close(ctx, input, record.EffectID); err != nil {
			return effect.ExecutionResult{}, err
		}
		return executionResult(infoFromRecord(input))
	default:
		return effect.ExecutionResult{}, fmt.Errorf("unsupported connection effect %q", e.operation)
	}
}

type operationReconciler struct {
	module    *Module
	operation contract.EffectType
}

func (r operationReconciler) ReconcileEffect(_ context.Context, effectRecord persistence.EffectRecord) (effect.ReconciliationResult, error) {
	supervisor, found := r.module.resources.Resolve(effectRecord.InstanceID)
	if !found {
		return effect.ReconciliationResult{Action: effect.ReconcileRetry, Reason: "connection service is not active"}, nil
	}
	switch r.operation {
	case OpenEffectType:
		var input Record
		if err := json.Unmarshal(effectRecord.Payload, &input); err != nil {
			return effect.ReconciliationResult{}, err
		}
		if supervisor.IsActive(input.ConnectionID, input.Generation) {
			return effect.ReconciliationResult{Action: effect.ReconcileComplete, Reason: "connection is active"}, nil
		}
		return effect.ReconciliationResult{Action: effect.ReconcileRetry, Reason: "connection is not active"}, nil
	case SendEffectType:
		// A retried send carries the same Frame.ID. Drivers with exactly-once
		// requirements must deduplicate it at the protocol boundary.
		return effect.ReconciliationResult{Action: effect.ReconcileRetry, Reason: "retry send with stable frame id"}, nil
	case CloseEffectType:
		var input Record
		if err := json.Unmarshal(effectRecord.Payload, &input); err != nil {
			return effect.ReconciliationResult{}, err
		}
		if !supervisor.IsActive(input.ConnectionID, input.Generation) {
			return effect.ReconciliationResult{Action: effect.ReconcileComplete, Reason: "connection is already closed"}, nil
		}
		return effect.ReconciliationResult{Action: effect.ReconcileRetry, Reason: "connection still active"}, nil
	default:
		return effect.ReconciliationResult{}, fmt.Errorf("unsupported connection effect %q", r.operation)
	}
}

func executionResult(value any) (effect.ExecutionResult, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	return effect.ExecutionResult{Payload: payload}, nil
}

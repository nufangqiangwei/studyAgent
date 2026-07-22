package capability

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"fmt"
)

type executionEffectEnvelope struct {
	CallID            string                  `json:"call_id"`
	ExecutionKey      string                  `json:"execution_key"`
	Generation        uint64                  `json:"generation"`
	CapabilityAddress contract.ServiceAddress `json:"capability_address"`
	ExecutorRef       string                  `json:"executor_ref"`
	Payload           json.RawMessage         `json:"payload,omitempty"`
	CorrelationID     string                  `json:"correlation_id"`
	UserID            string                  `json:"user_id,omitempty"`
}

// ExecutorOutput is the optional normalized result envelope returned by a
// Capability executor. Executors may also return an arbitrary JSON payload,
// which is treated as the inline Result directly.
type ExecutorOutput struct {
	ResultRef *contract.ArtifactRef `json:"result_ref,omitempty"`
	Result    json.RawMessage       `json:"result,omitempty"`
}

type feedbackPendingError struct{ cause error }

func (e feedbackPendingError) Error() string {
	return "capability execution outcome requires reconciliation: " + e.cause.Error()
}
func (e feedbackPendingError) Unwrap() error      { return e.cause }
func (feedbackPendingError) OutcomeUnknown() bool { return true }

type wrappedExecutor struct {
	module   *Module
	delegate effect.Executor
}

func (e wrappedExecutor) ExecuteEffect(ctx context.Context, record persistence.EffectRecord) (effect.ExecutionResult, error) {
	binding, envelope, delegated, err := e.module.executionDependencies(record)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	result, executeErr := e.delegate.ExecuteEffect(ctx, delegated)
	if executeErr != nil {
		if fault.KindOf(executeErr) != fault.Permanent {
			return effect.ExecutionResult{}, executeErr
		}
		failure := ExecutionFailed{
			CallID: envelope.CallID, ExecutionKey: envelope.ExecutionKey,
			ExecutorRef: envelope.ExecutorRef, Generation: envelope.Generation,
			OutcomeID: stableID("capability-outcome", record.EffectID, "failed"), ErrorCode: errExecutionFailed,
			ErrorMessage: "capability execution failed",
		}
		if err := publishExecutionFailure(ctx, binding, record, envelope, failure); err != nil {
			return effect.ExecutionResult{}, feedbackPendingError{cause: err}
		}
		payload, _ := json.Marshal(failure)
		return effect.ExecutionResult{Payload: payload}, nil
	}
	completed, err := executionCompletion(envelope, stableID("capability-outcome", record.EffectID, "succeeded"), result.Payload)
	if err != nil {
		return effect.ExecutionResult{}, fault.Wrap(fault.Permanent, "normalize_capability_result", err)
	}
	if err := publishExecutionCompletion(ctx, binding, record, envelope, completed); err != nil {
		return effect.ExecutionResult{}, feedbackPendingError{cause: err}
	}
	return result, nil
}

type wrappedReconciler struct {
	module   *Module
	delegate effect.Reconciler
}

func (r wrappedReconciler) ReconcileEffect(ctx context.Context, record persistence.EffectRecord) (effect.ReconciliationResult, error) {
	binding, envelope, delegated, err := r.module.executionDependencies(record)
	if err != nil {
		return effect.ReconciliationResult{}, err
	}
	result, err := r.delegate.ReconcileEffect(ctx, delegated)
	if err != nil {
		return result, err
	}
	switch result.Action {
	case effect.ReconcileComplete:
		completed, err := executionCompletion(envelope, stableID("capability-outcome", record.EffectID, "succeeded"), result.Result)
		if err != nil {
			return effect.ReconciliationResult{}, err
		}
		if err := publishExecutionCompletion(ctx, binding, record, envelope, completed); err != nil {
			return effect.ReconciliationResult{}, err
		}
	case effect.ReconcileFail:
		failure := ExecutionFailed{
			CallID: envelope.CallID, ExecutionKey: envelope.ExecutionKey,
			ExecutorRef: envelope.ExecutorRef, Generation: envelope.Generation,
			OutcomeID: stableID("capability-outcome", record.EffectID, "failed"), ErrorCode: errExecutionFailed,
			ErrorMessage: "capability execution could not be reconciled",
		}
		if err := publishExecutionFailure(ctx, binding, record, envelope, failure); err != nil {
			return effect.ReconciliationResult{}, err
		}
		payload, _ := json.Marshal(failure)
		return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: payload}, nil
	}
	return result, nil
}

type wrappedTerminalFailure struct{ module *Module }

func (n wrappedTerminalFailure) NotifyTerminalFailure(ctx context.Context, record persistence.EffectRecord, _ error) error {
	binding, envelope, _, err := n.module.executionDependencies(record)
	if err != nil {
		return err
	}
	return publishExecutionFailure(ctx, binding, record, envelope, ExecutionFailed{
		CallID: envelope.CallID, ExecutionKey: envelope.ExecutionKey,
		ExecutorRef: envelope.ExecutorRef, Generation: envelope.Generation,
		OutcomeID: stableID("capability-outcome", record.EffectID, "failed"), ErrorCode: errExecutionFailed,
		ErrorMessage: "capability execution reached a terminal failure",
	})
}

func (m *Module) executionDependencies(record persistence.EffectRecord) (runtimeBinding, executionEffectEnvelope, persistence.EffectRecord, error) {
	binding, found := m.resolveBinding(record.RuntimeID)
	if !found {
		return runtimeBinding{}, executionEffectEnvelope{}, persistence.EffectRecord{}, fmt.Errorf("capability module is not bound for runtime %q", record.RuntimeID)
	}
	var envelope executionEffectEnvelope
	if err := json.Unmarshal(record.Payload, &envelope); err != nil {
		return runtimeBinding{}, executionEffectEnvelope{}, persistence.EffectRecord{}, fmt.Errorf("decode capability effect envelope: %w", err)
	}
	if clean(envelope.CallID) == "" || clean(envelope.ExecutionKey) == "" || envelope.Generation == 0 ||
		envelope.CapabilityAddress == "" || envelope.ExecutorRef != record.ExecutorRef || !jsonPayloadValid(envelope.Payload) {
		return runtimeBinding{}, executionEffectEnvelope{}, persistence.EffectRecord{}, fmt.Errorf("capability effect envelope is invalid")
	}
	delegated := record.Clone()
	delegated.Payload = contract.CloneRaw(envelope.Payload)
	return binding, envelope, delegated, nil
}

func executionCompletion(envelope executionEffectEnvelope, outcomeID string, payload json.RawMessage) (ExecutionCompleted, error) {
	completed := ExecutionCompleted{
		CallID: envelope.CallID, ExecutionKey: envelope.ExecutionKey,
		ExecutorRef: envelope.ExecutorRef, Generation: envelope.Generation,
		OutcomeID: outcomeID,
	}
	if len(payload) == 0 {
		completed.Result = json.RawMessage(`{}`)
		return completed, nil
	}
	if !json.Valid(payload) {
		return ExecutionCompleted{}, fmt.Errorf("executor result is not valid JSON")
	}
	var normalized ExecutorOutput
	if err := json.Unmarshal(payload, &normalized); err == nil && (normalized.ResultRef != nil || len(normalized.Result) > 0) {
		completed.ResultRef = normalized.ResultRef
		completed.Result = contract.CloneRaw(normalized.Result)
		return completed, nil
	}
	completed.Result = contract.CloneRaw(payload)
	return completed, nil
}

func publishExecutionCompletion(ctx context.Context, binding runtimeBinding, record persistence.EffectRecord, envelope executionEffectEnvelope, completed ExecutionCompleted) error {
	payload, err := json.Marshal(completed)
	if err != nil {
		return err
	}
	return publishExecutionMessage(ctx, binding, record, envelope, ExecutionCompletedMessageType, payload)
}

func publishExecutionFailure(ctx context.Context, binding runtimeBinding, record persistence.EffectRecord, envelope executionEffectEnvelope, failed ExecutionFailed) error {
	payload, err := json.Marshal(failed)
	if err != nil {
		return err
	}
	return publishExecutionMessage(ctx, binding, record, envelope, ExecutionFailedMessageType, payload)
}

func publishExecutionMessage(ctx context.Context, binding runtimeBinding, record persistence.EffectRecord, envelope executionEffectEnvelope, messageType contract.MessageType, payload json.RawMessage) error {
	message := contract.Message{
		ID:   stableID("capability-effect-result", record.EffectID, string(messageType)),
		Kind: contract.MessageEvent, Type: messageType, Version: ProtocolVersion,
		From: envelope.CapabilityAddress, To: envelope.CapabilityAddress,
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		UserID: envelope.UserID, CorrelationID: envelope.CorrelationID,
		CausationID: record.SourceMessageID,
		StreamID:    contract.StreamID("capability/effect/" + envelope.CallID), Payload: payload,
	}
	if err := binding.ingress.Send(ctx, message); err != nil {
		return fmt.Errorf("publish durable capability execution result: %w", err)
	}
	return nil
}

var _ effect.UnknownOutcome = feedbackPendingError{}
var _ assembly.RuntimeBinder = (*Module)(nil)

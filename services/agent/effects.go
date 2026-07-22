package agent

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/persistence"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	promptContentType = "text/markdown; charset=utf-8"
	outputContentType = "text/plain; charset=utf-8"
)

func (m *Module) executeArtifactEffect(ctx context.Context, record persistence.EffectRecord) (effect.ExecutionResult, error) {
	binding, input, err := m.artifactEffectDependencies(record)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	ref, found, err := findPreparedArtifact(ctx, binding.artifacts, record.EffectID, input.Operation)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	if !found {
		request := artifact.WriteRequest{Key: preparedArtifactKey(record.EffectID, input.Operation), ContentType: preparedContentType(input.Operation)}
		session, err := binding.artifacts.Begin(ctx, request)
		if err != nil {
			return effect.ExecutionResult{}, err
		}
		writeErr := func() error {
			switch input.Operation {
			case preparePrompt:
				return writePrompt(ctx, session, binding.artifacts, input)
			case prepareOutput:
				return writeFinalOutput(ctx, session, binding.artifacts, input)
			default:
				return fmt.Errorf("unsupported agent artifact operation %q", input.Operation)
			}
		}()
		if writeErr != nil {
			_ = session.Abort(context.Background())
			return effect.ExecutionResult{}, writeErr
		}
		ref, err = session.Commit(ctx)
		if err != nil {
			_ = session.Abort(context.Background())
			return effect.ExecutionResult{}, err
		}
	}
	prepared := artifactPrepared{Operation: input.Operation, RunID: input.RunID, Turn: input.Turn, Artifact: ref}
	if err := publishPreparedArtifact(ctx, binding, record, input, prepared); err != nil {
		return effect.ExecutionResult{}, err
	}
	payload, err := json.Marshal(prepared)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	return effect.ExecutionResult{Payload: payload, Metadata: map[string]string{"artifact_key": ref.Key}}, nil
}

func (m *Module) reconcileArtifactEffect(ctx context.Context, record persistence.EffectRecord) (effect.ReconciliationResult, error) {
	binding, input, err := m.artifactEffectDependencies(record)
	if err != nil {
		return effect.ReconciliationResult{}, err
	}
	ref, found, err := findPreparedArtifact(ctx, binding.artifacts, record.EffectID, input.Operation)
	if err != nil {
		return effect.ReconciliationResult{}, err
	}
	if !found {
		return effect.ReconciliationResult{Action: effect.ReconcileRetry, Reason: "prepared artifact is absent"}, nil
	}
	prepared := artifactPrepared{Operation: input.Operation, RunID: input.RunID, Turn: input.Turn, Artifact: ref}
	if err := publishPreparedArtifact(ctx, binding, record, input, prepared); err != nil {
		return effect.ReconciliationResult{Action: effect.ReconcileRetry, Reason: "retry durable artifact notification"}, nil
	}
	payload, err := json.Marshal(prepared)
	if err != nil {
		return effect.ReconciliationResult{}, err
	}
	return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: payload}, nil
}

func (m *Module) notifyArtifactTerminalFailure(ctx context.Context, record persistence.EffectRecord, _ error) error {
	binding, input, err := m.artifactEffectDependencies(record)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(artifactFailed{
		Operation: input.Operation, RunID: input.RunID, Turn: input.Turn,
		ErrorCode: "agent_artifact_preparation_failed",
	})
	if err != nil {
		return err
	}
	message := contract.Message{
		ID:   binding.ids.Derive("agent-artifact-failed", record.EffectID),
		Kind: contract.MessageEvent, Type: ArtifactFailedMessageType, Version: ProtocolVersion,
		From: input.AgentAddress, To: input.AgentAddress,
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		UserID: input.UserID, GoalID: input.GoalID, RunID: input.RunID,
		CorrelationID: input.CorrelationID, CausationID: record.SourceMessageID,
		Payload: payload,
	}
	return binding.ingress.Send(ctx, message)
}

func (m *Module) artifactEffectDependencies(record persistence.EffectRecord) (runtimeBinding, promptPreparation, error) {
	binding, found := m.resolve(record.RuntimeID)
	if !found {
		return runtimeBinding{}, promptPreparation{}, fmt.Errorf("agent module is not bound for runtime %q", record.RuntimeID)
	}
	var input promptPreparation
	if err := json.Unmarshal(record.Payload, &input); err != nil {
		return runtimeBinding{}, promptPreparation{}, fmt.Errorf("decode agent artifact effect: %w", err)
	}
	input.Spec = input.Spec.withDefaults()
	if err := input.Spec.validate(); err != nil {
		return runtimeBinding{}, promptPreparation{}, fmt.Errorf("validate persisted agent spec: %w", err)
	}
	if input.AgentAddress == "" || input.RunID == "" || input.Turn <= 0 || input.CorrelationID == "" {
		return runtimeBinding{}, promptPreparation{}, fmt.Errorf("agent artifact effect identity is incomplete")
	}
	switch input.Operation {
	case preparePrompt:
		if input.Source != nil {
			return runtimeBinding{}, promptPreparation{}, fmt.Errorf("prompt preparation cannot have an output source")
		}
	case prepareOutput:
		if input.Source == nil {
			return runtimeBinding{}, promptPreparation{}, fmt.Errorf("output preparation requires a source artifact")
		}
	default:
		return runtimeBinding{}, promptPreparation{}, fmt.Errorf("agent artifact operation %q is invalid", input.Operation)
	}
	return binding, input, nil
}

func publishPreparedArtifact(ctx context.Context, binding runtimeBinding, record persistence.EffectRecord, input promptPreparation, prepared artifactPrepared) error {
	payload, err := json.Marshal(prepared)
	if err != nil {
		return err
	}
	message := contract.Message{
		ID:   binding.ids.Derive("agent-artifact-prepared", record.EffectID),
		Kind: contract.MessageEvent, Type: ArtifactPreparedMessageType, Version: ProtocolVersion,
		From: input.AgentAddress, To: input.AgentAddress,
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		UserID: input.UserID, GoalID: input.GoalID, RunID: input.RunID,
		CorrelationID: input.CorrelationID, CausationID: record.SourceMessageID,
		Payload: payload,
	}
	if err := binding.ingress.Send(ctx, message); err != nil {
		return fmt.Errorf("publish prepared agent artifact: %w", err)
	}
	return nil
}

func findPreparedArtifact(ctx context.Context, store artifact.Store, effectID string, operation artifactOperation) (contract.ArtifactRef, bool, error) {
	probe := contract.ArtifactRef{Store: store.Name(), Key: preparedArtifactKey(effectID, operation), ContentType: preparedContentType(operation)}
	info, err := store.Stat(ctx, probe)
	if err == nil {
		return info.Ref, true, nil
	}
	if errors.Is(err, artifact.ErrNotFound) {
		return contract.ArtifactRef{}, false, nil
	}
	return contract.ArtifactRef{}, false, err
}

func preparedArtifactKey(effectID string, operation artifactOperation) string {
	sum := sha256.Sum256([]byte(effectID))
	directory, extension := "prompts", ".md"
	if operation == prepareOutput {
		directory, extension = "outputs", ".txt"
	}
	return "agent/" + directory + "/" + hex.EncodeToString(sum[:]) + extension
}

func preparedContentType(operation artifactOperation) string {
	if operation == prepareOutput {
		return outputContentType
	}
	return promptContentType
}

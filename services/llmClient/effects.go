package llmClient

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/effect"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/persistence"
	"agent/serviceruntime/service"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const completionContentType = "text/plain; charset=utf-8"

type completionEffectPayload struct {
	Request       CompletionRequest       `json:"request"`
	ReplyTo       contract.ServiceAddress `json:"reply_to"`
	SourceAddress contract.ServiceAddress `json:"source_address"`
	CorrelationID string                  `json:"correlation_id,omitempty"`
	UserID        string                  `json:"user_id,omitempty"`
	GoalID        string                  `json:"goal_id,omitempty"`
	RunID         string                  `json:"run_id,omitempty"`
	StreamID      contract.StreamID       `json:"stream_id,omitempty"`
}

func (m *Module) executeEffect(ctx context.Context, record persistence.EffectRecord) (effect.ExecutionResult, error) {
	binding, input, err := m.effectDependencies(record)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	ref, found, err := m.findCompletionArtifact(ctx, binding.artifacts, record.EffectID)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	if !found {
		request, err := m.materializeRequest(ctx, binding.artifacts, input.Request)
		if err != nil {
			return effect.ExecutionResult{}, err
		}
		completion, err := m.client.Complete(ctx, request, record.IdempotencyKey)
		if err != nil {
			var httpErr *ProviderHTTPError
			if errors.As(err, &httpErr) && !httpErr.Retryable() {
				return effect.ExecutionResult{}, fault.Wrap(fault.Permanent, "call_model_provider", err)
			}
			return effect.ExecutionResult{}, err
		}
		ref, err = artifact.WriteAll(ctx, binding.artifacts, artifact.WriteRequest{
			Key: completionArtifactKey(record.EffectID), ContentType: completionContentType,
		}, strings.NewReader(completion.Content))
		if err != nil {
			return effect.ExecutionResult{}, fmt.Errorf("store model completion artifact: %w", err)
		}
	}
	reply := m.completionReply(input, ref)
	if err := publishCompletionReply(ctx, binding, record, input, reply); err != nil {
		return effect.ExecutionResult{}, err
	}
	payload, err := json.Marshal(reply)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	return effect.ExecutionResult{Payload: payload, Metadata: map[string]string{"artifact_key": ref.Key}}, nil
}

func (m *Module) reconcileEffect(ctx context.Context, record persistence.EffectRecord) (effect.ReconciliationResult, error) {
	binding, input, err := m.effectDependencies(record)
	if err != nil {
		return effect.ReconciliationResult{}, err
	}
	ref, found, err := m.findCompletionArtifact(ctx, binding.artifacts, record.EffectID)
	if err != nil {
		return effect.ReconciliationResult{}, err
	}
	if !found {
		return effect.ReconciliationResult{
			Action: effect.ReconcileRetry,
			Reason: "model response artifact is absent; retry with the stable provider idempotency key",
		}, nil
	}
	reply := m.completionReply(input, ref)
	if err := publishCompletionReply(ctx, binding, record, input, reply); err != nil {
		return effect.ReconciliationResult{Action: effect.ReconcileRetry, Reason: "retry durable completion reply"}, nil
	}
	payload, err := json.Marshal(reply)
	if err != nil {
		return effect.ReconciliationResult{}, err
	}
	return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: payload}, nil
}

func (m *Module) notifyTerminalFailure(ctx context.Context, record persistence.EffectRecord, _ error) error {
	binding, input, err := m.effectDependencies(record)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(service.ReplyError{
		Code: "model_completion_failed", Message: "model completion reached a terminal failure",
	})
	if err != nil {
		return err
	}
	correlationID := input.CorrelationID
	if correlationID == "" {
		correlationID = record.SourceMessageID
	}
	from := input.SourceAddress
	if from == "" {
		from = DefaultAddress
	}
	message := contract.Message{
		ID:   binding.ids.Derive("model-completion-terminal-failure", record.EffectID),
		Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion,
		From: from, To: input.ReplyTo,
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		UserID: input.UserID, GoalID: input.GoalID, RunID: input.RunID,
		CorrelationID: correlationID, CausationID: record.SourceMessageID,
		StreamID: input.StreamID, Payload: payload,
		Metadata: map[string]string{contract.MetadataReplyError: "true"},
	}
	if err := binding.ingress.Send(ctx, message); err != nil {
		return fmt.Errorf("publish terminal model failure: %w", err)
	}
	return nil
}

func (m *Module) effectDependencies(record persistence.EffectRecord) (runtimeBinding, completionEffectPayload, error) {
	binding, found := m.resolve(record.RuntimeID)
	if !found {
		return runtimeBinding{}, completionEffectPayload{}, fmt.Errorf("llm client module is not bound for runtime %q", record.RuntimeID)
	}
	var input completionEffectPayload
	if err := json.Unmarshal(record.Payload, &input); err != nil {
		return runtimeBinding{}, completionEffectPayload{}, fmt.Errorf("decode model completion effect: %w", err)
	}
	if input.ReplyTo == "" {
		return runtimeBinding{}, completionEffectPayload{}, fmt.Errorf("model completion effect requires reply_to")
	}
	if err := input.Request.validate(); err != nil {
		return runtimeBinding{}, completionEffectPayload{}, fmt.Errorf("validate persisted model completion request: %w", err)
	}
	if input.Request.RequestID == "" {
		return runtimeBinding{}, completionEffectPayload{}, fmt.Errorf("model completion effect requires request_id")
	}
	return binding, input, nil
}

func (m *Module) materializeRequest(ctx context.Context, store artifact.Store, input CompletionRequest) (ClientRequest, error) {
	messages := append([]ChatMessage(nil), input.Messages...)
	if input.Prompt != "" {
		messages = append(messages, ChatMessage{Role: "user", Content: input.Prompt})
	}
	if input.InputArtifact != nil {
		reader, _, err := store.Open(ctx, *input.InputArtifact)
		if err != nil {
			return ClientRequest{}, fmt.Errorf("open model input artifact: %w", err)
		}
		defer reader.Close()
		data, err := io.ReadAll(io.LimitReader(reader, m.config.MaxInputArtifactBytes+1))
		if err != nil {
			return ClientRequest{}, fmt.Errorf("read model input artifact: %w", err)
		}
		if int64(len(data)) > m.config.MaxInputArtifactBytes {
			return ClientRequest{}, fmt.Errorf("model input artifact exceeds %d bytes", m.config.MaxInputArtifactBytes)
		}
		messages = append(messages, ChatMessage{Role: "user", Content: string(data)})
	}
	return ClientRequest{
		Provider: m.config.Provider, ModelName: m.config.ModelName, System: input.System,
		Messages: messages, Temperature: input.Temperature, MaxTokens: input.MaxTokens,
	}, nil
}

func (m *Module) findCompletionArtifact(ctx context.Context, store artifact.Store, effectID string) (contract.ArtifactRef, bool, error) {
	probe := contract.ArtifactRef{Store: store.Name(), Key: completionArtifactKey(effectID), ContentType: completionContentType}
	info, err := store.Stat(ctx, probe)
	if err == nil {
		return info.Ref, true, nil
	}
	if errors.Is(err, artifact.ErrNotFound) {
		return contract.ArtifactRef{}, false, nil
	}
	return contract.ArtifactRef{}, false, fmt.Errorf("inspect model completion artifact: %w", err)
}

func (m *Module) completionReply(input completionEffectPayload, ref contract.ArtifactRef) CompletionReply {
	return CompletionReply{
		RequestID: input.Request.RequestID, ArtifactKey: ref.Key, Artifact: ref,
		Provider: m.config.Provider, ModelName: m.config.ModelName,
	}
}

func publishCompletionReply(ctx context.Context, binding runtimeBinding, record persistence.EffectRecord, input completionEffectPayload, reply CompletionReply) error {
	payload, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	correlationID := input.CorrelationID
	if correlationID == "" {
		correlationID = record.SourceMessageID
	}
	from := input.SourceAddress
	if from == "" {
		from = DefaultAddress
	}
	message := contract.Message{
		ID:   binding.ids.Derive("model-completion-reply", record.EffectID),
		Kind: contract.MessageReply, Type: CompletedMessageType, Version: ProtocolVersion,
		From: from, To: input.ReplyTo,
		RuntimeID: record.RuntimeID, PlanRevision: record.PlanRevision,
		UserID: input.UserID, GoalID: input.GoalID, RunID: input.RunID,
		CorrelationID: correlationID, CausationID: record.SourceMessageID,
		StreamID: input.StreamID, Payload: payload,
		Metadata: map[string]string{"artifact_key": reply.ArtifactKey, "provider": reply.Provider, "model": reply.ModelName},
	}
	if err := binding.ingress.Send(ctx, message); err != nil {
		return fmt.Errorf("publish model completion reply: %w", err)
	}
	return nil
}

func completionArtifactKey(effectID string) string {
	sum := sha256.Sum256([]byte(effectID))
	return "llm/responses/" + hex.EncodeToString(sum[:]) + ".txt"
}

package webgateway

import (
	"agent/serviceruntime/effect"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"fmt"
)

type presentationResult struct {
	PresentationID string `json:"presentation_id"`
}

func (m *Module) executePresentation(ctx context.Context, record persistence.EffectRecord) (effect.ExecutionResult, error) {
	payload, err := m.deliverPresentation(ctx, record)
	if err != nil {
		return effect.ExecutionResult{}, err
	}
	return effect.ExecutionResult{Payload: payload}, nil
}

func (m *Module) reconcilePresentation(ctx context.Context, record persistence.EffectRecord) (effect.ReconciliationResult, error) {
	payload, err := m.deliverPresentation(ctx, record)
	if err != nil {
		return effect.ReconciliationResult{}, err
	}
	return effect.ReconciliationResult{Action: effect.ReconcileComplete, Result: payload}, nil
}

func (m *Module) deliverPresentation(ctx context.Context, record persistence.EffectRecord) (json.RawMessage, error) {
	if record.Type != PresentationEffectType || record.Version != ProtocolVersion || record.ExecutorRef != PresentationExecutorRef {
		return nil, fault.Wrap(fault.Permanent, "validate_web_task_presentation_effect",
			fmt.Errorf("unsupported presentation effect %q v%d via %q", record.Type, record.Version, record.ExecutorRef))
	}
	var presentation Presentation
	if err := json.Unmarshal(record.Payload, &presentation); err != nil {
		return nil, fault.Wrap(fault.Permanent, "decode_web_task_presentation", err)
	}
	if err := presentation.validate(); err != nil {
		return nil, fault.Wrap(fault.Permanent, "validate_web_task_presentation", err)
	}
	if err := m.presenter.Present(ctx, presentation.clone()); err != nil {
		return nil, fmt.Errorf("present web task result: %w", err)
	}
	payload, err := json.Marshal(presentationResult{PresentationID: presentation.PresentationID})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

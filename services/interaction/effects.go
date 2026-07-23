package interaction

import (
	"agent/serviceruntime/effect"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	binding, found := m.resolve(record.RuntimeID)
	if !found {
		return nil, fmt.Errorf("interaction module is not bound for runtime %q", record.RuntimeID)
	}
	var presentation Presentation
	if err := json.Unmarshal(record.Payload, &presentation); err != nil {
		return nil, fault.Wrap(fault.Permanent, "decode_interaction_presentation", err)
	}
	if presentation.ID == "" || presentation.Kind == "" {
		return nil, fault.Wrap(fault.Permanent, "validate_interaction_presentation", fmt.Errorf("presentation id and kind are required"))
	}
	if presentation.Content != "" {
		return nil, fault.Wrap(fault.Permanent, "validate_interaction_presentation", fmt.Errorf("persisted presentation cannot contain materialized content"))
	}
	switch presentation.Kind {
	case PresentationAnswer:
		if presentation.Output == nil {
			return nil, fault.Wrap(fault.Permanent, "validate_interaction_presentation", fmt.Errorf("answer presentation requires an output artifact"))
		}
	case PresentationError:
		if presentation.ErrorMessage == "" || presentation.Output != nil {
			return nil, fault.Wrap(fault.Permanent, "validate_interaction_presentation", fmt.Errorf("error presentation requires an error message and no output"))
		}
	case PresentationApproval:
		if presentation.Approval == nil || presentation.Output != nil {
			return nil, fault.Wrap(fault.Permanent, "validate_interaction_presentation", fmt.Errorf("approval presentation requires approval details and no output"))
		}
	default:
		return nil, fault.Wrap(fault.Permanent, "validate_interaction_presentation", fmt.Errorf("presentation kind %q is unsupported", presentation.Kind))
	}
	if presentation.Output != nil {
		reader, _, err := binding.artifacts.Open(ctx, *presentation.Output)
		if err != nil {
			return nil, fmt.Errorf("open interaction output artifact: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, m.maxOutputBytes+1))
		closeErr := reader.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read interaction output artifact: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close interaction output artifact: %w", closeErr)
		}
		if int64(len(data)) > m.maxOutputBytes {
			return nil, fault.Wrap(fault.Permanent, "read_interaction_output", fmt.Errorf("interaction output exceeds %d bytes", m.maxOutputBytes))
		}
		presentation.Content = string(data)
	}
	if err := m.presenter.Present(ctx, presentation.clone()); err != nil {
		return nil, fmt.Errorf("present interaction output: %w", err)
	}
	payload, err := json.Marshal(presentationResult{PresentationID: presentation.ID})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

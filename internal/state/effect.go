package state

import "encoding/json"

type EffectType string

const (
	EffectNoop      EffectType = "noop"
	EffectCallModel EffectType = "model.call"
	EffectFinalize  EffectType = "run.finalize"
)

type Effect struct {
	ID      string          `json:"id"`
	RunID   string          `json:"run_id"`
	Type    EffectType      `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func NewEffect(runID string, typ EffectType) Effect {
	return Effect{
		ID:    NewID("eff"),
		RunID: runID,
		Type:  typ,
	}
}

package state

import "encoding/json"

type EffectType string

const (
	EffectNoop         EffectType = "noop"
	EffectCallModel    EffectType = "model.call"
	EffectExecuteModel EffectType = "model.execute"
	EffectDispatchTool EffectType = "tool.dispatch"
	EffectExecuteTool  EffectType = "tool.execute"
	EffectCompleteRun  EffectType = "run.complete"
	EffectFailRun      EffectType = "run.fail"
	EffectFinalize     EffectType = "run.finalize"
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

func (e Effect) Clone() Effect {
	cloned := e
	cloned.Payload = append(json.RawMessage(nil), e.Payload...)
	return cloned
}
